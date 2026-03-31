# Implementation Plan — SharePoint File Ingestion Job
## Agent Session Brief

**Role:** You are a senior Databricks Python developer.  
**Principles:** TDD, DRY, KISS. No over-engineering. Code must be readable and maintainable by a human.  
**Target:** A small, fully tested Python library + a Databricks Asset Bundle job declaration.  
**Python stack:** `uv` for dependency management and virtual environments, `ruff` for linting and formatting.

---

## Context

You are implementing a daily Databricks job that ingests new and modified files from a SharePoint Online folder into a data lake raw layer.

Change detection uses the **Microsoft Graph API delta endpoint**, which returns only changed items since the last run via a `deltaLink` token. The token is persisted in a **Delta Lake table** (`sp_delta_token`). No other persistence layer exists.

The pipeline is read-only on SharePoint and append-only on the raw layer.

---

## Reference Documentation

### Job Parameters

Passed at Databricks job runtime. All required unless marked optional.

| Parameter | Required | Description | Example |
|---|---|---|---|
| `drive_id` | ✅ | SharePoint drive ID (rename-safe GUID) | `b!xYz123...` |
| `folder_item_id` | ❌ | Subfolder item ID. Omit to ingest full library. | `01ABCDEF...` |
| `raw_base_path` | ✅ | Raw layer base path | `abfss://...` or `/Volumes/...` |
| `library_name` | ✅ | Path namespace segment for this library | `finance-reports` |
| `file_ext_filter` | ❌ | Comma-separated extensions to include. Omit = all files. | `.xlsx,.csv` |

### Secrets (Databricks secret scope: `sharepoint`)

| Key | Description |
|---|---|
| `spn-client-id` | Service principal client ID |
| `spn-client-secret` | Service principal client secret |
| `spn-tenant-id` | Azure AD tenant ID |

### Delta Lake State Table (`sp_delta_token`)

```sql
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.sp_delta_token (
    drive_id     STRING    NOT NULL,  -- natural key
    delta_link   STRING,              -- NULL = first run
    updated_at   TIMESTAMP NOT NULL,
    last_status  STRING    NOT NULL   -- success | failed
) USING DELTA;
```

Read by `drive_id`. Written only on full job success. Never written on failure — this ensures the next run replays from the last valid token automatically.

### Raw Layer Path

```
{raw_base_path}/{library_name}/{yyyy}/{mm}/{dd}/{file_id}_{file_name}
```

`file_id` is the Graph API item ID — guarantees uniqueness even if the file is renamed.

### Graph API — Delta Endpoint

```
# First run (delta_link is None):
GET https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{folder_item_id}/delta

# First run without subfolder scoping:
GET https://graph.microsoft.com/v1.0/drives/{drive_id}/root/delta

# Subsequent runs:
GET https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{folder_item_id}/delta?token={token}
```

Paginate `@odata.nextLink` until exhausted. The final page contains `@odata.deltaLink` — this is the new token to store.

**Collect all pages before writing any file.** The token committed must reflect the complete delta.

Per item, skip if:
- `item.get("deleted")` is truthy
- `"file"` key is absent (it's a folder)
- file extension not in filter (if filter is set)

File download: use `@microsoft.graph.downloadUrl` from the item — it is a pre-authenticated short-lived URL, download immediately.

### Error Handling

| HTTP Status | Action |
|---|---|
| 429 | Respect `Retry-After` header, retry up to 3 times |
| 410 | Token expired — reset `delta_link = NULL` in state table, raise to fail the job |
| 401 | Raise immediately — auth failure, needs human intervention |
| Any other error | Raise, let Databricks job retry policy handle it |

---

## Deliverables

### 1. Project Structure

```
sharepoint_ingestion/
├── sharepoint_ingestion/
│   ├── __init__.py
│   ├── auth.py           # get_access_token()
│   ├── graph.py          # fetch_delta_changes(), download_file()
│   ├── storage.py        # read_token(), write_token(), write_file()
│   └── job.py            # run() — orchestrates the full pipeline
├── tests/
│   ├── conftest.py
│   ├── test_auth.py
│   ├── test_graph.py
│   ├── test_storage.py
│   └── test_job.py
├── resources/
│   └── sharepoint_ingestion_job.yml
├── databricks.yml        # asset bundle root config
├── pyproject.toml
└── README.md
```

---

### 2. Module Responsibilities

#### `auth.py`
Single function. Takes tenant_id, client_id, client_secret. Returns a Bearer token string.
Uses `msal.ConfidentialClientApplication`. No state.

```python
def get_access_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    ...
```

---

#### `graph.py`
Two functions. No state. Pure HTTP.

```python
def fetch_delta_changes(
    access_token: str,
    drive_id: str,
    folder_item_id: str | None,
    delta_link: str | None,
    file_ext_filter: list[str] | None
) -> tuple[list[dict], str]:
    """
    Returns (filtered_items, new_delta_link).
    Handles pagination internally.
    Raises on 401, 410, unrecoverable errors.
    Retries on 429 with Retry-After.
    """
    ...

def download_file(download_url: str) -> bytes:
    """
    Downloads file binary from pre-authenticated Graph API URL.
    Returns raw bytes.
    """
    ...
```

---

#### `storage.py`
Three functions. Keeps Spark and file I/O isolated from business logic.

```python
def read_token(spark, table: str, drive_id: str) -> str | None:
    """Returns delta_link string or None if no row exists."""
    ...

def write_token(spark, table: str, drive_id: str, delta_link: str, status: str) -> None:
    """Upserts a single row by drive_id using replaceWhere."""
    ...

def write_file(base_path: str, library_name: str, file_id: str, file_name: str, content: bytes) -> str:
    """
    Writes bytes to raw layer path.
    Returns the full path written.
    Uses today's UTC date for the date partition.
    """
    ...
```

---

#### `job.py`
The entrypoint. Orchestrates the pipeline by calling the other modules in order.
Contains a single `run()` function. Reads job parameters from `sys.argv` or `dbutils.widgets`.

```python
def run(spark, params: dict, secrets: dict) -> None:
    """
    Full pipeline:
    1. Read token from state table
    2. Fetch delta changes from Graph API
    3. Download and write each file to raw layer
    4. Commit new token on success
    Raises on any unrecoverable error — does NOT commit token on partial failure.
    """
    ...
```

---

### 3. Testing Requirements

**Framework:** `pytest`  
**Coverage target:** 100%  
**No real HTTP calls, no real Spark, no real filesystem in tests.**  
Use `unittest.mock` / `pytest-mock` throughout. Keep fixtures in `conftest.py`.

#### `test_auth.py`
- Token returned on successful MSAL call
- Exception raised when MSAL returns an error (no `access_token` in response)

#### `test_graph.py`
- Single-page delta response returns correct items and delta link
- Multi-page response (nextLink pagination) is fully collected
- Items with `deleted` flag are skipped
- Folder items (no `file` key) are skipped
- Extension filter excludes non-matching files
- Extension filter of `None` includes all files
- 429 response retries and succeeds after wait
- 429 response raises after max retries exceeded
- 410 response raises `TokenExpiredError` (custom exception)
- 401 response raises immediately
- `download_file()` returns bytes on 200
- `download_file()` raises on non-200

#### `test_storage.py`
- `read_token()` returns `None` when table has no row for `drive_id`
- `read_token()` returns `delta_link` string when row exists
- `write_token()` calls Spark write with correct schema and `replaceWhere`
- `write_file()` writes to correct path given inputs
- `write_file()` returns the full resolved path

#### `test_job.py`
- Full happy path: token read → changes fetched → files written → token committed
- No files in delta (0 items): token still committed, no writes
- File download failure raises and token is NOT committed
- 410 error resets token to NULL and raises

---

### 4. Databricks Asset Bundle — `resources/sharepoint_ingestion_job.yml`

```yaml
resources:
  jobs:
    sharepoint_ingestion:
      name: sharepoint_file_ingestion

      schedule:
        quartz_cron_expression: "0 0 6 * * ?"
        timezone_id: UTC
        pause_status: UNPAUSED

      job_clusters:
        - job_cluster_key: ingestion_cluster
          new_cluster:
            spark_version: 15.4.x-scala2.12
            node_type_id: Standard_DS3_v2
            num_workers: 0                        # single-node
            spark_conf:
              spark.master: local[*, 4]
            azure_attributes:
              availability: ON_DEMAND_AZURE

      tasks:
        - task_key: ingest
          job_cluster_key: ingestion_cluster
          python_wheel_task:
            package_name: sharepoint_ingestion
            entry_point: run
          libraries:
            - whl: dist/sharepoint_ingestion-*.whl

      parameters:
        - name: tenant_domain
          default: ""
        - name: site_id
          default: ""
        - name: drive_id
          default: ""
        - name: folder_item_id
          default: ""                             # empty = full library
        - name: raw_base_path
          default: ""
        - name: library_name
          default: ""
        - name: file_ext_filter
          default: ""                             # empty = all files
        - name: state_table
          default: "{catalog}.{schema}.sp_delta_token"

      health:
        rules:
          - metric: RUN_DURATION_SECONDS
            op: GREATER_THAN
            value: 1800                           # alert if > 30 min

      email_notifications:
        on_failure:
          - data-platform-alerts@my-company.com
```

---

### 5. `databricks.yml` (Asset Bundle Root)

```yaml
bundle:
  name: sharepoint_ingestion

workspace:
  host: https://{workspace}.azuredatabricks.net

include:
  - resources/*.yml

targets:
  dev:
    mode: development
    default: true

  prod:
    mode: production
    workspace:
      host: https://{workspace-prod}.azuredatabricks.net
```

---

### 6. `pyproject.toml`

```toml
[build-system]
requires = ["setuptools"]
build-backend = "setuptools.backends.legacy:build"

[project]
name = "sharepoint_ingestion"
version = "1.0.0"
requires-python = ">=3.10"
dependencies = [
    "msal>=1.28",
    "requests>=2.31",
]

[project.scripts]
run = "sharepoint_ingestion.job:run"

# ── Dev dependencies (uv) ─────────────────────────────────────
[tool.uv]
dev-dependencies = [
    "pytest>=8.0",
    "pytest-mock>=3.14",
    "pytest-cov>=5.0",
    "ruff>=0.4",
]

# ── Pytest ────────────────────────────────────────────────────
[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "--cov=sharepoint_ingestion --cov-report=term-missing --cov-fail-under=100"

# ── Coverage ──────────────────────────────────────────────────
[tool.coverage.run]
source = ["sharepoint_ingestion"]
branch = true

[tool.coverage.report]
fail_under = 100
show_missing = true

# ── Ruff — linting & formatting ───────────────────────────────
[tool.ruff]
target-version = "py310"
line-length = 88

[tool.ruff.lint]
select = [
    "E",    # pycodestyle errors
    "F",    # pyflakes
    "I",    # isort
    "UP",   # pyupgrade
    "B",    # flake8-bugbear
    "SIM",  # flake8-simplify
]
ignore = []

[tool.ruff.format]
quote-style = "double"
indent-style = "space"
```

---

### 7. Developer Setup

```bash
# 1. Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 2. Create virtual environment and install all dependencies
uv sync

# 3. Run tests with coverage
uv run pytest

# 4. Lint
uv run ruff check .

# 5. Format
uv run ruff format .

# 6. Lint + format in one pass (CI)
uv run ruff check . && uv run ruff format --check .

# 7. Build wheel for Databricks deployment
uv build
```

> **Note:** `uv sync` installs both main and dev dependencies from `pyproject.toml` into the local `.venv`. No manual `pip install` or `requirements.txt` needed.

---

## Implementation Order (TDD)

Follow this sequence strictly. Write the test first, then the implementation, then move on.

```
1. auth.py         → test_auth.py
2. graph.py        → test_graph.py       (fetch_delta_changes first, then download_file)
3. storage.py      → test_storage.py
4. job.py          → test_job.py
5. resources/      → sharepoint_ingestion_job.yml + databricks.yml
```

At each step: **red → green → refactor**. Do not move to the next module until:
- `uv run pytest` passes with 100% coverage on the current module
- `uv run ruff check .` returns no errors
- `uv run ruff format --check .` returns no errors

---

## Constraints & Guard Rails

- **No business logic in `job.py`** — it only wires modules together. All logic lives in `auth`, `graph`, and `storage`.
- **No Spark in `graph.py` or `auth.py`** — keep HTTP and Spark concerns fully separated.
- **No hardcoded paths, secrets, or table names** — everything comes from parameters or secrets.
- **One custom exception only:** `TokenExpiredError` in `graph.py` for the 410 case. Use standard exceptions everywhere else (`ValueError`, `RuntimeError`, `requests.HTTPError`).
- **`write_token()` is called exactly once per run** — at the end of `job.py`, only inside the success path. Never in a loop, never in a partial state.
- **Do not use `dbutils` inside library modules** — only in the job entrypoint. Keeps the library testable without a Databricks runtime.
- **`ruff check` and `ruff format` must pass at every step** — treat linting errors as build failures, not warnings.
