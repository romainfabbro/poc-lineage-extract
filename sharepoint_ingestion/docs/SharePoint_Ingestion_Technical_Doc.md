# Technical Documentation — SharePoint File Ingestion via Databricks

**Type:** Technical Reference  
**Status:** ✅ Approved  
**Version:** 2.3  
**Date:** 2026-03-29  
**Scope:** Raw layer ingestion only (SharePoint → ADLS Gen2 or Unity Catalog Volume)

---

## Table of Contents

1. [Overview](#1-overview)
2. [Solution Architecture](#2-solution-architecture)
3. [Sequence Design](#3-sequence-design)
4. [Parameters](#4-parameters)
5. [Raw Layer Path Convention](#5-raw-layer-path-convention)
6. [Delta Endpoint & Token Lifecycle](#6-delta-endpoint--token-lifecycle)
7. [Error Handling](#7-error-handling)

---

## 1. Overview

This pipeline ingests **new and modified files** from a scoped SharePoint Online folder into the **raw layer** of the data lake, running as a **Databricks job on a daily schedule**.

Change detection is handled entirely by the **Microsoft Graph API delta endpoint**, which returns only items modified since the last run via a persisted `deltaLink` token stored in a **Delta Lake table** inside the platform.

The pipeline is **read-only on SharePoint** and **idempotent on the raw layer**: files are written to deterministic `{file_id}_{file_name}` paths, so reprocessing the same SharePoint item overwrites its previous raw object but does not delete or mutate other files.

---

## 2. Solution Architecture

```
┌──────────────────────────────────────────────────────────┐
│  SharePoint Online                                       │
│                                                          │
│  📁 /drives/{drive_id}/items/{folder_item_id}/           │
│      ├── Q1_Report.xlsx                                  │
│      ├── Q2_Report.xlsx                                  │
│      └── Archive/                                        │
│             └── 2025_Report.xlsx                         │
└───────────────────────┬──────────────────────────────────┘
                        │ Graph API delta endpoint
                        │ OAuth 2.0 — Client Credentials (MSAL)
                        │
┌───────────────────────▼──────────────────────────────────┐
│  Databricks Job — sharepoint_file_ingestion              │
│  Daily @ 06:00 UTC — Single-node job cluster             │
│                                                          │
│   1. Read deltaLink token        ◄──  Delta Lake table    │
│   2. Authenticate via MSAL       ◄──  Key Vault           │
│   3. Call Graph API delta                                 │
│   4. Paginate nextLink chain                              │
│   5. Filter by file extension                             │
│   6. Download + write to raw layer  ──►  ADLS / Volume   │
│   7. Commit new deltaLink token  ──►  Delta Lake table    │
│   8. Emit metrics                ──►  Datadog             │
└──────────────────────────────────────────────────────────┘
          │                              │
          ▼                              ▼
┌──────────────────────┐   ┌─────────────────────────────────────┐
│  Delta Lake Table    │   │  Raw Layer                          │
│  (Unity Catalog or   │   │                                     │
│   ADLS Gen2)         │   │  abfss://{container}@{account}      │
│                      │   │  .dfs.core.windows.net/raw/         │
│  sp_delta_token      │   │  sharepoint/{library}/              │
│  (1 row per library) │   │  ingest_date={yyyy}-{mm}-{dd}/      │
│                      │   │  {file_id}_{name}                   │
│  drive_id            │   │                                     │
│  delta_link          │   │  — or —                             │
│  updated_at          │   │                                     │
│  last_status         │   │  /Volumes/{catalog}/{schema}/       │
└──────────────────────┘   │  {volume}/raw/sharepoint/           │
                           │  {library}/ingest_date={yyyy}-      │
                           │  {mm}-{dd}/{file_id}_{name}         │
                           └─────────────────────────────────────┘
```

---

## 3. Sequence Design

```
Databricks Job       Delta Lake Table      Key Vault        Graph API         Raw Layer
      │                   │                  │                │                  │
      │─ (1) read token ──►                  │                │                  │
      │   filter: drive_id│                  │                │                  │
      │◄─ delta_link ─────│                  │                │                  │
      │   (NULL=1st run)  │                  │                │                  │
      │                   │                  │                │                  │
      │─ (2) get secrets ────────────────────►                │                  │
      │◄─ client_id,      │                  │                │                  │
      │   client_secret   │                  │                │                  │
      │                   │                  │                │                  │
      │─ (3) MSAL token request ─────────────────────────────►                  │
      │◄─ Bearer access_token ────────────────────────────────│                  │
      │                   │                  │                │                  │
      │         FIRST RUN (delta_link is NULL)                │                  │
      │─ (4a) GET /drives/{drive_id}/items/{folder_item_id}/delta ──────────────►
      │                   │                  │                │                  │
      │         SUBSEQUENT RUN               │                │                  │
      │─ (4b) GET /drives/{drive_id}/items/{folder_item_id}/delta?token={...} ──►
      │                   │                  │                │                  │
      │◄─ page 1: { value:[...], @odata.nextLink } ───────────│                  │
      │─ (5) GET @odata.nextLink ─────────────────────────────►                  │
      │◄─ page 2: { value:[...], @odata.nextLink } ───────────│                  │
      │         ... repeat until no nextLink ...              │                  │
      │◄─ last page: { value:[...], @odata.deltaLink } ───────│                  │
      │                   │                  │   ↑ store this token              │
      │                   │                  │                │                  │
      │   FOR EACH item collected:           │                │                  │
      │   ─────────────────────────────────  │                │                  │
      │   skip if deleted                    │                │                  │
      │   skip if folder (no item.file)      │                │                  │
      │   skip if extension not in filter    │                │                  │
      │                   │                  │                │                  │
      │─ (6) GET /drives/{drive_id}/items/{item_id}/content ─►                  │
      │      (Graph API with Authorization: Bearer {token})   │                  │
      │◄─ file binary ────────────────────────────────────────│                  │
      │─ (7) write to raw path ──────────────────────────────────────────────── ►│
      │◄─ confirmed ────────────────────────────────────────────────────────────│ │
      │         ... repeat (6)(7) for each file ...           │                  │
      │                   │                  │                │                  │
      │─ (8) UPDATE sp_delta_token ──────────►                │                  │
      │      WHERE drive_id = drive_id       │  ← only on     │                  │
      │      delta_link = new token          │    full        │                  │
      │      last_status = 'success'         │    success     │                  │
      │─ (9) emit Datadog metrics            │                │                  │
      │                   │                  │                │                  │


  ON FAILURE (any step after step 4):
  ──────────────────────────────────────────────────────────
      │─ UPDATE sp_delta_token: last_status = 'failed' ──── ►│
      │  delta_link is NOT updated                           │
      │  → next run replays from last valid token            │
      │─ emit Datadog alert                                  │
```

---

## 4. Parameters

### 4.1 Job Parameters (Databricks runtime)

Passed as job parameters at runtime. No config table needed.

| Parameter | Type | Required | Description | Example |
|---|---|---|---|---|
| `drive_id` | `str` | ✅ | SharePoint drive ID. Resolved once at setup via Graph API. | `b!xYz123...` |
| `folder_item_id` | `str` | ❌ | Item ID of the target subfolder. Scopes delta to this folder and all descendants. Omit to ingest the full library. | `01ABCDEF...` |
| `raw_base_path` | `str` | ✅ | Base path for raw layer writes. See Section 5. | `abfss://...` or `/Volumes/...` |
| `library_name` | `str` | ✅ | Path segment namespacing files by source library. | `finance-reports` |
| `file_ext_filter` | `str` | ❌ | Comma-separated extensions to include. Empty = all files. | `.xlsx,.csv` |

---

### 4.2 State Table (`sp_delta_token`) — Delta Lake

One row per `drive_id`. This is the **entire persistence layer** of the pipeline, stored as a managed Delta Lake table — no external database dependency.

**Location (choose one based on platform setup):**

```
-- Unity Catalog (preferred)
{catalog}.{schema}.sp_delta_token

-- ADLS Gen2 without Unity Catalog
delta.`abfss://{container}@{account}.dfs.core.windows.net/platform/sp_delta_token`
```

**Schema:**

```python
# PySpark — create once
spark.sql("""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.sp_delta_token (
        drive_id     STRING       NOT NULL,  -- natural key, rename-safe
        delta_link   STRING,                 -- NULL = first run / force full reload
        updated_at   TIMESTAMP    NOT NULL,
        last_status  STRING       NOT NULL   -- success | failed
    )
    USING DELTA
    COMMENT 'SharePoint ingestion deltaLink token state'
""")
```

**Read / write pattern in the job:**

```python
from pyspark.sql import Row
from datetime import datetime, timezone

# Read token
row = spark.table("sp_delta_token").filter(f"drive_id = '{drive_id}'").first()
delta_link = row["delta_link"] if row else None

# Commit on success
token_df = spark.createDataFrame([Row(
    drive_id    = drive_id,
    delta_link  = new_delta_link,
    updated_at  = datetime.now(timezone.utc),
    last_status = "success"
)])

token_df.write.format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", f"drive_id = '{drive_id}'") \
    .saveAsTable("sp_delta_token")
```

| Column | Description |
|---|---|
| `drive_id` | Natural key — the SharePoint drive being ingested. Rename-safe GUID. |
| `delta_link` | Full deltaLink URL from the last successful run. `NULL` triggers a full folder sync. |
| `updated_at` | UTC timestamp of last update. Used for staleness monitoring in Datadog. |
| `last_status` | Last run outcome. `failed` means token was not advanced — next run replays. |

> **Force full reload:** `UPDATE sp_delta_token SET delta_link = NULL WHERE drive_id = '{drive_id}';`

---

### 4.3 Authentication — Service Principal via Azure Key Vault

The pipeline authenticates to the Microsoft Graph API using a **Service Principal (App Registration)** with a client secret stored in Azure Key Vault, accessed via a Databricks secret scope.

**Why not Managed Identity?**

Managed Identity was evaluated and rejected for this pipeline. While it eliminates secret rotation, assigning Graph API application permissions (`Sites.Read.All`) to a Managed Identity is not supported through the Azure portal — it requires Global Administrator PowerShell scripts that are fragile, underdocumented, and not reproducible via standard IaC tooling. The Databricks-specific token retrieval also requires calling the Azure IMDS endpoint directly rather than MSAL, which adds a separate code path and complicates testing. The operational cost outweighs the benefit of removing one secret.

The secret rotation concern is mitigated by a Key Vault expiry alert (90 days before expiry) and a Key Vault rotation policy — standard practice in the platform.

Revisit Managed Identity when Microsoft improves native support for this pattern in Microsoft 365.

**Secrets** — accessed via Databricks secret scope `sharepoint`, backed by Azure Key Vault. Never stored in job parameters or code.

| Secret Name | Description |
|---|---|
| `spn-tenant-id` | Azure AD tenant ID |
| `spn-client-id` | Service principal application ID |
| `spn-client-secret` | Service principal client secret |

---

## 5. Raw Layer Path Convention

```
{raw_base_path}/{library_name}/ingest_date={yyyy}-{mm}-{dd}/{file_id}_{file_name}
```

**abfss example:**
```
abfss://datalake@mystorageaccount.dfs.core.windows.net/raw/sharepoint/finance-reports/ingest_date=2026-03-29/01ABCDEF_Q1_Report.xlsx
```

**Unity Catalog Volume example:**
```
/Volumes/platform/raw/landing/raw/sharepoint/finance-reports/ingest_date=2026-03-29/01ABCDEF_Q1_Report.xlsx
```

| Segment | Source | Purpose |
|---|---|---|
| `{library_name}` | Job parameter | Namespaces files by SharePoint library |
| `ingest_date={yyyy}-{mm}-{dd}` | Job run date (UTC) | Hive-style partitioning for automatic Spark column discovery |
| `{file_id}_` | Graph API `item.id` | Guarantees uniqueness if file is renamed |
| `{file_name}` | Graph API `item.name` | Human-readable, preserves file extension |

---

## 6. Delta Endpoint & Token Lifecycle

### Endpoint

```
# First run — full folder sync:
GET https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{folder_item_id}/delta

# Subsequent runs — incremental only:
GET https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{folder_item_id}/delta?token={token}
```

### How to resolve `folder_item_id` (one-time setup)

```
GET https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{folder_path}

# Example:
GET https://graph.microsoft.com/v1.0/drives/b!xYz123.../root:/Finance/2026
→ use the "id" field from the response as folder_item_id
```

### Token Rules

| Rule | Detail |
|---|---|
| **Commit on full success only** | Token written to `sp_delta_token` Delta table only after all files are written to the raw layer |
| **Preserve on failure** | Failed run keeps previous token — next run replays the same delta |
| **NULL = full sync** | Set `delta_link = NULL` to force a complete folder reload on next run |
| **410 Gone** | Token expired (inactive library). Catch error, reset `delta_link = NULL`, alert. Next run auto-recovers. |
| **Token is opaque** | Never parse or construct the token. Store and pass exactly as returned by the API. |

### Pagination

```
GET /delta (or /delta?token=...)
    │
    ├── @odata.nextLink present? → GET nextLink → repeat
    │
    └── @odata.deltaLink present (no more pages)
            │
            └── process all collected items
                write all files to raw layer
                commit deltaLink to sp_delta_token Delta table
```

> **Important:** collect all pages before writing any file. The token committed must reflect the complete delta, not a partial one.

---

## 7. Error Handling

| Scenario | Behaviour |
|---|---|
| **429 — throttled** | Respect `Retry-After` header, retry up to 3 times |
| **410 — token expired** | Reset `delta_link = NULL`, alert Datadog, recover on next scheduled run |
| **401 — auth failure** | Fail immediately, alert Datadog — Key Vault access or SP permission issue |
| **File download failure** | Fail the job immediately on first download error, log and alert. `delta_link` not committed → same items are retried on next run. |
| **Raw layer write failure** | Same as above: fail-fast, log and alert, do not commit `delta_link`. |
| **Full job failure** | `delta_link` not updated → next run replays automatically from last valid token |

---

*For the decision rationale and option comparison, see: [SharePoint Ingestion — Decision & Solution Design](./SharePoint_Ingestion_Confluence.md)*
