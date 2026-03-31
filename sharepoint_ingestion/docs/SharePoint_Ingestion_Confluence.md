# SharePoint File Ingestion Pipeline — Decision & Solution Design

**Document type:** ADR + Solution Design  
**Status:** ✅ Accepted  
**Version:** 1.0  
**Date:** 2026-03-29  
**Author:** Data Platform Engineering  
**Reviewed by:** _[Tech Lead / Architect name]_  
**Confluence space:** Data Platform / Architecture Decisions

---

> **Summary:** This document records the architectural decision to implement SharePoint Online file ingestion using a Databricks job and the Microsoft Graph API delta endpoint. It covers the problem context, all options evaluated, the decision rationale, and the approved solution design. It is intended as a permanent reference for engineering teams and management.

---

## Table of Contents

1. [Context & Problem Statement](#1-context--problem-statement)
2. [Requirements](#2-requirements)
3. [Options Evaluated](#3-options-evaluated)
4. [Decision](#4-decision)
5. [Option Comparison](#5-option-comparison)
6. [Solution Design](#6-solution-design)
7. [Consequences](#7-consequences)
8. [Open Questions & Actions](#8-open-questions--actions)

---

## 1. Context & Problem Statement

The data platform needs to ingest files from a **SharePoint Online document library** into the raw zone of our ADLS Gen2 data lake on a **daily basis**. Files in the source library are updated or created infrequently — typically once per week or once per month.

This ingestion must be **incremental** (detect only new or modified files, not re-ingest the full library on every run), **idempotent** (safe to rerun without duplicating data), and **observable** through our existing Datadog setup.

The question driving this decision was: **which Azure service or combination of services should own this pipeline**, given our current platform stack and long-term direction?

### Current Platform Stack

| Layer | Tool |
|---|---|
| Cloud | Azure |
| Compute / Orchestration (primary) | Azure Databricks |
| Transformation | dbt |
| Warehouse / Serving | Azure SQL Server |
| Legacy Orchestration | Azure Data Factory (being phased out) |
| Observability | Datadog |
| Other services in use | Azure Functions, Azure Key Vault |

### Platform Direction

The platform is actively consolidating onto **Databricks as the primary compute and orchestration layer**. ADF is being treated as legacy — no new pipelines should be built on ADF unless there is no viable alternative. The engineering team is majority Databricks-oriented.

---

## 2. Requirements

### Functional

- Detect new and modified files in a SharePoint Online document library
- Copy files to ADLS Gen2 raw zone (path: `/raw/sharepoint/{library}/{year}/{month}/{day}/`)
- Maintain incremental state — do not re-ingest files already processed
- Support manual full reload when required

### Non-Functional

- **SLA:** Files available in raw zone within 30 minutes of daily job start (06:00 UTC)
- **Idempotency:** Rerunning any day must produce the same result
- **Observability:** Job success/failure, file counts, and duration visible in Datadog
- **No new Azure services:** Solution must use services already in the platform stack
- **Cost:** Minimal — this is a lightweight, low-frequency pipeline

---

## 3. Options Evaluated

Three options were considered. Each was assessed against the requirements and platform constraints.

---

### Option A — ADF + Logic Apps (Event-Driven)

**Description:** Use a SharePoint webhook via Microsoft Graph API to trigger a Logic App, which calls an ADF pipeline to copy the file.

**Why it was rejected:**

Logic Apps are **not part of the current platform stack**. Introducing them would add a new service requiring its own deployment process, monitoring setup, and operational knowledge. The event-driven nature is also unnecessary given the low file frequency — true event-driven ingestion adds complexity without meaningful benefit when files arrive weekly or monthly.

**Verdict:** ❌ Rejected — introduces new technology, over-engineered for the use case.

---

### Option B — ADF Scheduled with Native Incremental (Polling)

**Description:** Use an ADF tumbling window trigger on a daily schedule. A `GetMetadata` activity lists the SharePoint library, filters by `lastModifiedDateTime > watermark`, copies new files, and updates the watermark in Azure SQL via a Lookup/stored procedure.

**Strengths:**
- No new services — ADF is already in the stack
- Native SharePoint connector in ADF

**Weaknesses:**
- ADF is the **legacy orchestration layer** — deepening this dependency contradicts platform direction
- ADF SharePoint connector is fragile: breaks on files > 250 MB, opaque throttling behavior, limited error context
- State management (watermark via Lookup activity + stored proc) is clunky and hard to test
- Datadog observability requires significant custom instrumentation — ADF's native metrics are coarse
- Timestamp-based watermarking is less reliable than Graph API's native delta mechanism

**Verdict:** ❌ Rejected — viable technically but moves against platform direction and introduces operational fragility.

---

### Option C — Azure Function (Timer-Triggered, Standalone)

**Description:** A Timer-triggered Azure Function runs daily, calls the Graph API delta endpoint, manages deltaLink state in Azure SQL, downloads files, and writes them to ADLS. ADF is not involved.

**Strengths:**
- Azure Functions are already in the platform stack
- Near-zero cost on consumption plan (millisecond startup, pay-per-execution)
- Lightweight — no cluster spin-up overhead for a job that often ingests 0 files
- Straightforward for HTTP-based orchestration patterns

**Weaknesses:**
- **10-minute timeout on consumption plan** — risk if Graph API throttling causes retries on a growing library. Solvable with Premium plan but adds cost and complexity.
- Azure Functions are not the primary platform — this creates a permanent exception to the Databricks-first direction
- No Unity Catalog lineage for data written from a Function
- Team is Databricks-oriented — Functions will be maintained with less confidence and familiarity
- Fragmented observability: requires separate App Insights configuration alongside Datadog

**Verdict:** ⚠️ Acceptable but not preferred — technically sound, but misaligns with platform direction and team orientation.

---

### Option D — Databricks Job (Daily Scheduled)

**Description:** A Databricks job runs on a daily schedule, calls the Microsoft Graph API delta endpoint to detect changes, manages deltaLink state in Azure SQL, downloads new/modified files, and writes them to ADLS Gen2 raw zone.

**Strengths:**
- Databricks is the **primary platform** — no new technology introduced
- Team is majority Databricks-oriented — higher confidence in implementation and maintenance
- Graph API delta endpoint is the native Microsoft mechanism for incremental sync — more reliable than timestamp watermarking
- Full Datadog observability with custom metrics natively supported
- Unity Catalog lineage for raw zone writes (if Unity Catalog is enabled)
- Idempotency is straightforward to implement in Python
- No timeout ceiling
- Aligns with platform consolidation direction

**Weaknesses:**
- Job cluster spin-up: 3–5 minutes startup time per run (negligible for daily SLA)
- Daily cost of ~$0.20–0.50 even on idle runs (vs ~$0.00 for a Function) — acceptable at this scale

**Verdict:** ✅ Selected.

---

## 4. Decision

> **We will implement the SharePoint ingestion pipeline as a Databricks job using the Microsoft Graph API delta endpoint for incremental change detection.**

### ADR-001: SharePoint Ingestion — Databricks Job over Azure Function

| | |
|---|---|
| **Status** | Accepted |
| **Decided by** | Data Platform Engineering |
| **Date** | 2026-03-29 |

**Context:** The platform needed an incremental SharePoint ingestion pipeline. Multiple options were evaluated against the requirements and the platform's Databricks-first consolidation direction.

**Decision:** Databricks job with Graph API delta endpoint (Option D).

**Rationale:** While an Azure Function is technically viable and marginally cheaper at idle, Databricks is the established primary platform and the team's dominant skillset. Every exception to the Databricks-first direction creates long-term maintenance surface area. The cost difference is negligible at this scale. Team orientation is the deciding factor — a pipeline owned by people confident in the tooling is more reliable than a cheaper pipeline maintained reluctantly.

**What was explicitly ruled out:** ADF + Azure Function together. ADF adds no value in this pattern — if a Function approach had been chosen, ADF would have been excluded entirely.

---

## 5. Option Comparison

| Dimension | ADF + Logic Apps | ADF Scheduled | Azure Function | **Databricks Job** |
|---|---|---|---|---|
| New technology introduced | ❌ Yes (Logic Apps) | ❌ No | ❌ No | ❌ No |
| Aligns with platform direction | ❌ No | ❌ No (legacy) | ⚠️ Partial | ✅ Yes |
| Team familiarity | ❌ Low | ⚠️ Medium | ⚠️ Medium | ✅ High |
| Incremental mechanism | ⚠️ Webhook (complex) | ⚠️ Timestamp watermark | ✅ Graph API delta | ✅ Graph API delta |
| Idempotency | ⚠️ Complex | ⚠️ Fragile | ✅ Straightforward | ✅ Straightforward |
| Observability (Datadog) | ❌ Fragmented | ❌ Weak | ⚠️ Extra setup | ✅ Native |
| Cost (daily) | ~$0.10–0.30 | ~$0.10–0.30 | ~$0.00 | ~$0.20–0.50 |
| Startup latency | Low | Low | ~0ms | 3–5 min |
| Execution timeout risk | None | None | ⚠️ 10 min (consumption) | None |
| Unity Catalog lineage | ❌ No | ❌ No | ❌ No | ✅ Yes |
| **Overall verdict** | ❌ Rejected | ❌ Rejected | ⚠️ Acceptable | ✅ **Selected** |

---

## 6. Solution Design

### 6.1 Architecture Overview

```
┌─────────────────────────────────┐
│   SharePoint Online             │
│   Document Library              │
└────────────┬────────────────────┘
             │ Graph API
             │ GET /drives/{id}/root/delta
             ▼
┌─────────────────────────────────┐
│   Databricks Job                │
│   Daily @ 06:00 UTC             │
│   Single-node job cluster       │
└────┬───────────────────┬────────┘
     │                   │
     ▼                   ▼
┌──────────────┐   ┌─────────────────────────────┐
│  Azure SQL   │   │  ADLS Gen2 — Raw Zone        │
│  State table │   │  /raw/sharepoint/{lib}/      │
│  (deltaLink) │   │  {year}/{month}/{day}/       │
└──────────────┘   └──────────────┬──────────────┘
                                  │
                                  ▼
                   ┌─────────────────────────────┐
                   │  dbt Transformation          │
                   │  (existing pipeline)         │
                   └──────────────┬──────────────┘
                                  │
                                  ▼
                   ┌─────────────────────────────┐
                   │  Azure SQL — Serving Layer   │
                   └─────────────────────────────┘

             All runs → Datadog (metrics + alerts)
```

---

### 6.2 Key Design Choice — Graph API Delta Endpoint

The pipeline uses the Microsoft Graph API **delta endpoint** (`/drives/{id}/root/delta`) rather than listing the full library and comparing timestamps.

The delta endpoint returns only items that have changed since the last call, identified by a **deltaLink token** stored in Azure SQL. This is Microsoft's native mechanism for incremental sync and is significantly more reliable than timestamp watermarking because:

- No clock skew or timezone edge cases
- No risk of missing files modified between runs
- Single lightweight HTTP call returns exactly what changed
- The token encodes Microsoft's server-side state — not our interpretation of it

**deltaLink commit rule:** The new token is only written to Azure SQL **after the full run completes successfully**. If the job fails mid-run, the previous token is retained and the next run re-processes from the last safe checkpoint.

---

### 6.3 State Tables (Azure SQL)

**`sharepoint_ingestion_config`** — one row per library, holds the deltaLink token and last run metadata.

```sql
CREATE TABLE sharepoint_ingestion_config (
    config_id        INT IDENTITY PRIMARY KEY,
    site_id          NVARCHAR(255)  NOT NULL,
    drive_id         NVARCHAR(255)  NOT NULL,
    library_name     NVARCHAR(255)  NOT NULL,
    delta_link       NVARCHAR(MAX)  NULL,       -- NULL = first run (full sync)
    last_run_at      DATETIME2      NULL,
    last_run_status  NVARCHAR(50)   NULL,       -- success | failed | partial
    updated_at       DATETIME2      DEFAULT GETUTCDATE()
);
```

**`sharepoint_ingestion_state`** — one row per file version ingested, used for deduplication and audit.

```sql
CREATE TABLE sharepoint_ingestion_state (
    state_id              BIGINT IDENTITY PRIMARY KEY,
    config_id             INT           NOT NULL,
    file_id               NVARCHAR(255) NOT NULL,
    file_name             NVARCHAR(512) NOT NULL,
    last_modified_dt      DATETIME2     NOT NULL,
    adls_path             NVARCHAR(MAX) NOT NULL,
    ingested_at           DATETIME2     DEFAULT GETUTCDATE(),
    status                NVARCHAR(50)  NOT NULL,
    error_message         NVARCHAR(MAX) NULL,
    CONSTRAINT uq_file_modified UNIQUE (config_id, file_id, last_modified_dt)
);
```

---

### 6.4 Job Logic (Pseudocode)

```python
# 1. Read deltaLink and config from Azure SQL
config = read_config(config_id)

# 2. Authenticate via MSAL (service principal, credentials from Key Vault)
token = get_access_token()

# 3. Call Graph API delta endpoint
changes = call_delta_endpoint(token, config.drive_id, config.delta_link)

# 4. Process changes
for item in changes['value']:
    if item.get('deleted'): continue        # skip deletions
    if not item.get('file'): continue       # skip folders
    if already_ingested(item): continue     # idempotency check

    content = download_file(token, item['id'])
    adls_path = write_to_adls(content, item, run_date)
    record_state(item, adls_path, status='success')

# 5. Commit new deltaLink — only on full success
update_delta_link(config_id, changes['@odata.deltaLink'])

# 6. Emit Datadog metrics
emit_metrics(files_ingested, files_skipped, errors, duration)
```

---

### 6.5 ADLS Raw Zone Path Convention

```
/raw/sharepoint/{library_name}/{year}/{month}/{day}/{file_id}_{file_name}

Example:
/raw/sharepoint/finance-reports/2026/03/29/01ABCD_Q1_Report.xlsx
```

The `file_id` prefix ensures uniqueness if a file is renamed between runs.

---

### 6.6 Databricks Job Configuration

| Property | Value |
|---|---|
| Job type | Databricks Workflow — Python wheel or notebook |
| Cluster type | Job cluster (spun up per run, auto-terminates) |
| Cluster size | Single node — Standard_DS3_v2 (4 vCPU, 14 GB RAM) |
| Schedule | Daily CRON: `0 6 * * *` (06:00 UTC) |
| Retry policy | 2 retries, 10-minute interval |
| Timeout | 60 minutes |
| Parameters | `config_id` — allows reuse across multiple libraries |
| Secrets | Azure Key Vault-backed Databricks secret scope |

---

### 6.7 Idempotency Summary

| Mechanism | How it works |
|---|---|
| deltaLink commit-on-success | Token only saved after full successful run — failed run replays from last checkpoint |
| Per-file UNIQUE constraint | `(config_id, file_id, last_modified_dt)` prevents duplicate state records |
| file_id in ADLS path | Prevents path collision if file is renamed; preserves version history |
| Manual full reload | Set `delta_link = NULL` in config table — next run performs full library sync |

---

### 6.8 Observability

**Datadog Monitors**

| Monitor | Alert Condition |
|---|---|
| Job success | Alert if daily run does not complete within 30 min of scheduled start |
| Files ingested | Warn if `files_ingested = 0` for 7 consecutive days (possible auth break) |
| Job duration | Warn if `job_duration_seconds > 1800` |
| Error count | Alert if `errors > 0` on any run |
| Missed run | Warn if `last_run_at > 25 hours` |

**Custom Metrics emitted per run**

| Metric | Description |
|---|---|
| `sharepoint.ingestion.files_ingested` | Files successfully written to ADLS |
| `sharepoint.ingestion.files_skipped` | Files already ingested (deduplication) |
| `sharepoint.ingestion.errors` | Files that failed download or write |
| `sharepoint.ingestion.job_duration_seconds` | Total wall-clock time |
| `sharepoint.ingestion.delta_pages` | Graph API pages paginated |

---

### 6.9 FinOps

This pipeline is intentionally right-sized. Expected daily cost is **< $0.50 USD** — a single-node job cluster running for 2–5 minutes. On days with zero file changes, the job runs the same cluster for the same short duration (Graph API delta call + empty loop + state update) and exits.

If file volume grows significantly (100+ files/day, large binaries >50 MB), revisit cluster sizing and consider parallelizing file downloads across Databricks tasks.

---

## 7. Consequences

### What becomes easier

- **Maintenance:** Owned entirely by the Databricks-oriented team with no context switching
- **Observability:** Single Datadog dashboard covers the full pipeline
- **Platform consistency:** No exceptions to the Databricks-first consolidation direction
- **Extensibility:** Adding new SharePoint libraries is a new row in `sharepoint_ingestion_config`, not a new pipeline
- **Lineage:** Unity Catalog lineage available end-to-end (raw → dbt → serving)

### What becomes harder or more expensive

- **Idle cost:** ~$0.20–0.50/day even when no files are ingested — negligible but non-zero
- **Startup latency:** 3–5 minute cluster spin-up means the pipeline is not suitable for near-real-time use cases (not a current requirement)
- **Graph API expertise:** Team needs familiarity with Microsoft Graph API authentication (MSAL) and the delta endpoint — low learning curve but a new dependency

### What is explicitly not changing

- Azure Functions remain in the stack for their existing use cases — this decision does not deprecate them
- ADF remains available for existing pipelines — this decision does not accelerate ADF deprecation beyond the current plan

---

## 8. Open Questions & Actions

| # | Question | Owner | Due |
|---|---|---|---|
| 1 | Confirm service principal has `Files.Read.All` on target SharePoint site | Platform Team | Before build |
| 2 | Confirm maximum expected file size — files > 4 MB require chunked download via Graph API range requests | Source System Owner | Before build |
| 3 | Are files in this library subject to PII classification or GDPR? Raw zone write policy may require encryption-at-rest tagging | Data Governance | Before build |
| 4 | Is Unity Catalog enabled for the raw zone? If yes, confirm Delta format is required over binary writes | Platform Team | Before build |
| 5 | Multiple libraries in scope? v1 supports one library via `config_id` — confirm if parallel libraries are needed in v1 or deferred to v2 | Product Owner | Before build |
| 6 | deltaLink token expiry — test behaviour if library has zero changes for > 30 days. Handle 410 Gone response with automatic fallback to full sync | Engineering | During build |

---

*This document should be kept up to date as the implementation progresses. Superseding decisions should reference this ADR by its Confluence page ID.*
