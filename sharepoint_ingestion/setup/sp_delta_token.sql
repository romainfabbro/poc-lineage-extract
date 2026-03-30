-- State table for SharePoint delta tokens.
-- Run once per catalog/schema before deploying the ingestion job.
-- Replace {catalog} and {schema} with your target Unity Catalog values.

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.sp_delta_token (
    drive_id     STRING    NOT NULL,   -- natural key — Graph API drive GUID
    delta_link   STRING,               -- NULL on first run; set by the job
    updated_at   TIMESTAMP NOT NULL,
    last_status  STRING    NOT NULL    -- 'success' | 'failed'
)
USING DELTA
COMMENT 'Persists Microsoft Graph API delta tokens for SharePoint drive ingestion.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'false'
);
