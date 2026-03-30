"""Databricks runtime entry point.

Wires SparkSession, dbutils.widgets (job params) and dbutils.secrets into
run(). Called by Databricks python_wheel_task via the 'ingest' console script.

This module is intentionally excluded from unit-test coverage: it can only
run inside a live Databricks cluster where SparkSession and DBUtils are
available.
"""

from __future__ import annotations


def main() -> None:  # pragma: no cover
    from pyspark.dbutils import DBUtils  # type: ignore[import]
    from pyspark.sql import SparkSession  # type: ignore[import]

    from sharepoint_ingestion.job import run

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)

    params = {
        "tenant_domain": dbutils.widgets.get("tenant_domain"),
        "site_id": dbutils.widgets.get("site_id"),
        "drive_id": dbutils.widgets.get("drive_id"),
        "folder_item_id": dbutils.widgets.get("folder_item_id"),
        "raw_base_path": dbutils.widgets.get("raw_base_path"),
        "library_name": dbutils.widgets.get("library_name"),
        "file_ext_filter": dbutils.widgets.get("file_ext_filter"),
        "state_table": dbutils.widgets.get("state_table"),
    }

    secret_scope = "sharepoint"
    secrets = {
        "spn-tenant-id": dbutils.secrets.get(secret_scope, "spn-tenant-id"),
        "spn-client-id": dbutils.secrets.get(secret_scope, "spn-client-id"),
        "spn-client-secret": dbutils.secrets.get(secret_scope, "spn-client-secret"),
    }

    run(spark, params, secrets)
