"""Storage — Spark Delta Lake token state and raw layer file writes."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

import fsspec

# Graph API drive IDs are URL-safe base64 strings with an optional '!' prefix
# (e.g. "b!xYz123-abc_def"). Reject anything containing SQL special characters.
_DRIVE_ID_RE = re.compile(r"^[A-Za-z0-9!._,\-]{1,512}$")


def _validate_drive_id(drive_id: str) -> None:
    if not _DRIVE_ID_RE.match(drive_id):
        raise ValueError(f"Invalid drive_id format: {drive_id!r}")


def read_token(spark, table: str, drive_id: str) -> str | None:
    """Return the stored delta_link for drive_id, or None if no row exists."""
    _validate_drive_id(drive_id)
    rows = (
        spark.sql(f"SELECT * FROM {table}")
        .filter(f"drive_id = '{drive_id}'")
        .limit(1)
        .collect()
    )
    if not rows:
        return None
    return rows[0].delta_link


def write_token(
    spark, table: str, drive_id: str, delta_link: str | None, status: str
) -> None:
    """Upsert a single row for drive_id using replaceWhere."""
    _validate_drive_id(drive_id)
    now = datetime.now(timezone.utc)
    data = [
        {
            "drive_id": drive_id,
            "delta_link": delta_link,
            "updated_at": now,
            "last_status": status,
        }
    ]
    df = spark.createDataFrame(data)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"drive_id = '{drive_id}'")
        .saveAsTable(table)
    )


def write_file(
    base_path: str,
    library_name: str,
    file_id: str,
    file_name: str,
    content: bytes,
    storage_options: dict | None = None,
) -> str:
    """Write bytes to the raw layer and return the full path written.

    Two backends are supported based on the base_path scheme:

    - UC Volume (POSIX path, e.g. ``/Volumes/catalog/schema/volume``):
      Written via standard Python file I/O using pathlib. ``storage_options``
      is ignored.  UC Volumes are mounted as POSIX paths on Databricks clusters.

    - Azure Data Lake Storage Gen2 (``abfss://container@account.dfs.core.windows.net``):
      Written via ``fsspec`` + ``adlfs``.  Pass ``storage_options`` with service
      principal credentials::

          storage_options = {
              "tenant_id": "...",
              "client_id": "...",
              "client_secret": "...",
          }

      Install the optional extra to enable ADLS support:
      ``pip install sharepoint_ingestion[adls]``

    Any other base_path format raises ``ValueError``.
    """
    now = datetime.now(timezone.utc)
    yyyy = now.strftime("%Y")
    mm = now.strftime("%m")
    dd = now.strftime("%d")
    file_segment = f"{file_id}_{file_name}"

    if "://" in base_path:
        # Cloud path — delegate to fsspec (adlfs for abfss://, etc.)
        path = "/".join(
            [base_path.rstrip("/"), library_name, yyyy, mm, dd, file_segment]
        )
        with fsspec.open(path, "wb", **(storage_options or {})) as fh:
            fh.write(content)
    elif base_path.startswith("/"):
        # POSIX path — UC Volume or local filesystem
        p = Path(base_path) / library_name / yyyy / mm / dd / file_segment
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(content)
        path = str(p)
    else:
        raise ValueError(
            "base_path must be an absolute POSIX path "
            "(e.g. /Volumes/catalog/schema/volume) "
            "or a cloud URI (e.g. abfss://container@account.dfs.core.windows.net), "
            f"got: {base_path!r}"
        )

    return path
