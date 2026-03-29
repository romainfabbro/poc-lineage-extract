"""Storage — Spark Delta Lake token state and raw layer file writes."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

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
) -> str:
    """Write bytes to a UC Volume path and return the full path written.

    base_path must be an absolute POSIX path, e.g. /Volumes/catalog/schema/volume.
    UC Volumes are mounted as POSIX paths on Databricks clusters and support
    standard Python file I/O. Cloud URI schemes (abfss://, s3://, dbfs:/) are
    not supported — use a UC Volume mount instead.
    """
    if not base_path.startswith("/"):
        raise ValueError(
            "base_path must be an absolute POSIX path "
            f"(e.g. /Volumes/catalog/schema/volume), got: {base_path!r}"
        )
    now = datetime.now(timezone.utc)
    yyyy = now.strftime("%Y")
    mm = now.strftime("%m")
    dd = now.strftime("%d")
    path = Path(base_path) / library_name / yyyy / mm / dd / f"{file_id}_{file_name}"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return str(path)
