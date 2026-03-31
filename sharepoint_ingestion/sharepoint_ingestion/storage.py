"""Storage — Spark Delta Lake token state and raw layer file writes."""

from __future__ import annotations

import os
import re
import tempfile
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
    dbutils=None,
) -> str:
    """Write bytes to the raw layer and return the full path written.

    Two backends are supported based on the base_path scheme:

    - UC Volume (POSIX path, e.g. ``/Volumes/catalog/schema/volume``):
      Written via standard Python file I/O using pathlib.  UC Volumes are
      mounted as POSIX paths on Databricks clusters — no credentials needed.

    - Cloud URI (e.g. ``abfss://container@account.dfs.core.windows.net``):
      Pass the Databricks ``dbutils`` object.  The file is staged in a local
      temp file and copied via ``dbutils.fs.cp()``, which uses the cluster's
      ambient credentials (managed identity, instance profile, UC external
      location)::

          write_file(base_path, ..., dbutils=dbutils)

    Any other base_path format raises ``ValueError``.
    """
    now = datetime.now(timezone.utc)
    yyyy = now.strftime("%Y")
    mm = now.strftime("%m")
    dd = now.strftime("%d")
    file_segment = f"{file_id}_{file_name}"

    if "://" in base_path:
        if dbutils is None:
            raise ValueError(
                "dbutils must be provided when base_path is a cloud URI"
            )
        path = "/".join(
            [base_path.rstrip("/"), library_name, yyyy, mm, dd, file_segment]
        )
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(content)
            local_path = tmp.name
        try:
            dbutils.fs.cp(f"file://{local_path}", path)
        finally:
            os.unlink(local_path)
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
