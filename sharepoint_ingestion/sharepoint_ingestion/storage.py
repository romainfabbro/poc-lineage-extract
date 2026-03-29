"""Storage — Spark Delta Lake token state and raw layer file writes."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path


def read_token(spark, table: str, drive_id: str) -> str | None:
    """Return the stored delta_link for drive_id, or None if no row exists."""
    rows = (
        spark.sql(f"SELECT * FROM {table}")
        .filter(f"drive_id = '{drive_id}'")
        .limit(1)
        .collect()
    )
    if not rows:
        return None
    return rows[0].delta_link


def write_token(spark, table: str, drive_id: str, delta_link: str, status: str) -> None:
    """Upsert a single row for drive_id using replaceWhere."""
    now = datetime.utcnow()
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
    """Write bytes to the raw layer path and return the full path written."""
    now = datetime.utcnow()
    yyyy = now.strftime("%Y")
    mm = now.strftime("%m")
    dd = now.strftime("%d")
    path = Path(base_path) / library_name / yyyy / mm / dd / f"{file_id}_{file_name}"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return str(path)
