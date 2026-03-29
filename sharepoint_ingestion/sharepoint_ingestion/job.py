"""Job entrypoint — orchestrates the SharePoint file ingestion pipeline."""

from __future__ import annotations

from sharepoint_ingestion.auth import get_access_token
from sharepoint_ingestion.graph import (
    TokenExpiredError,
    download_file,
    fetch_delta_changes,
)
from sharepoint_ingestion.storage import read_token, write_file, write_token


def run(spark, params: dict, secrets: dict) -> None:
    """
    Full pipeline:
    1. Read token from state table
    2. Fetch delta changes from Graph API
    3. Download and write each file to raw layer
    4. Commit new token on success

    Raises on any unrecoverable error — does NOT commit token on partial failure.
    On 410 (token expired), resets the stored token to NULL and raises.
    """
    drive_id: str = params["drive_id"]
    state_table: str = params["state_table"]
    raw_base_path: str = params["raw_base_path"]
    library_name: str = params["library_name"]
    folder_item_id: str | None = params.get("folder_item_id") or None
    raw_ext_filter: str = params.get("file_ext_filter", "")
    file_ext_filter: list[str] | None = (
        [e.strip() for e in raw_ext_filter.split(",") if e.strip()]
        if raw_ext_filter
        else None
    )

    access_token = get_access_token(
        secrets["spn-tenant-id"],
        secrets["spn-client-id"],
        secrets["spn-client-secret"],
    )

    delta_link = read_token(spark, state_table, drive_id)

    try:
        items, new_delta_link = fetch_delta_changes(
            access_token,
            drive_id,
            folder_item_id,
            delta_link,
            file_ext_filter,
        )
    except TokenExpiredError:
        write_token(spark, state_table, drive_id, None, "failed")
        raise

    for item in items:
        content = download_file(item["@microsoft.graph.downloadUrl"])
        write_file(raw_base_path, library_name, item["id"], item["name"], content)

    write_token(spark, state_table, drive_id, new_delta_link, "success")
