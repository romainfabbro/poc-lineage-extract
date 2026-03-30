"""Job entrypoint — orchestrates the SharePoint file ingestion pipeline."""

from __future__ import annotations

from sharepoint_ingestion.auth import get_access_token
from sharepoint_ingestion.graph import (
    TokenExpiredError,
    download_file,
    fetch_delta_changes,
)
from sharepoint_ingestion.storage import read_token, write_file, write_token

_REQUIRED_PARAMS = ("drive_id", "state_table", "raw_base_path", "library_name")


def _validate_params(params: dict) -> None:
    for key in _REQUIRED_PARAMS:
        if not params.get(key):
            raise ValueError(f"Missing required job parameter: '{key}'")


def run(spark, params: dict, secrets: dict) -> None:
    """Full pipeline: read token → fetch changes → write files → commit token.

    Raises on any unrecoverable error — does NOT commit token on partial failure.
    On 410 (token expired), resets the stored token to NULL and raises.

    Partial-failure note: if a file download raises mid-loop, files already
    written in this run are persisted at their deterministic paths
    ({file_id}_{file_name}). The next run replays from the same delta token
    and overwrites them idempotently. This relies on the path template staying
    stable — any change to the naming scheme would break this guarantee.
    """
    _validate_params(params)

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

    # For cloud URIs (abfss://...) authenticate with the same SPN used for
    # SharePoint.  For POSIX paths (UC Volumes) no credentials are needed.
    storage_options: dict | None = (
        {
            "tenant_id": secrets["spn-tenant-id"],
            "client_id": secrets["spn-client-id"],
            "client_secret": secrets["spn-client-secret"],
        }
        if "://" in raw_base_path
        else None
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
        write_file(
            raw_base_path,
            library_name,
            item["id"],
            item["name"],
            content,
            storage_options,
        )

    write_token(spark, state_table, drive_id, new_delta_link, "success")
