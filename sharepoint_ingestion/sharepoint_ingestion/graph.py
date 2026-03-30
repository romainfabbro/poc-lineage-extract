"""Graph API interactions — delta changes and file download."""

from __future__ import annotations

import time

import requests

_GRAPH_BASE = "https://graph.microsoft.com/v1.0"
_MAX_RETRIES = 3


class TokenExpiredError(Exception):
    """Raised when the Graph API returns 410 — delta token has expired."""


def fetch_delta_changes(
    access_token: str,
    drive_id: str,
    folder_item_id: str | None,
    delta_link: str | None,
    file_ext_filter: list[str] | None,
) -> tuple[list[dict], str]:
    """Return (filtered_items, new_delta_link) by paginating the delta endpoint."""
    headers = {"Authorization": f"Bearer {access_token}"}

    if delta_link:
        url: str | None = delta_link
    elif folder_item_id:
        url = f"{_GRAPH_BASE}/drives/{drive_id}/items/{folder_item_id}/delta"
    else:
        url = f"{_GRAPH_BASE}/drives/{drive_id}/root/delta"

    collected: list[dict] = []
    new_delta_link: str = ""

    while url:
        response = _get_with_retry(url, headers)
        data = response.json()

        for item in data.get("value", []):
            if item.get("deleted"):
                continue
            if "file" not in item:
                continue
            if file_ext_filter is not None:
                name: str = item.get("name", "")
                ext = "." + name.rsplit(".", 1)[-1] if "." in name else ""
                if ext.lower() not in file_ext_filter:
                    continue
            collected.append(item)

        if "@odata.deltaLink" in data:
            new_delta_link = data["@odata.deltaLink"]
            url = None
        else:
            url = data.get("@odata.nextLink")

    if not new_delta_link:
        raise RuntimeError(
            "Graph API response did not contain '@odata.deltaLink' on the final page. "
            "The response may be malformed."
        )

    return collected, new_delta_link


def download_file(download_url: str) -> bytes:
    """Download a file from a pre-authenticated Graph API URL and return its bytes."""
    response = requests.get(download_url, timeout=60)
    response.raise_for_status()
    return response.content


# ── Internal helpers ──────────────────────────────────────────


def _get_with_retry(url: str, headers: dict) -> requests.Response:
    retries = 0
    while True:
        response = requests.get(url, headers=headers, timeout=30)

        if response.status_code == 410:
            raise TokenExpiredError("Delta token expired (HTTP 410). Reset required.")

        if response.status_code == 401:
            response.raise_for_status()

        if response.status_code == 429:
            if retries >= _MAX_RETRIES:
                raise RuntimeError(f"Rate limit exceeded after {_MAX_RETRIES} retries.")
            retry_after = int(response.headers.get("Retry-After", 1))
            time.sleep(retry_after)
            retries += 1
            continue

        response.raise_for_status()
        return response
