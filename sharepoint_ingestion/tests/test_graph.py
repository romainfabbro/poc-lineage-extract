"""Tests for graph.py — fetch_delta_changes() and download_file()."""

import pytest

from sharepoint_ingestion.graph import (
    TokenExpiredError,
    download_file,
    fetch_delta_changes,
)

TOKEN = "bearer-token"
DRIVE_ID = "drive-001"
FOLDER_ITEM_ID = "folder-item-001"
DELTA_LINK = (
    "https://graph.microsoft.com/v1.0"
    "/drives/drive-001/items/folder-item-001/delta?token=abc"
)
HEADERS = {"Authorization": f"Bearer {TOKEN}"}


# ── Helpers ───────────────────────────────────────────────────


def make_file_item(name: str, item_id: str = "id1") -> dict:
    return {
        "id": item_id,
        "name": name,
        "file": {},
        "@microsoft.graph.downloadUrl": "https://dl.example.com/file",
    }


def make_folder_item(name: str) -> dict:
    return {"id": "fid", "name": name}


def make_deleted_item(item_id: str = "del1") -> dict:
    return {"id": item_id, "deleted": {"state": "deleted"}}


def make_response(
    items: list,
    next_link: str | None = None,
    delta_link: str | None = None,
) -> dict:
    resp: dict = {"value": items}
    if next_link:
        resp["@odata.nextLink"] = next_link
    if delta_link:
        resp["@odata.deltaLink"] = delta_link
    return resp


# ── fetch_delta_changes ───────────────────────────────────────


class TestFetchDeltaChanges:
    def test_single_page_returns_items_and_delta_link(self, mocker):
        item = make_file_item("report.xlsx")
        resp = make_response([item], delta_link=DELTA_LINK)
        mock_get = mocker.patch("sharepoint_ingestion.graph.requests.get")
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = resp

        items, new_link = fetch_delta_changes(
            TOKEN, DRIVE_ID, FOLDER_ITEM_ID, None, None
        )

        assert items == [item]
        assert new_link == DELTA_LINK

    def test_multi_page_collects_all_items(self, mocker):
        item1 = make_file_item("a.xlsx", "id1")
        item2 = make_file_item("b.xlsx", "id2")
        page1 = make_response([item1], next_link="https://graph.microsoft.com/next")
        page2 = make_response([item2], delta_link=DELTA_LINK)

        mock_get = mocker.patch("sharepoint_ingestion.graph.requests.get")
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.side_effect = [page1, page2]

        items, new_link = fetch_delta_changes(
            TOKEN, DRIVE_ID, FOLDER_ITEM_ID, None, None
        )

        assert items == [item1, item2]
        assert new_link == DELTA_LINK

    def test_deleted_items_are_skipped(self, mocker):
        deleted = make_deleted_item()
        resp = make_response([deleted], delta_link=DELTA_LINK)
        mock_get = mocker.patch("sharepoint_ingestion.graph.requests.get")
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = resp

        items, _ = fetch_delta_changes(TOKEN, DRIVE_ID, FOLDER_ITEM_ID, None, None)

        assert items == []

    def test_folder_items_are_skipped(self, mocker):
        folder = make_folder_item("SubFolder")
        resp = make_response([folder], delta_link=DELTA_LINK)
        mock_get = mocker.patch("sharepoint_ingestion.graph.requests.get")
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = resp

        items, _ = fetch_delta_changes(TOKEN, DRIVE_ID, FOLDER_ITEM_ID, None, None)

        assert items == []

    def test_extension_filter_excludes_non_matching(self, mocker):
        xlsx = make_file_item("report.xlsx", "id1")
        csv = make_file_item("data.csv", "id2")
        pdf = make_file_item("scan.pdf", "id3")
        resp = make_response([xlsx, csv, pdf], delta_link=DELTA_LINK)
        mock_get = mocker.patch("sharepoint_ingestion.graph.requests.get")
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = resp

        items, _ = fetch_delta_changes(
            TOKEN, DRIVE_ID, FOLDER_ITEM_ID, None, [".xlsx", ".csv"]
        )

        assert items == [xlsx, csv]

    def test_extension_filter_none_includes_all(self, mocker):
        xlsx = make_file_item("report.xlsx", "id1")
        pdf = make_file_item("scan.pdf", "id2")
        resp = make_response([xlsx, pdf], delta_link=DELTA_LINK)
        mock_get = mocker.patch("sharepoint_ingestion.graph.requests.get")
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = resp

        items, _ = fetch_delta_changes(TOKEN, DRIVE_ID, None, None, None)

        assert items == [xlsx, pdf]

    def test_uses_existing_delta_link_as_start_url(self, mocker):
        item = make_file_item("update.xlsx")
        resp = make_response([item], delta_link=DELTA_LINK)
        mock_get = mocker.patch("sharepoint_ingestion.graph.requests.get")
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = resp

        fetch_delta_changes(TOKEN, DRIVE_ID, FOLDER_ITEM_ID, DELTA_LINK, None)

        mock_get.assert_called_once_with(DELTA_LINK, headers=HEADERS, timeout=30)

    def test_first_run_with_folder_builds_url(self, mocker):
        resp = make_response([], delta_link=DELTA_LINK)
        mock_get = mocker.patch("sharepoint_ingestion.graph.requests.get")
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = resp

        fetch_delta_changes(TOKEN, DRIVE_ID, FOLDER_ITEM_ID, None, None)

        expected_url = (
            f"https://graph.microsoft.com/v1.0"
            f"/drives/{DRIVE_ID}/items/{FOLDER_ITEM_ID}/delta"
        )
        mock_get.assert_called_once_with(expected_url, headers=HEADERS, timeout=30)

    def test_first_run_without_folder_uses_root(self, mocker):
        resp = make_response([], delta_link=DELTA_LINK)
        mock_get = mocker.patch("sharepoint_ingestion.graph.requests.get")
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = resp

        fetch_delta_changes(TOKEN, DRIVE_ID, None, None, None)

        expected_url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/root/delta"
        mock_get.assert_called_once_with(expected_url, headers=HEADERS, timeout=30)

    def test_429_retries_and_succeeds(self, mocker):
        item = make_file_item("report.xlsx")
        ok_resp = make_response([item], delta_link=DELTA_LINK)

        mock_429 = mocker.MagicMock()
        mock_429.status_code = 429
        mock_429.headers = {"Retry-After": "0"}

        mock_ok = mocker.MagicMock()
        mock_ok.status_code = 200
        mock_ok.json.return_value = ok_resp

        mocker.patch(
            "sharepoint_ingestion.graph.requests.get",
            side_effect=[mock_429, mock_ok],
        )
        mocker.patch("sharepoint_ingestion.graph.time.sleep")

        items, _ = fetch_delta_changes(TOKEN, DRIVE_ID, FOLDER_ITEM_ID, None, None)

        assert items == [item]

    def test_429_raises_after_max_retries(self, mocker):
        mock_429 = mocker.MagicMock()
        mock_429.status_code = 429
        mock_429.headers = {"Retry-After": "0"}

        mocker.patch("sharepoint_ingestion.graph.requests.get", return_value=mock_429)
        mocker.patch("sharepoint_ingestion.graph.time.sleep")

        with pytest.raises(RuntimeError, match="Rate limit exceeded after"):
            fetch_delta_changes(TOKEN, DRIVE_ID, FOLDER_ITEM_ID, None, None)

    def test_410_raises_token_expired_error(self, mocker):
        mock_410 = mocker.MagicMock()
        mock_410.status_code = 410

        mocker.patch("sharepoint_ingestion.graph.requests.get", return_value=mock_410)

        with pytest.raises(TokenExpiredError):
            fetch_delta_changes(TOKEN, DRIVE_ID, FOLDER_ITEM_ID, None, None)

    def test_401_raises_immediately(self, mocker):
        mock_401 = mocker.MagicMock()
        mock_401.status_code = 401
        mock_401.raise_for_status.side_effect = Exception("401 Unauthorized")

        mocker.patch("sharepoint_ingestion.graph.requests.get", return_value=mock_401)

        with pytest.raises(Exception, match="401 Unauthorized"):
            fetch_delta_changes(TOKEN, DRIVE_ID, FOLDER_ITEM_ID, None, None)


# ── download_file ─────────────────────────────────────────────


class TestDownloadFile:
    def test_returns_bytes_on_200(self, mocker):
        mock_resp = mocker.MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = b"file-bytes"
        mocker.patch("sharepoint_ingestion.graph.requests.get", return_value=mock_resp)

        result = download_file("https://dl.example.com/file")

        assert result == b"file-bytes"

    def test_raises_on_non_200(self, mocker):
        mock_resp = mocker.MagicMock()
        mock_resp.status_code = 403
        mock_resp.raise_for_status.side_effect = Exception("403 Forbidden")
        mocker.patch("sharepoint_ingestion.graph.requests.get", return_value=mock_resp)

        with pytest.raises(Exception, match="403 Forbidden"):
            download_file("https://dl.example.com/file")
