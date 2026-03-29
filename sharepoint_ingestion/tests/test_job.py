"""Tests for job.py — run() orchestration."""

from unittest.mock import MagicMock

import pytest

from sharepoint_ingestion.graph import TokenExpiredError
from sharepoint_ingestion.job import run

PARAMS = {
    "tenant_domain": "company.sharepoint.com",
    "site_id": "company.sharepoint.com,abc,def",
    "drive_id": "drive-001",
    "folder_item_id": "folder-001",
    "raw_base_path": "/mnt/raw",
    "library_name": "finance",
    "file_ext_filter": ".xlsx,.csv",
    "state_table": "catalog.schema.sp_delta_token",
}

SECRETS = {
    "spn-tenant-id": "tenant-id",
    "spn-client-id": "client-id",
    "spn-client-secret": "client-secret",
}

DELTA_LINK = "https://graph.microsoft.com/delta?token=new"
OLD_LINK = "https://graph.microsoft.com/delta?token=old"

FILE_ITEM = {
    "id": "FILEID01",
    "name": "report.xlsx",
    "file": {},
    "@microsoft.graph.downloadUrl": "https://dl.example.com/report.xlsx",
}


@pytest.fixture()
def spark():
    return MagicMock()


class TestRunHappyPath:
    def test_full_pipeline_commits_token(self, spark, mocker):
        mocker.patch("sharepoint_ingestion.job.get_access_token", return_value="tok")
        mocker.patch("sharepoint_ingestion.job.read_token", return_value=OLD_LINK)
        mocker.patch(
            "sharepoint_ingestion.job.fetch_delta_changes",
            return_value=([FILE_ITEM], DELTA_LINK),
        )
        mocker.patch("sharepoint_ingestion.job.download_file", return_value=b"bytes")
        mock_write_file = mocker.patch(
            "sharepoint_ingestion.job.write_file", return_value="/mnt/raw/file"
        )
        mock_write_token = mocker.patch("sharepoint_ingestion.job.write_token")

        run(spark, PARAMS, SECRETS)

        mock_write_file.assert_called_once_with(
            "/mnt/raw", "finance", "FILEID01", "report.xlsx", b"bytes"
        )
        mock_write_token.assert_called_once_with(
            spark,
            "catalog.schema.sp_delta_token",
            "drive-001",
            DELTA_LINK,
            "success",
        )

    def test_no_items_still_commits_token(self, spark, mocker):
        mocker.patch("sharepoint_ingestion.job.get_access_token", return_value="tok")
        mocker.patch("sharepoint_ingestion.job.read_token", return_value=None)
        mocker.patch(
            "sharepoint_ingestion.job.fetch_delta_changes",
            return_value=([], DELTA_LINK),
        )
        mock_write_file = mocker.patch("sharepoint_ingestion.job.write_file")
        mock_write_token = mocker.patch("sharepoint_ingestion.job.write_token")

        run(spark, PARAMS, SECRETS)

        mock_write_file.assert_not_called()
        mock_write_token.assert_called_once()

    def test_extension_filter_parsed_correctly(self, spark, mocker):
        mocker.patch("sharepoint_ingestion.job.get_access_token", return_value="tok")
        mocker.patch("sharepoint_ingestion.job.read_token", return_value=None)
        mock_fetch = mocker.patch(
            "sharepoint_ingestion.job.fetch_delta_changes",
            return_value=([], DELTA_LINK),
        )
        mocker.patch("sharepoint_ingestion.job.write_token")

        run(spark, PARAMS, SECRETS)

        _, kwargs_or_args = mock_fetch.call_args
        # fetch_delta_changes called with file_ext_filter as list
        assert mock_fetch.call_args[0][4] == [".xlsx", ".csv"]

    def test_no_extension_filter_passes_none(self, spark, mocker):
        params = {**PARAMS, "file_ext_filter": ""}
        mocker.patch("sharepoint_ingestion.job.get_access_token", return_value="tok")
        mocker.patch("sharepoint_ingestion.job.read_token", return_value=None)
        mock_fetch = mocker.patch(
            "sharepoint_ingestion.job.fetch_delta_changes",
            return_value=([], DELTA_LINK),
        )
        mocker.patch("sharepoint_ingestion.job.write_token")

        run(spark, params, SECRETS)

        assert mock_fetch.call_args[0][4] is None

    def test_no_folder_item_passes_none(self, spark, mocker):
        params = {**PARAMS, "folder_item_id": ""}
        mocker.patch("sharepoint_ingestion.job.get_access_token", return_value="tok")
        mocker.patch("sharepoint_ingestion.job.read_token", return_value=None)
        mock_fetch = mocker.patch(
            "sharepoint_ingestion.job.fetch_delta_changes",
            return_value=([], DELTA_LINK),
        )
        mocker.patch("sharepoint_ingestion.job.write_token")

        run(spark, params, SECRETS)

        assert mock_fetch.call_args[0][2] is None


class TestRunErrorHandling:
    def test_download_failure_does_not_commit_token(self, spark, mocker):
        mocker.patch("sharepoint_ingestion.job.get_access_token", return_value="tok")
        mocker.patch("sharepoint_ingestion.job.read_token", return_value=None)
        mocker.patch(
            "sharepoint_ingestion.job.fetch_delta_changes",
            return_value=([FILE_ITEM], DELTA_LINK),
        )
        mocker.patch(
            "sharepoint_ingestion.job.download_file",
            side_effect=RuntimeError("download failed"),
        )
        mock_write_token = mocker.patch("sharepoint_ingestion.job.write_token")

        with pytest.raises(RuntimeError, match="download failed"):
            run(spark, PARAMS, SECRETS)

        mock_write_token.assert_not_called()

    def test_410_resets_token_to_null_and_raises(self, spark, mocker):
        mocker.patch("sharepoint_ingestion.job.get_access_token", return_value="tok")
        mocker.patch("sharepoint_ingestion.job.read_token", return_value=OLD_LINK)
        mocker.patch(
            "sharepoint_ingestion.job.fetch_delta_changes",
            side_effect=TokenExpiredError("token expired"),
        )
        mock_write_token = mocker.patch("sharepoint_ingestion.job.write_token")

        with pytest.raises(TokenExpiredError):
            run(spark, PARAMS, SECRETS)

        mock_write_token.assert_called_once_with(
            spark,
            "catalog.schema.sp_delta_token",
            "drive-001",
            None,
            "failed",
        )
