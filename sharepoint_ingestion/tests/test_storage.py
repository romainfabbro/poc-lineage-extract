"""Tests for storage.py — read_token(), write_token(), write_file()."""

from unittest.mock import MagicMock, patch

import pytest

from sharepoint_ingestion.storage import read_token, write_file, write_token

DRIVE_ID = "drive-001"
BAD_DRIVE_ID = "drive'; DROP TABLE sp_delta_token; --"
TABLE = "catalog.schema.sp_delta_token"
DELTA_LINK = "https://graph.microsoft.com/delta?token=abc"
ADLS_BASE = "abfss://container@account.dfs.core.windows.net"
STORAGE_OPTS = {"tenant_id": "t", "client_id": "c", "client_secret": "s"}


# ── read_token ────────────────────────────────────────────────


class TestReadToken:
    def test_returns_none_when_no_row(self):
        spark = MagicMock()
        chain = spark.sql.return_value.filter.return_value.limit.return_value
        chain.collect.return_value = []

        result = read_token(spark, TABLE, DRIVE_ID)

        assert result is None

    def test_returns_delta_link_when_row_exists(self):
        mock_row = MagicMock()
        mock_row.delta_link = DELTA_LINK
        spark = MagicMock()
        chain = spark.sql.return_value.filter.return_value.limit.return_value
        chain.collect.return_value = [mock_row]

        result = read_token(spark, TABLE, DRIVE_ID)

        assert result == DELTA_LINK

    def test_raises_on_invalid_drive_id(self):
        spark = MagicMock()
        with pytest.raises(ValueError, match="Invalid drive_id format"):
            read_token(spark, TABLE, BAD_DRIVE_ID)


# ── write_token ───────────────────────────────────────────────


class TestWriteToken:
    def test_writes_with_correct_data_and_replace_where(self):
        spark = MagicMock()
        mock_df = MagicMock()
        spark.createDataFrame.return_value = mock_df

        write_token(spark, TABLE, DRIVE_ID, DELTA_LINK, "success")

        spark.createDataFrame.assert_called_once()
        args, _ = spark.createDataFrame.call_args
        row_data = args[0]
        assert row_data[0]["drive_id"] == DRIVE_ID
        assert row_data[0]["delta_link"] == DELTA_LINK
        assert row_data[0]["last_status"] == "success"

        (
            mock_df.write.format.return_value.mode.return_value.option.return_value.saveAsTable.assert_called_once_with(
                TABLE
            )
        )
        call_chain = mock_df.write.format.return_value.mode.return_value.option
        call_chain.assert_called_once_with("replaceWhere", f"drive_id = '{DRIVE_ID}'")

    def test_accepts_none_delta_link(self):
        spark = MagicMock()
        spark.createDataFrame.return_value = MagicMock()

        write_token(spark, TABLE, DRIVE_ID, None, "failed")

        args, _ = spark.createDataFrame.call_args
        assert args[0][0]["delta_link"] is None

    def test_raises_on_invalid_drive_id(self):
        spark = MagicMock()
        with pytest.raises(ValueError, match="Invalid drive_id format"):
            write_token(spark, TABLE, BAD_DRIVE_ID, DELTA_LINK, "success")


# ── write_file — POSIX (UC Volume) ───────────────────────────


class TestWriteFilePosix:
    def test_writes_to_correct_path(self, tmp_path):
        content = b"hello bytes"

        with patch("sharepoint_ingestion.storage.datetime") as mock_dt:
            mock_dt.now.return_value.strftime.side_effect = lambda fmt: {
                "%Y": "2024",
                "%m": "03",
                "%d": "15",
            }[fmt]
            path = write_file(
                str(tmp_path), "finance", "FILEID01", "report.xlsx", content
            )

        expected = str(
            tmp_path / "finance" / "2024" / "03" / "15" / "FILEID01_report.xlsx"
        )
        assert path == expected

    def test_returns_full_path(self, tmp_path):
        with patch("sharepoint_ingestion.storage.datetime") as mock_dt:
            mock_dt.now.return_value.strftime.side_effect = lambda fmt: {
                "%Y": "2024",
                "%m": "01",
                "%d": "01",
            }[fmt]
            result = write_file(str(tmp_path), "lib", "ID1", "file.csv", b"data")

        assert result == str(tmp_path / "lib" / "2024" / "01" / "01" / "ID1_file.csv")

    def test_file_content_is_written(self, tmp_path):
        content = b"actual content"

        with patch("sharepoint_ingestion.storage.datetime") as mock_dt:
            mock_dt.now.return_value.strftime.side_effect = lambda fmt: {
                "%Y": "2024",
                "%m": "06",
                "%d": "20",
            }[fmt]
            write_file(str(tmp_path), "lib", "ID2", "data.csv", content)

        written = tmp_path / "lib" / "2024" / "06" / "20" / "ID2_data.csv"
        assert written.read_bytes() == content

    def test_raises_on_relative_path(self):
        with pytest.raises(ValueError, match="absolute POSIX path"):
            write_file("relative/path", "lib", "ID1", "file.csv", b"data")


# ── write_file — ADLS Gen2 (abfss://) ────────────────────────


class TestWriteFileAdls:
    def _mock_date(self, mocker, yyyy="2024", mm="03", dd="15"):
        mock_dt = mocker.patch("sharepoint_ingestion.storage.datetime")
        mock_dt.now.return_value.strftime.side_effect = lambda fmt: {
            "%Y": yyyy,
            "%m": mm,
            "%d": dd,
        }[fmt]

    def test_writes_via_fsspec(self, mocker):
        self._mock_date(mocker)
        mock_open = mocker.patch("sharepoint_ingestion.storage.fsspec.open")
        mock_fh = mock_open.return_value.__enter__.return_value

        write_file(ADLS_BASE, "finance", "ID1", "report.xlsx", b"bytes", STORAGE_OPTS)

        expected_path = f"{ADLS_BASE}/finance/2024/03/15/ID1_report.xlsx"
        mock_open.assert_called_once_with(expected_path, "wb", **STORAGE_OPTS)
        mock_fh.write.assert_called_once_with(b"bytes")

    def test_returns_cloud_path(self, mocker):
        self._mock_date(mocker)
        mocker.patch("sharepoint_ingestion.storage.fsspec.open")

        result = write_file(
            ADLS_BASE, "finance", "ID1", "report.xlsx", b"bytes", STORAGE_OPTS
        )

        assert result == f"{ADLS_BASE}/finance/2024/03/15/ID1_report.xlsx"

    def test_strips_trailing_slash_from_base(self, mocker):
        self._mock_date(mocker)
        mock_open = mocker.patch("sharepoint_ingestion.storage.fsspec.open")

        write_file(
            ADLS_BASE + "/", "finance", "ID1", "report.xlsx", b"bytes", STORAGE_OPTS
        )

        called_path = mock_open.call_args[0][0]
        assert "//" not in called_path.replace("://", "")

    def test_empty_storage_options_uses_no_kwargs(self, mocker):
        self._mock_date(mocker)
        mock_open = mocker.patch("sharepoint_ingestion.storage.fsspec.open")

        write_file(ADLS_BASE, "lib", "ID1", "file.csv", b"data", None)

        mock_open.assert_called_once_with(
            f"{ADLS_BASE}/lib/2024/03/15/ID1_file.csv", "wb"
        )
