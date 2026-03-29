"""Tests for storage.py — read_token(), write_token(), write_file()."""

from unittest.mock import MagicMock, patch

from sharepoint_ingestion.storage import read_token, write_file, write_token

DRIVE_ID = "drive-001"
TABLE = "catalog.schema.sp_delta_token"
DELTA_LINK = "https://graph.microsoft.com/delta?token=abc"


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

        mock_df.write.format.return_value.mode.return_value.option.return_value.saveAsTable.assert_called_once_with(
            TABLE
        )
        call_chain = mock_df.write.format.return_value.mode.return_value.option
        call_chain.assert_called_once_with("replaceWhere", f"drive_id = '{DRIVE_ID}'")


# ── write_file ────────────────────────────────────────────────


class TestWriteFile:
    def test_writes_to_correct_path(self, tmp_path):
        content = b"hello bytes"

        with patch("sharepoint_ingestion.storage.datetime") as mock_dt:
            mock_dt.utcnow.return_value.strftime.side_effect = lambda fmt: {
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
            mock_dt.utcnow.return_value.strftime.side_effect = lambda fmt: {
                "%Y": "2024",
                "%m": "01",
                "%d": "01",
            }[fmt]
            result = write_file(str(tmp_path), "lib", "ID1", "file.csv", b"data")

        assert result == str(tmp_path / "lib" / "2024" / "01" / "01" / "ID1_file.csv")

    def test_file_content_is_written(self, tmp_path):
        content = b"actual content"

        with patch("sharepoint_ingestion.storage.datetime") as mock_dt:
            mock_dt.utcnow.return_value.strftime.side_effect = lambda fmt: {
                "%Y": "2024",
                "%m": "06",
                "%d": "20",
            }[fmt]
            write_file(str(tmp_path), "lib", "ID2", "data.csv", content)

        written = tmp_path / "lib" / "2024" / "06" / "20" / "ID2_data.csv"
        assert written.read_bytes() == content
