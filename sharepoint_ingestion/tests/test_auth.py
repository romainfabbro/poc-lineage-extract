"""Tests for auth.py — get_access_token()."""

import pytest

from sharepoint_ingestion.auth import get_access_token


class TestGetAccessToken:
    def test_returns_env_token_if_set(self, mocker):
        mocker.patch("sharepoint_ingestion.auth.os.getenv", return_value="env-tok-123")
        mock_post = mocker.patch("sharepoint_ingestion.auth.requests.post")

        token = get_access_token("tid", "cid", "sec")

        assert token == "env-tok-123"
        mock_post.assert_not_called()

    def test_returns_token_on_success(self, mocker):
        mocker.patch("sharepoint_ingestion.auth.os.getenv", return_value=None)
        mock_post = mocker.patch("sharepoint_ingestion.auth.requests.post")
        mock_post.return_value.json.return_value = {"access_token": "tok123"}

        token = get_access_token("tenant-id", "client-id", "client-secret")

        assert token == "tok123"
        mock_post.assert_called_once_with(
            "https://login.microsoftonline.com/tenant-id/oauth2/v2.0/token",
            data={
                "grant_type": "client_credentials",
                "client_id": "client-id",
                "client_secret": "client-secret",
                "scope": "https://graph.microsoft.com/.default",
            },
            timeout=30,
        )

    def test_raises_on_http_error(self, mocker):
        mocker.patch("sharepoint_ingestion.auth.os.getenv", return_value=None)
        mock_post = mocker.patch("sharepoint_ingestion.auth.requests.post")
        mock_post.return_value.raise_for_status.side_effect = Exception("401 Unauthorized")

        with pytest.raises(Exception, match="401 Unauthorized"):
            get_access_token("tenant-id", "client-id", "bad-secret")
