"""Tests for auth.py — get_access_token()."""

import pytest

from sharepoint_ingestion.auth import get_access_token


class TestGetAccessToken:
    def test_returns_env_token_if_set(self, mocker):
        mocker.patch("sharepoint_ingestion.auth.os.getenv", return_value="env-tok-123")
        mock_msal = mocker.patch("sharepoint_ingestion.auth.msal.ConfidentialClientApplication")

        token = get_access_token("tid", "cid", "sec")

        assert token == "env-tok-123"
        mock_msal.assert_not_called()

    def test_returns_token_on_success(self, mocker):
        mock_app = mocker.MagicMock()
        mock_app.acquire_token_for_client.return_value = {"access_token": "tok123"}
        mocker.patch(
            "sharepoint_ingestion.auth.msal.ConfidentialClientApplication",
            return_value=mock_app,
        )

        token = get_access_token("tenant-id", "client-id", "client-secret")

        assert token == "tok123"
        mock_app.acquire_token_for_client.assert_called_once_with(
            scopes=["https://graph.microsoft.com/.default"]
        )

    def test_raises_on_msal_error(self, mocker):
        mock_app = mocker.MagicMock()
        mock_app.acquire_token_for_client.return_value = {
            "error": "invalid_client",
            "error_description": "bad credentials",
        }
        mocker.patch(
            "sharepoint_ingestion.auth.msal.ConfidentialClientApplication",
            return_value=mock_app,
        )

        with pytest.raises(RuntimeError, match="MSAL token acquisition failed"):
            get_access_token("tenant-id", "client-id", "client-secret")
