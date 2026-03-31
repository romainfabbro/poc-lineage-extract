"""Authentication — acquire a Bearer token from Azure AD."""

import os

import requests

_TOKEN_URL = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"


def get_access_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    """Return a Bearer token string for the Microsoft Graph API.

    If the environment variable 'GRAPH_API_TOKEN' is set, it is returned
    directly. Otherwise, a new token is acquired via the OAuth2 client
    credentials flow.
    """
    env_token = os.getenv("GRAPH_API_TOKEN")
    if env_token:
        return env_token

    response = requests.post(
        _TOKEN_URL.format(tenant_id=tenant_id),
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://graph.microsoft.com/.default",
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["access_token"]
