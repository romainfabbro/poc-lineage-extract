"""Authentication — acquire a Bearer token from Azure AD via MSAL."""

import msal


def get_access_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    """Return a Bearer token string for the Microsoft Graph API."""
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.ConfidentialClientApplication(
        client_id,
        authority=authority,
        client_credential=client_secret,
    )
    result = app.acquire_token_for_client(
        scopes=["https://graph.microsoft.com/.default"]
    )
    if "access_token" not in result:
        error = result.get("error_description") or result.get("error", "unknown")
        raise RuntimeError(f"MSAL token acquisition failed: {error}")
    return result["access_token"]
