"""CLI utility to resolve and print SharePoint job configuration parameters."""

from __future__ import annotations

from typing import Annotated

import requests
import typer

app = typer.Typer(
    help="Resolve SharePoint site/drive/folder IDs for job configuration."
)

_GRAPH_BASE = "https://graph.microsoft.com/v1.0"


@app.command()
def main(
    tenant_domain: Annotated[
        str,
        typer.Option(
            help="SharePoint tenant domain, e.g. company.sharepoint.com",
            envvar="SP_TENANT_DOMAIN",
        ),
    ],
    site_name: Annotated[
        str,
        typer.Option(
            help="SharePoint site name, e.g. AD-Sonic-Team", envvar="SP_SITE_NAME"
        ),
    ],
    tenant_id: Annotated[
        str | None, typer.Option(help="Azure AD tenant ID", envvar="SP_TENANT_ID")
    ] = None,
    client_id: Annotated[
        str | None,
        typer.Option(help="Service principal client ID", envvar="SP_CLIENT_ID"),
    ] = None,
    client_secret: Annotated[
        str | None,
        typer.Option(help="Service principal client secret", envvar="SP_CLIENT_SECRET"),
    ] = None,
    graph_api_token: Annotated[
        str | None,
        typer.Option(
            help="Pre-acquired Graph API bearer token; skips SPN authentication",
            envvar="GRAPH_API_TOKEN",
        ),
    ] = None,
    subfolder_path: Annotated[
        str | None,
        typer.Option(help="Optional subfolder path to scope, e.g. 'Tech Watch'"),
    ] = None,
) -> None:
    # ── Authenticate ──────────────────────────────────────────
    if graph_api_token:
        access_token = graph_api_token
    elif tenant_id and client_id and client_secret:
        r = requests.post(
            f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": f"{_GRAPH_BASE}/.default",
            },
            timeout=30,
        )
        r.raise_for_status()
        access_token = r.json()["access_token"]
    else:
        typer.echo(
            "Error: provide either --graph-api-token or all three of "
            "--tenant-id, --client-id, --client-secret.",
            err=True,
        )
        raise typer.Exit(1)
    headers = {"Authorization": f"Bearer {access_token}"}
    typer.echo("✅ Authenticated")

    # ── Resolve site_id ───────────────────────────────────────
    r = requests.get(
        f"{_GRAPH_BASE}/sites/{tenant_domain}:/sites/{site_name}",
        headers=headers,
        timeout=30,
    )
    r.raise_for_status()
    site_id = r.json()["id"]
    typer.echo(f"site_id:        {site_id}")

    # ── Resolve drive_id ──────────────────────────────────────
    r = requests.get(
        f"{_GRAPH_BASE}/sites/{site_id}/drive", headers=headers, timeout=30
    )
    r.raise_for_status()
    drive_id = r.json()["id"]
    typer.echo(f"drive_id:       {drive_id}")

    # ── Resolve folder_item_id (optional) ─────────────────────
    folder_item_id = None
    if subfolder_path:
        r = requests.get(
            f"{_GRAPH_BASE}/drives/{drive_id}/root:/{subfolder_path}",
            headers=headers,
            timeout=30,
        )
        r.raise_for_status()
        item = r.json()
        if "folder" not in item:
            typer.echo(
                f"Error: '{subfolder_path}' is not a folder — check the path.", err=True
            )
            raise typer.Exit(1)
        folder_item_id = item["id"]
        typer.echo(f"folder_item_id: {folder_item_id}")

    # ── Verify — list visible items ───────────────────────────
    base = f"{_GRAPH_BASE}/drives/{drive_id}"
    url = (
        f"{base}/items/{folder_item_id}/children"
        if folder_item_id
        else f"{base}/root/children"
    )
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    items = r.json().get("value", [])
    typer.echo(f"\n✅ Permission check — {len(items)} item(s) visible:")
    for item in items[:10]:
        kind = "[folder]" if "folder" in item else "[file] "
        typer.echo(f"   {kind} {item['name']}")

    # ── Print job configuration block ─────────────────────────
    typer.echo("\n✅ ── Job Configuration ─────────────────────────────────")
    typer.echo(f'tenant_domain  = "{tenant_domain}"')
    typer.echo(f'site_id        = "{site_id}"')
    typer.echo(f'drive_id       = "{drive_id}"')
    typer.echo(f'folder_item_id = "{folder_item_id}"   # None = full library')
