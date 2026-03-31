# Developer Guide — Azure Setup & SharePoint ID Retrieval for Ingestion Job Configuration

**Type:** Developer How-To  
**Audience:** Data Engineers configuring the SharePoint ingestion job  
**Version:** 2.0  
**Date:** 2026-03-29  
**Prerequisite:** You have Azure AD access with permission to create App Registrations, or a platform/Azure admin who can do it for you.

---

## Overview

Two things need to happen before configuring the ingestion job:

1. **Azure setup** — create an App Registration (service principal) and grant it access to SharePoint
2. **ID retrieval** — use the Graph API to resolve the stable GUIDs needed as job parameters

| ID | What it is | Rename-safe? |
|---|---|---|
| `client_id` | Identity of the App Registration | ✅ Yes |
| `client_secret` | Secret credential for the App Registration | ✅ Yes |
| `site_id` | GUID identifying the SharePoint site | ✅ Yes |
| `drive_id` | GUID identifying the document library within the site | ✅ Yes |
| `folder_item_id` | GUID identifying the target subfolder (optional) | ✅ Yes |

All GUIDs are **stable** — they never change even if someone renames the site, library, or folder in SharePoint. You resolve them once and store them permanently in the job configuration.

---

## Part 1 — Azure Setup (do this once)

> **Who does this:** an Azure AD administrator, or a developer with the `Application Administrator` role in Azure AD.

### Step 1 — Create an App Registration

An App Registration is how you give a non-human identity (your Databricks job) the ability to authenticate against Microsoft services.

1. Go to [portal.azure.com](https://portal.azure.com)
2. Navigate to **Azure Active Directory → App registrations → New registration**
3. Fill in:
   - **Name:** `databricks-sharepoint-ingestion` (or follow your org's naming convention)
   - **Supported account types:** `Accounts in this organizational directory only`
   - **Redirect URI:** leave blank
4. Click **Register**

After registration, you land on the app overview page. Note down:

```
Application (client) ID  → this is your client_id
Directory (tenant) ID    → this is your tenant_id
```

---

### Step 2 — Create a Client Secret

1. In the App Registration, go to **Certificates & secrets → Client secrets → New client secret**
2. Set a description (e.g. `databricks-ingestion-secret`) and an expiry (12 or 24 months)
3. Click **Add**
4. **Copy the secret value immediately** — it is only shown once

```
Secret Value  → this is your client_secret
```

> **Important:** store `client_id` and `client_secret` in Azure Key Vault immediately. Never commit them to code or config files.

---

### Step 3 — Grant Microsoft Graph API Permissions

The App Registration needs permission to read SharePoint files via the Graph API.

1. In the App Registration, go to **API permissions → Add a permission → Microsoft Graph**
2. Select **Application permissions** (not Delegated — this is a background job with no user)
3. Search for and add: **`Sites.Read.All`**

> `Sites.Read.All` grants read access to all SharePoint sites. If your organisation requires narrower permissions, `Files.Read.All` is the alternative — it grants read access to files across all drives but requires additional SharePoint-level setup with your admin.

4. Click **Add permissions**
5. Click **Grant admin consent for {your organisation}** — this step requires an Azure AD Global Administrator or Privileged Role Administrator

The permission status should show ✅ **Granted** after this step.

---

### Step 4 — Grant Access to the Target SharePoint Site

Graph API application permissions (`Sites.Read.All`) give broad access by default. If your organisation restricts this via SharePoint App-Only access policies, the SharePoint site administrator needs to explicitly grant the App Registration access to the target site.

Ask the **SharePoint site administrator** to run the following in PowerShell:

```powershell
# Install module if needed
Install-Module -Name PnP.PowerShell

# Connect to the target SharePoint site
Connect-PnPOnline -Url "https://my-company.sharepoint.com/sites/finance-data" -Interactive

# Grant the App Registration read access to this site
Grant-PnPAzureADAppSitePermission `
  -AppId "{client_id}" `
  -DisplayName "databricks-sharepoint-ingestion" `
  -Permissions Read
```

> If your organisation does not restrict `Sites.Read.All`, this step may not be required. Confirm with your SharePoint administrator.

---

### Step 5 — Store Secrets in Azure Key Vault

Store credentials in the Key Vault linked to your Databricks workspace. Never pass them as plain job parameters.

```bash
# Using Azure CLI
az keyvault secret set --vault-name "{keyvault-name}" --name "sharepoint-tenant-id"    --value "{tenant_id}"
az keyvault secret set --vault-name "{keyvault-name}" --name "sharepoint-client-id"    --value "{client_id}"
az keyvault secret set --vault-name "{keyvault-name}" --name "sharepoint-client-secret" --value "{client_secret}"
```

Then create a **Databricks secret scope** backed by this Key Vault (one-time setup, done by the platform team):

```bash
databricks secrets create-scope \
  --scope sharepoint \
  --scope-backend-type AZURE_KEYVAULT \
  --resource-id "/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.KeyVault/vaults/{kv}" \
  --dns-name "https://{keyvault-name}.vault.azure.net/"
```

---

### Azure Setup Summary

At the end of Part 1 you should have:

| Item | Where it lives |
|---|---|
| `tenant_id` | Noted from App Registration overview |
| `client_id` | Noted from App Registration overview |
| `client_secret` | Stored in Azure Key Vault |
| Graph API permission `Sites.Read.All` | Granted with admin consent |
| SharePoint site access | Granted by site admin (if required) |
| Databricks secret scope `sharepoint` | Created by platform team |

---

## Part 2 — Retrieve SharePoint IDs

With the App Registration in place, you can now authenticate and resolve the stable GUIDs needed for the job.

### What you need

| Item | Example | Where it comes from |
|---|---|---|
| `client_id` | `a1b2c3d4-1234-...` | Part 1 — App Registration |
| `client_secret` | `xK8~abc...` | Part 1 — Key Vault |
| `tenant_domain` | `my-company.sharepoint.com` | Your organisation's SharePoint domain |
| `tenant_id` | `72f988bf-1234-...` | Part 1 — App Registration |
| `site_name` | `finance-data` | Human-readable site name — ask SharePoint site owner |
| `subfolder_path` | `Finance/2026` | Folder path within the library — ask SharePoint site owner (optional) |

> `site_name` and `subfolder_path` are only needed to **look up** the GUIDs. Once you have the GUIDs, these human-readable names are no longer used by the job.

---

## Step 6 — Get an Access Token

Everything starts here. You authenticate as the service principal using the client credentials flow and get a Bearer token to call the Graph API.

### Using curl

```bash
curl -X POST \
  "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  -d "client_id={client_id}" \
  -d "client_secret={client_secret}" \
  -d "scope=https://graph.microsoft.com/.default"
```

### Using Python

```python
import msal

tenant_id     = "72f988bf-1234-..."
client_id     = "a1b2c3d4-1234-..."
client_secret = "xK8~abc..."

app = msal.ConfidentialClientApplication(
    client_id,
    authority=f"https://login.microsoftonline.com/{tenant_id}",
    client_credential=client_secret
)

result = app.acquire_token_for_client(
    scopes=["https://graph.microsoft.com/.default"]
)

access_token = result["access_token"]
print(access_token)
```

### Expected response

```json
{
  "token_type": "Bearer",
  "access_token": "eyJ0eXAiOiJKV1Qi...",
  "expires_in": 3599
}
```

Copy the `access_token` value. You will use it in all subsequent steps as the `Authorization` header.

> **Token lifetime:** the token expires after ~1 hour. If you get a 401 response in later steps, re-run Step 6 to get a fresh token.

---

## Step 7 — Retrieve the `site_id`

The `site_id` is the GUID that permanently identifies the SharePoint site. You resolve it from the human-readable site name.

### Request

```bash
curl -X GET \
  "https://graph.microsoft.com/v1.0/sites/{tenant_domain}:/sites/{site_name}" \
  -H "Authorization: Bearer {access_token}"
```

**Example:**
```bash
curl -X GET \
  "https://graph.microsoft.com/v1.0/sites/my-company.sharepoint.com:/sites/finance-data" \
  -H "Authorization: Bearer eyJ0eXAiOiJKV1Qi..."
```

### Using Python

```python
import requests

tenant_domain = "my-company.sharepoint.com"
site_name     = "finance-data"

response = requests.get(
    f"https://graph.microsoft.com/v1.0/sites/{tenant_domain}:/sites/{site_name}",
    headers={"Authorization": f"Bearer {access_token}"}
)

site = response.json()
site_id = site["id"]
print(f"site_id: {site_id}")
```

### Expected response (truncated)

```json
{
  "id": "my-company.sharepoint.com,a1b2c3d4-...,e5f6g7h8-...",
  "displayName": "Finance Data",
  "name": "finance-data",
  "webUrl": "https://my-company.sharepoint.com/sites/finance-data"
}
```

> **Important:** the `id` field is a composite string in the format `{hostname},{site-guid},{web-guid}`. Use the **full string as-is** — this is your `site_id`. Do not extract just one part of it.

✅ **Store this value:** `site_id = "my-company.sharepoint.com,a1b2c3d4-...,e5f6g7h8-..."`

---

## Step 8 — Retrieve the `drive_id`

The `drive_id` identifies the document library within the site. Most SharePoint sites have one default library ("Documents"). If your target library is the default one, use the `/drive` shortcut. If it is a named library, list all drives and pick the right one.

### Option A — Default document library (most common)

```bash
curl -X GET \
  "https://graph.microsoft.com/v1.0/sites/{site_id}/drive" \
  -H "Authorization: Bearer {access_token}"
```

### Option B — Named library (list all and pick)

```bash
curl -X GET \
  "https://graph.microsoft.com/v1.0/sites/{site_id}/drives" \
  -H "Authorization: Bearer {access_token}"
```

### Using Python

```python
# Option A — default library
response = requests.get(
    f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive",
    headers={"Authorization": f"Bearer {access_token}"}
)
drive = response.json()
drive_id = drive["id"]
print(f"drive_id: {drive_id}")

# Option B — list all libraries and pick by name
response = requests.get(
    f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives",
    headers={"Authorization": f"Bearer {access_token}"}
)
for drive in response.json()["value"]:
    print(f"  name: {drive['name']}  |  id: {drive['id']}")
```

### Expected response (Option A, truncated)

```json
{
  "id": "b!xYz123abc...",
  "name": "Documents",
  "driveType": "documentLibrary",
  "webUrl": "https://my-company.sharepoint.com/sites/finance-data/Shared%20Documents"
}
```

✅ **Store this value:** `drive_id = "b!xYz123abc..."`

---

## Step 9 — Retrieve the `folder_item_id` (optional)

Only needed if you want to scope ingestion to a **specific subfolder** within the library. If you want to ingest the entire library, skip this step.

### Request

```bash
curl -X GET \
  "https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{subfolder_path}" \
  -H "Authorization: Bearer {access_token}"
```

**Example** — targeting the folder `Finance/2026`:
```bash
curl -X GET \
  "https://graph.microsoft.com/v1.0/drives/b!xYz123abc.../root:/Finance/2026" \
  -H "Authorization: Bearer eyJ0eXAiOiJKV1Qi..."
```

### Using Python

```python
subfolder_path = "Finance/2026"

response = requests.get(
    f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{subfolder_path}",
    headers={"Authorization": f"Bearer {access_token}"}
)

folder = response.json()
folder_item_id = folder["id"]
print(f"folder_item_id: {folder_item_id}")
```

### Expected response (truncated)

```json
{
  "id": "01ABCDEFGHIJ...",
  "name": "2026",
  "folder": {},
  "webUrl": "https://my-company.sharepoint.com/sites/finance-data/Shared%20Documents/Finance/2026"
}
```

> **Tip:** the presence of the `"folder": {}` field in the response confirms this is a folder item, not a file. If you get a file object instead, check your subfolder path.

✅ **Store this value:** `folder_item_id = "01ABCDEFGHIJ..."`

---

## Step 10 — Verify permissions (recommended)

Before handing the IDs to the job, verify the service principal can actually list files in the target location. This catches permission issues before the first scheduled run.

### List files in the target folder

```python
# With subfolder scoping
response = requests.get(
    f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{folder_item_id}/children",
    headers={"Authorization": f"Bearer {access_token}"}
)

# Without subfolder (root of library)
response = requests.get(
    f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children",
    headers={"Authorization": f"Bearer {access_token}"}
)

for item in response.json().get("value", []):
    print(f"  {'[folder]' if 'folder' in item else '[file] '} {item['name']}")
```

If you see the expected files and folders listed, permissions are correct and the job will work.

**Common errors at this step:**

| Error | Cause | Fix |
|---|---|---|
| `401 Unauthorized` | Token expired or wrong `client_id` | Re-run Step 6, verify credentials |
| `403 Forbidden` | SPN lacks `Sites.Read.All` permission on this site | Re-run Part 1 Steps 3–4, ask admin to grant consent |
| `404 Not Found` | Wrong `site_id`, `drive_id`, or `folder_item_id` | Re-run the relevant step above |

---

## Summary — Values to hand over for job configuration

Once all steps are complete, collect these values and store them in the Databricks job configuration (credentials go to Key Vault, IDs go to job parameters):

```python
# --- Key Vault secrets (handled by platform team) ---
spn_client_id     = "a1b2c3d4-1234-..."
spn_client_secret = "xK8~abc..."

# --- Databricks job parameters ---
drive_id          = "b!xYz123abc..."
folder_item_id    = "01ABCDEFGHIJ..."    # omit if ingesting full library
raw_base_path     = "abfss://..."        # or /Volumes/...
library_name      = "finance-reports"
file_ext_filter   = ".xlsx,.csv"         # omit if all file types
```

> **Reminder:** these IDs are permanent. Even if the site, library, or folder is renamed in SharePoint, the GUIDs remain valid and the job does not need to be reconfigured.

---

## Full Python Script (Part 2 — all ID retrieval steps in one)

For convenience, here is a single script that covers Steps 6–10 in sequence and prints the final job configuration block.

```python
import msal
import requests

# ── Inputs ────────────────────────────────────────────────
tenant_id      = "72f988bf-1234-..."
client_id      = "a1b2c3d4-1234-..."
client_secret  = "xK8~abc..."
tenant_domain  = "my-company.sharepoint.com"
site_name      = "finance-data"
subfolder_path = "Finance/2026"          # set to None to skip folder scoping

# ── Step 1: Authenticate ──────────────────────────────────
app = msal.ConfidentialClientApplication(
    client_id,
    authority=f"https://login.microsoftonline.com/{tenant_id}",
    client_credential=client_secret
)
token = app.acquire_token_for_client(
    scopes=["https://graph.microsoft.com/.default"]
)
headers = {"Authorization": f"Bearer {token['access_token']}"}
print("✅ Authenticated")

# ── Step 2: Resolve site_id ───────────────────────────────
r = requests.get(
    f"https://graph.microsoft.com/v1.0/sites/{tenant_domain}:/sites/{site_name}",
    headers=headers
)
r.raise_for_status()
site_id = r.json()["id"]
print(f"✅ site_id:        {site_id}")

# ── Step 3: Resolve drive_id ──────────────────────────────
r = requests.get(
    f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive",
    headers=headers
)
r.raise_for_status()
drive_id = r.json()["id"]
print(f"✅ drive_id:       {drive_id}")

# ── Step 4: Resolve folder_item_id (optional) ─────────────
folder_item_id = None
if subfolder_path:
    r = requests.get(
        f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{subfolder_path}",
        headers=headers
    )
    r.raise_for_status()
    item = r.json()
    assert "folder" in item, f"Path '{subfolder_path}' is not a folder — check the path."
    folder_item_id = item["id"]
    print(f"✅ folder_item_id: {folder_item_id}")

# ── Step 5: Verify — list files ───────────────────────────
base = f"https://graph.microsoft.com/v1.0/drives/{drive_id}"
url  = f"{base}/items/{folder_item_id}/children" if folder_item_id else f"{base}/root/children"
r    = requests.get(url, headers=headers)
r.raise_for_status()
items = r.json().get("value", [])
print(f"\n✅ Permission check — {len(items)} item(s) visible:")
for item in items[:10]:
    kind = "[folder]" if "folder" in item else "[file] "
    print(f"   {kind} {item['name']}")

# ── Output: job configuration block ───────────────────────
print("\n── Job Configuration ─────────────────────────────────")
print(f'tenant_domain  = "{tenant_domain}"')
print(f'site_id        = "{site_id}"')
print(f'drive_id       = "{drive_id}"')
print(f'folder_item_id = "{folder_item_id}"   # None = full library')
```

---

*For the ingestion job technical documentation, see: [SharePoint Ingestion — Technical Documentation](./SharePoint_Ingestion_Technical_Doc.md)*
