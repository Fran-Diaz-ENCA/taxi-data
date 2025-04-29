import requests
import os

# ==== CONFIGURACIÓN ====
client_id     = os.environ.get('FABRIC_CLIENT_ID') 
client_secret = os.environ.get('FABRIC_CLIENT_SECRET') 
tenant_id     = os.environ.get('FABRIC_TENANT_ID')
workspace_id  = os.environ.get('FABRIC_WORKSPACE_ID')
git_username  = os.environ.get('USER_GITHUB')
git_pat       = os.environ.get('FABRIC_GITHUB_TOKEN')  # Personal Access Token


authority_url = f"https://login.microsoftonline.com/{tenant_id}"
token_url = f"{authority_url}/oauth2/v2.0/token"
scope = "https://analysis.windows.net/powerbi/api/.default"

# ==== 1. Obtener token de acceso ====
def get_access_token():
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": scope
    }
    response = requests.post(token_url, data=payload)
    response.raise_for_status()
    return response.json()["access_token"]

access_token = get_access_token()

headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

# ==== 3. Commit a Git ====
commit_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/commitToGit"

commit_payload = {
    "commitMessage": "Actualización desde API",
    "includeItemsNotInGit": True
}

resp = requests.post(commit_url, json=commit_payload, headers=headers)
if resp.status_code == 200:
    print("✅ Commit a Git exitoso.")
else:
    print(f"❌ Error al hacer commit: {resp.status_code} - {resp.text}")
