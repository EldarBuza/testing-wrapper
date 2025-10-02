import os, json, requests
from typing import Any, Dict

class BackendAPI:
    def __init__(self, base_url: str | None = None, token: str | None = None):
        self.base_url = (base_url or os.getenv("SE_API_URL", "")).rstrip("/")
        self.token = token or os.getenv("SE_TOKEN", "")

    def _headers(self):
        return {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}

    def register_manifest(self, manifest: Dict[str, Any]):
        url = f"{self.base_url}/pipelines/register"
        resp = requests.post(url, headers=self._headers(), json=manifest, timeout=20)
        resp.raise_for_status()
        return resp.json()