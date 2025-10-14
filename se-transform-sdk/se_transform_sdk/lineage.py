import os, time, requests
from typing import Any, Dict, List

def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

class LineageClient:
    def __init__(self, api_url: str | None = None, token: str | None = None):
        self.api_url = api_url or os.getenv("SE_API_URL")
        self.token = token or os.getenv("SE_TOKEN")

    def emit(self, events: List[Dict[str, Any]]):
        if not self.api_url or not self.token or not events:
            return
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        url = f"{self.api_url.rstrip('/')}/lineage/events/"
        resp = requests.post(url, headers=headers, json={"events": events}, timeout=20)
        resp.raise_for_status()

def job(ns: str, name: str) -> Dict[str, Any]:
    return {"namespace": ns, "name": name}

def dataset(ns: str, name: str) -> Dict[str, Any]:
    return {"namespace": ns, "name": name}

def start(producer: str, job_: Dict[str, Any], run_id: str, facets: Dict[str, Any] | None = None):
    return {"eventType": "START", "eventTime": now_iso(), "producer": producer, "job": job_, "run": {"runId": run_id, "facets": facets or {}}}

def complete(producer: str, job_: Dict[str, Any], run_id: str, inputs=None, outputs=None, facets=None):
    return {
        "eventType": "COMPLETE",
        "eventTime": now_iso(),
        "producer": producer,
        "job": job_,
        "run": {"runId": run_id, "facets": facets or {}},
        "inputs": inputs or [],
        "outputs": outputs or [],
    }

def fail(producer: str, job_: Dict[str, Any], run_id: str, facets=None):
    return {"eventType": "FAIL", "eventTime": now_iso(), "producer": producer, "job": job_, "run": {"runId": run_id, "facets": facets or {}}}