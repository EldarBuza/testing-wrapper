import os, uuid
from dataclasses import dataclass
from typing import Any, Dict
from .storage import get_storage, StorageIO

@dataclass
class RunContext:
    run_id: str
    params: Dict[str, Any]
    tenant: str
    repo: str
    github_run_url: str
    storage: StorageIO

_ctx: RunContext | None = None

def _set_run_context(ctx: RunContext):
    global _ctx
    _ctx = ctx

def get_run_context() -> RunContext:
    global _ctx
    if _ctx is None:
        _ctx = RunContext(
            run_id=str(uuid.uuid4()),
            params={},
            tenant=os.getenv("SE_TENANT", "dev"),
            repo=os.getenv("GITHUB_REPOSITORY", "local/repo"),
            github_run_url=os.getenv("GITHUB_RUN_URL", ""),
            storage=get_storage(),
        )
    return _ctx