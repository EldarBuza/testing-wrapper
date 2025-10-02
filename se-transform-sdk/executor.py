import os, uuid, traceback
from typing import Any, Dict, List, Tuple
from .context import RunContext, _set_run_context, get_run_context
from .lineage import LineageClient, job, dataset, start, complete, fail

PRODUCER = "https://se-transform/sdk/0.1.0"

def _job_ns(tenant: str, repo: str) -> str:
    return f"se://{tenant}/{repo}"

def _invoke_task(spec, fn, args: Tuple[Any, ...], kwargs: Dict[str, Any]):
    ctx = get_run_context()
    ns = _job_ns(ctx.tenant, ctx.repo)
    lc = LineageClient()
    task_job = job(ns, f"{fn.__module__}.{fn.__name__}")
    inputs = [dataset(d.namespace, d.name) for d in spec.inputs]
    lc.emit([start(PRODUCER, task_job, ctx.run_id)])
    attempt = 0
    while True:
        try:
            out = fn(*args, **kwargs)
            outputs = [dataset(d.namespace, d.name) for d in spec.outputs]
            lc.emit([complete(PRODUCER, task_job, ctx.run_id, inputs=inputs, outputs=outputs)])
            return out
        except Exception as e:
            attempt += 1
            if attempt <= spec.retries:
                continue
            tb = "".join(traceback.format_exception(e))
            lc.emit([fail(PRODUCER, task_job, ctx.run_id, facets={"errorMessage": {"message": str(e), "stack": tb[:5000]}})])
            raise

def run(pipeline_fn, params: Dict[str, Any]):
    repo = os.getenv("GITHUB_REPOSITORY", "local/repo")
    tenant = os.getenv("SE_TENANT", "dev")
    run_id = str(uuid.uuid4())
    ctx = RunContext(
        run_id=run_id,
        params=params,
        tenant=tenant,
        repo=repo,
        github_run_url=os.getenv("GITHUB_RUN_URL", ""),
        storage=get_run_context().storage,  # reuse factory
    )
    _set_run_context(ctx)

    lc = LineageClient()
    ns = _job_ns(tenant, repo)
    pipe_name = getattr(pipeline_fn, "__pipeline_spec__", None).name if hasattr(pipeline_fn, "__pipeline_spec__") else pipeline_fn.__name__
    pipe_job = job(ns, pipe_name)
    lc.emit([start(PRODUCER, pipe_job, run_id, facets={"sourceCodeLocation": {"git": {"repoUrl": repo}}})])
    try:
        result = pipeline_fn(**params)
        lc.emit([complete(PRODUCER, pipe_job, run_id)])
        return result
    except Exception as e:
        tb = "".join(traceback.format_exception(e))
        lc.emit([fail(PRODUCER, pipe_job, run_id, facets={"errorMessage": {"message": str(e), "stack": tb[:5000]}})])
        raise