from functools import wraps
from typing import Any, Callable, Dict, List, Optional
from .registry import PipelineSpec, TaskSpec, register_pipeline, register_task, DatasetRef
from .datasets import S3Dataset
from .executor import _invoke_task  # internal helper

def _dsref(d: Any) -> DatasetRef:
    if isinstance(d, S3Dataset):
        return DatasetRef(kind="s3", namespace=d.namespace, name=d.name)
    raise TypeError(f"Unsupported dataset type: {type(d)}")

def pipeline(name: str, params: Optional[Dict[str, Any]] = None):
    params = params or {}
    def decorator(fn: Callable[..., Any]):
        spec = PipelineSpec(
            name=name,
            fn=fn,
            params=params,
            module=fn.__module__,
            qualname=f"{fn.__module__}:{fn.__name__}",
        )
        register_pipeline(spec)

        @wraps(fn)
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)

        wrapper.__pipeline_spec__ = spec
        return wrapper
    return decorator

def task(
    name: Optional[str] = None,
    inputs: Optional[List[Any]] = None,
    outputs: Optional[List[Any]] = None,
    retries: int = 0,
    timeout_s: Optional[int] = None,
):
    inputs = inputs or []
    outputs = outputs or []
    def decorator(fn: Callable[..., Any]):
        spec = TaskSpec(
            name=name or fn.__name__,
            fn=fn,
            inputs=[_dsref(i) for i in inputs],
            outputs=[_dsref(o) for o in outputs],
            retries=retries,
            timeout_s=timeout_s,
        )
        register_task(spec)

        @wraps(fn)
        def wrapped(*args, **kwargs):
            return _invoke_task(spec, fn, args, kwargs)
        wrapped.__task_spec__ = spec
        return wrapped
    return decorator