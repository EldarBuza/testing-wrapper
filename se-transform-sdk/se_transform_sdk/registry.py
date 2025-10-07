from dataclasses import dataclass, field
import threading
from typing import Any, Callable, Dict, List, Optional

@dataclass
class DatasetRef:
    kind: str  # "s3"
    namespace: str
    name: str
    extras: Dict[str, Any] = field(default_factory=dict)

@dataclass
class TaskSpec:
    name: str
    fn: Callable[..., Any]
    inputs: List[DatasetRef]
    outputs: List[DatasetRef]
    retries: int
    timeout_s: Optional[int]

@dataclass
class PipelineSpec:
    name: str
    fn: Callable[..., Any]
    params: Dict[str, Any]
    module: str
    qualname: str
    tasks: Dict[str, TaskSpec] = field(default_factory=dict)

class CallRecorder(threading.local):
    def __init__(self):
        super().__init__()
        self.enabled = False
        self.calls = []

CALL_RECORDER = CallRecorder()
PIPELINES: Dict[str, PipelineSpec] = {}
TASKS_BY_FN: Dict[Callable[..., Any], TaskSpec] = {}

def register_pipeline(spec: PipelineSpec):
    PIPELINES[spec.name] = spec

def register_task(spec: TaskSpec):
    TASKS_BY_FN[spec.fn] = spec