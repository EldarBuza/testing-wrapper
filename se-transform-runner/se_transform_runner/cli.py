import argparse
import importlib
import json
import os
import pkgutil
import sys
from typing import Iterable, Optional

from se_transform_sdk.registry import PIPELINES, CALL_RECORDER
from se_transform_sdk.executor import run as run_exec
from .api import BackendAPI

def _fake_params(param_schema: dict) -> dict:
    out = {}
    for k,v in param_schema.items():
        out[k] = "1970-01-01" if ("date" in k.lower() or "ds" == k) else ""
    return out

def _add_entry_terminal(graph:dict) -> dict:
    nodes = [n["id"] for n in graph["nodes"]]
    incoming = {n: 0 for n in nodes}
    outgoing = {n: 0 for n in nodes}

    for e in graph["edges"]:
        if e["from"] in outgoing:
            outgoing[e["from"]] += 1
        if e["to"] in incoming:
            incoming[e["to"]] += 1
    entry = [n for n in nodes if incoming[n] == 0]
    terminal = [n for n in nodes if outgoing[n] == 0]
    graph["entry"] = entry
    graph["terminal"] = terminal
    return graph

def _build_dag_for_pipeline(spec) -> dict:
    CALL_RECORDER.enabled = True
    CALL_RECORDER.calls = []
    try:
        fake = _fake_params(spec.params)
        try:
            spec.fn(**fake)
        except Exception:
            pass
    finally:
        CALL_RECORDER.enabled = False
    
    seq = list(CALL_RECORDER.calls)
    seen = set()
    nodes = []
    for name in seq:
        if name not in seen:
            nodes.append({"id": name, "label": name})
            seen.add(name)
    
    edges = []
    for i in range(len(seq) - 1):
        edges.append({"from": seq[i], "to": seq[i+1]})
    graph = {"nodes": nodes, "edges": edges}
    return _add_entry_terminal(graph)

def discover_packages(roots: Optional[Iterable[str]]):
    if not roots:
        return
    for root_pkg in roots:
        try:
            pkg = importlib.import_module(root_pkg)
        except Exception as e:
            continue
        if hasattr(pkg, "__path__"):
            for _, modname, _ in pkgutil.walk_packages(
                pkg.__path__, prefix=pkg.__name__ + "."
            ):
                try:
                    importlib.import_module(modname)
                except Exception:
                    pass


def _pipeline_id(repo: str, qualname: str) -> str:
    return f"se://{repo}::{qualname}"


def cmd_discover(args):
    roots = args.root or []
    discover_packages(roots)

    repo = os.getenv("GITHUB_REPOSITORY", "local/repo")
    ref = os.getenv("GITHUB_SHA", "")
    manifest = {"repo": repo, "ref": ref, "pipelines": []}
    
    for _, spec in PIPELINES.items():
        pid = _pipeline_id(repo, spec.qualname)
        dag = _build_dag_for_pipeline(spec)
        manifest["pipelines"].append(
            {
                "id": pid,
                "name": spec.name,
                "entrypoint": spec.qualname,
                "params": list(spec.params.keys()),
                "module": spec.module,
                "graph": dag,
            }
        )

    if args.require and not manifest["pipelines"]:
        print("No pipelines discovered.", file=sys.stderr)
        sys.exit(2)

    print(json.dumps(manifest, indent=2))


def cmd_register(args):
    data = json.load(sys.stdin) if args.from_stdin else json.loads(args.manifest)
    api = BackendAPI()
    api.register_manifest(data)
    print("Registered pipelines.")


def cmd_run(args):
    if args.entrypoint:
        module, fn_name = args.entrypoint.split(":")
    else:
        roots = args.root or []
        discover_packages(roots)
        spec = next((p for p in PIPELINES.values() if p.name == args.pipeline), None)
        if not spec:
            raise SystemExit(f"Pipeline not found by name: {args.pipeline}")
        module, fn_name = spec.qualname.split(":")

    mod = importlib.import_module(module)
    fn = getattr(mod, fn_name)
    params = json.loads(args.params or "{}")
    run_exec(fn, params)


def main():
    ap = argparse.ArgumentParser("se-transform")
    sub = ap.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("discover", help="Discover pipelines and print manifest JSON")
    d.add_argument(
        "--root",
        action="append",
        help="Root package to import for discovery (repeatable, e.g., --root pipelines --root more)",
    )
    d.add_argument(
        "--require",
        action="store_true",
        help="Exit non-zero if no pipelines are discovered (CI-friendly)",
    )
    d.set_defaults(func=cmd_discover)

    r = sub.add_parser("register", help="Register manifest with backend API")
    r.add_argument(
        "--manifest", help="Manifest JSON string (omit when using --from-stdin)"
    )
    r.add_argument("--from-stdin", action="store_true")
    r.set_defaults(func=cmd_register)

    e = sub.add_parser("run", help="Run a pipeline by name or entrypoint")
    e.add_argument("--pipeline", help="Pipeline name (requires --root for discovery)")
    e.add_argument("--entrypoint", help="module:function (preferred for reproducibility)")
    e.add_argument("--params", help='JSON params, e.g. {"ds":"2025-10-01"}')
    e.add_argument(
        "--root",
        action="append",
        help="Root package(s) to import for discovery when using --pipeline",
    )
    e.set_defaults(func=cmd_run)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()