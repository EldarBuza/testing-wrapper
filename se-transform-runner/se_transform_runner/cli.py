import argparse
import importlib
import json
import os
import pkgutil
import sys
from typing import Iterable, Optional

from se_transform_sdk.registry import PIPELINES
from se_transform_sdk.executor import run as run_exec
from .api import BackendAPI


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
        manifest["pipelines"].append(
            {
                "id": pid,
                "name": spec.name,
                "entrypoint": spec.qualname,
                "params": list(spec.params.keys()),
                "module": spec.module,
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