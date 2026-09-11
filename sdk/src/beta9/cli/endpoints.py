"""`beta9 endpoints`: validate and deploy a hosted endpoints repo.

The repo holds one Beam app per model under ``endpoints/<id>/app.py`` and a
``config.yaml`` with placement. ``validate`` sends every app's spec and the
config to the gateway as a dry run; ``deploy`` deploys each app through the
ordinary stub RPCs and then applies the config. Both print exactly what is
wrong, per app, and exit non-zero on any problem.
"""

import importlib.util
import json
import os
import sys
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import click

from .. import terminal
from ..abstractions.managed_endpoint import ManagedEndpoint
from ..config import ConfigContext, get_config_context
from .extraclick import ClickCommonGroup, ClickManagementGroup, selected_context


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@click.group(
    name="endpoints", help="Validate and deploy a hosted endpoints repo.", cls=ClickManagementGroup
)
def management():
    pass


def _load(app: Path) -> ManagedEndpoint:
    """Import app.py from its own directory and return its one ManagedEndpoint."""
    name = "endpoint_app_" + app.parent.name.replace("-", "_").replace(".", "_")
    spec = importlib.util.spec_from_file_location(name, app)
    module = importlib.util.module_from_spec(spec)
    before_modules, before_path, cwd = set(sys.modules), list(sys.path), os.getcwd()
    sys.path.insert(0, str(app.parent))
    os.chdir(app.parent)
    try:
        spec.loader.exec_module(module)
        found = [v for v in vars(module).values() if isinstance(v, ManagedEndpoint)]
    finally:
        os.chdir(cwd)
        sys.path[:] = before_path
        for added in set(sys.modules) - before_modules:
            origin = getattr(sys.modules[added], "__file__", None)
            if origin and Path(origin).resolve().is_relative_to(app.parent.resolve()):
                sys.modules.pop(added, None)
    if len(found) != 1:
        raise ValueError(f"expected exactly one ManagedEndpoint, found {len(found)}")
    return found[0]


def _apps(repo: Path) -> List[Dict[str, Any]]:
    """One entry per app directory: its spec and image, or why it failed to import."""
    root = repo / "endpoints"
    out = []
    for app in sorted(root.glob("**/app.py")):
        path = app.parent.relative_to(root).as_posix()
        entry: Dict[str, Any] = {"path": path}
        try:
            endpoint = _load(app)
            entry.update(
                id=endpoint.id,
                spec_json=json.dumps(endpoint.spec()),
                image=endpoint.image.base_image or "",
            )
            entry["endpoint"] = endpoint
        except BaseException as exc:  # noqa: BLE001  (an app may sys.exit() at import)
            traceback.print_exc()
            entry["error"] = (
                f"import failed: {exc.code}"
                if isinstance(exc, SystemExit)
                else f"import failed: {exc!r}"
            )
        out.append(entry)
    return out


def _apply(
    context: ConfigContext, repo: Path, apps: List[Dict[str, Any]], dry_run: bool
) -> Dict[str, Any]:
    body = {
        "repo_url": os.environ.get("GITHUB_REPOSITORY", ""),
        "ref": os.environ.get("GITHUB_REF_NAME", ""),
        "sha": os.environ.get("GITHUB_SHA", ""),
        "config_yaml": (repo / "config.yaml").read_text()
        if (repo / "config.yaml").exists()
        else "",
        "endpoints": [{k: v for k, v in app.items() if k != "endpoint"} for app in apps],
        "dry_run": dry_run,
    }
    request = Request(
        context.http_url + "/api/v1/endpoints/gitops/apply",
        method="POST",
        data=json.dumps(body).encode(),
        headers={
            "Authorization": "Bearer " + (context.token or ""),
            "Content-Type": "application/json",
        },
    )
    try:
        with urlopen(request, timeout=120) as response:
            return json.load(response)
    except HTTPError as exc:
        try:
            return json.loads(exc.read())
        except ValueError:
            terminal.error(f"gateway returned HTTP {exc.code}: {exc.reason}")


def _report(result: Dict[str, Any]) -> None:
    for warning in result.get("warnings") or []:
        terminal.warn(warning)
    for error in result.get("errors") or []:
        terminal.error(error, exit=False)
    if result.get("ok"):
        terminal.success("All endpoints valid.")
    else:
        terminal.error(result.get("err_msg") or "validation failed")


@management.command(
    name="validate", help="Check every app and config.yaml against the gateway without deploying."
)
@click.argument("repo", type=click.Path(exists=True, file_okay=False, path_type=Path), default=".")
def validate(repo: Path):
    apps = _apps(repo)
    _report(_apply(get_config_context(selected_context()), repo, apps, dry_run=True))


@management.command(name="deploy", help="Deploy every app, then apply config.yaml.")
@click.argument("repo", type=click.Path(exists=True, file_okay=False, path_type=Path), default=".")
def deploy(repo: Path):
    context = get_config_context(selected_context())
    apps = _apps(repo)
    check = _apply(context, repo, apps, dry_run=True)
    if not check.get("ok"):
        _report(check)
    for app in apps:
        endpoint: Optional[ManagedEndpoint] = app.pop("endpoint", None)
        if endpoint is None:
            continue
        terminal.header(f"Deploying {endpoint.id}")
        try:
            out, ok = endpoint.deploy(context=context)
        except BaseException as exc:  # noqa: BLE001
            traceback.print_exc()
            out, ok = {}, False
            endpoint.deploy_error = f"deploy failed: {exc!r}"
        if ok:
            app.update(stub_id=out.get("stub_id") or "", version=int(out.get("version") or 0))
        else:
            app["error"] = endpoint.deploy_error or "deploy failed"
    _report(_apply(context, repo, apps, dry_run=False))
