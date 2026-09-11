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
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, List, Optional

import click

from .. import terminal
from ..abstractions.managed_endpoint import ManagedEndpoint
from ..channel import get_channel, handle_error
from ..clients.managedendpoint import (
    ApplyRepoRequest,
    ApplyRepoResponse,
    EndpointAdminServiceStub,
    RepoEndpoint,
)
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


@dataclass
class App:
    """One app directory: its endpoint, or why it failed to import; then its deploy outcome."""

    dir: Path
    path: str  # relative to endpoints/, which is also the endpoint id
    endpoint: Optional[ManagedEndpoint] = None
    error: str = ""
    stub_id: str = ""
    version: int = 0

    def to_proto(self) -> RepoEndpoint:
        out = RepoEndpoint(
            path=self.path, error=self.error, stub_id=self.stub_id, version=self.version
        )
        if self.endpoint is not None:
            out.id = self.endpoint.id
            out.spec_json = json.dumps(self.endpoint.spec())
            out.image = self.endpoint.image.base_image or ""
        return out


@contextmanager
def _inside(directory: Path) -> Iterator[None]:
    """Run with ``directory`` as cwd and on sys.path, then forget the modules it added."""
    before_modules, before_path, cwd = set(sys.modules), list(sys.path), os.getcwd()
    sys.path.insert(0, str(directory))
    os.chdir(directory)
    try:
        yield
    finally:
        os.chdir(cwd)
        sys.path[:] = before_path
        for name in set(sys.modules) - before_modules:
            origin = getattr(sys.modules[name], "__file__", None)
            if origin and Path(origin).resolve().is_relative_to(directory.resolve()):
                sys.modules.pop(name, None)


def _load(app: Path) -> ManagedEndpoint:
    """Import app.py from its own directory and return its one ManagedEndpoint."""
    name = "endpoint_app_" + app.parent.name.replace("-", "_").replace(".", "_")
    spec = importlib.util.spec_from_file_location(name, app)
    module = importlib.util.module_from_spec(spec)
    with _inside(app.parent):
        spec.loader.exec_module(module)
    found = [v for v in vars(module).values() if isinstance(v, ManagedEndpoint)]
    if len(found) != 1:
        raise ValueError(f"expected exactly one ManagedEndpoint, found {len(found)}")
    return found[0]


def _apps(repo: Path) -> List[App]:
    root = repo / "endpoints"
    apps = []
    for file in sorted(root.glob("**/app.py")):
        app = App(dir=file.parent, path=file.parent.relative_to(root).as_posix())
        try:
            app.endpoint = _load(file)
        except (Exception, SystemExit) as exc:  # an app may sys.exit() at import
            traceback.print_exc()
            app.error = f"import failed: {exc.code if isinstance(exc, SystemExit) else exc!r}"
        apps.append(app)
    return apps


def _deploy(app: App, context: ConfigContext) -> None:
    """Deploy one app from its directory, as `beta9 deploy` would, recording the outcome."""
    terminal.header(f"Deploying {app.endpoint.id}")
    try:
        with _inside(app.dir):
            out, ok = app.endpoint.deploy(context=context)
    except Exception as exc:
        traceback.print_exc()
        out, ok = {}, False
        app.endpoint.deploy_error = f"deploy failed: {exc!r}"
    if ok:
        app.stub_id, app.version = out.get("stub_id") or "", int(out.get("version") or 0)
    else:
        app.error = app.endpoint.deploy_error or "deploy failed"


def _commit() -> Dict[str, str]:
    """The commit being deployed, from GitHub Actions; empty outside CI."""
    return {
        "repo_url": os.environ.get("GITHUB_REPOSITORY", ""),
        "ref": os.environ.get("GITHUB_REF_NAME", ""),
        "sha": os.environ.get("GITHUB_SHA", ""),
    }


def _apply(
    context: ConfigContext, repo: Path, apps: List[App], dry_run: bool, commit: Dict[str, str]
) -> ApplyRepoResponse:
    """Send the repo to the gateway. A dry run that names a commit announces a deploy."""
    config = repo / "config.yaml"
    request = ApplyRepoRequest(
        config_yaml=config.read_text() if config.exists() else "",
        endpoints=[app.to_proto() for app in apps],
        dry_run=dry_run,
        **commit,
    )
    with handle_error(), get_channel(context) as channel:
        return EndpointAdminServiceStub(channel).apply_repo(request)


def _report(result: ApplyRepoResponse) -> None:
    """Print warnings and errors; exit non-zero unless the gateway accepted the repo."""
    for warning in result.warnings:
        terminal.warn(warning)
    for error in result.errors:
        terminal.error(error, exit=False)
    if not result.ok:
        terminal.error(result.err_msg or "validation failed")
    terminal.success("All endpoints valid.")


repo_argument = click.argument(
    "repo", type=click.Path(exists=True, file_okay=False, path_type=Path), default="."
)


@management.command(
    name="validate", help="Check every app and config.yaml against the gateway without deploying."
)
@repo_argument
def validate(repo: Path):
    context = get_config_context(selected_context())
    _report(_apply(context, repo, _apps(repo), dry_run=True, commit={}))


@management.command(name="deploy", help="Deploy every app, then apply config.yaml.")
@repo_argument
def deploy(repo: Path):
    context = get_config_context(selected_context())
    apps = _apps(repo.resolve())
    commit = _commit()
    check = _apply(context, repo, apps, dry_run=True, commit=commit)
    if not check.ok:
        _report(check)
    for app in apps:
        if app.endpoint is not None:
            _deploy(app, context)
    _report(_apply(context, repo, apps, dry_run=False, commit=commit))
