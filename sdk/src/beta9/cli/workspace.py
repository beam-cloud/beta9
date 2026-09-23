"""
`workspace duplicate|sync`: copy every app's latest active deployment,
secrets and databases from one context to another over existing gateway routes.
"""

import hashlib
import os
import tempfile
from typing import Any, Dict, List, Optional, Set

import click
import requests
from betterproto import Casing

from .. import terminal
from ..channel import GatewayHTTPError, ServiceClient
from ..clients.gateway import (
    DeployStubRequest,
    ListDeploymentsRequest,
)
from ..clients.secret import (
    CreateSecretRequest,
    GetSecretRequest,
    ListSecretsRequest,
    UpdateSecretRequest,
)
from ..config import get_config_context
from ..sync import FileSyncer
from .extraclick import ClickCommonGroup
from .stubconfig import stub_config, stub_request_from_config


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@common.group(name="workspace", help="Use workspaces as environments (staging, preview, prod).")
def workspace():
    pass


class _Env:
    """A client plus its latest active deployments."""

    def __init__(self, context_name: str):
        self.name = context_name
        self.config = get_config_context(context_name)
        if not self.config.is_valid():
            raise click.ClickException(f"Context {context_name!r} is not configured.")
        self.client = ServiceClient(self.config)

    @property
    def http(self):
        return self.client.http

    def latest_deployments(self) -> Dict[str, Dict[str, Any]]:
        res = self.client.gateway.list_deployments(ListDeploymentsRequest(limit=1000))
        if not res.ok:
            raise click.ClickException(res.err_msg or "Unable to list deployments")
        latest: Dict[str, Dict[str, Any]] = {}
        for d in res.deployments:
            row = d.to_dict(casing=Casing.SNAKE)  # type: ignore[attr-defined]
            if row.get("active") and row["version"] > latest.get(row["name"], {}).get(
                "version", -1
            ):
                latest[row["name"]] = row
        return latest

    def close(self) -> None:
        self.client.close()


def _copy_object(src: _Env, dst: _Env, stub_id: str) -> Optional[str]:
    """Copy a stub's code bundle from src to dst; returns the new object id."""
    r = src.http.request("GET", f"/api/v1/deployment/{{ws}}/download/{stub_id}", timeout=600)
    if r.status_code != 200 or not r.content:
        return None
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp.write(r.content)
    try:
        object_id = FileSyncer(dst.client.gateway).upload_archive(
            tmp.name, hashlib.sha256(r.content).hexdigest()
        )
    finally:
        os.remove(tmp.name)
    if object_id is None:
        raise click.ClickException("Failed to upload the code object to the target workspace")
    return object_id


def _copy_secrets(src: _Env, dst: _Env, force: bool) -> Dict[str, List[str]]:
    listed = src.client.secret.list_secrets(ListSecretsRequest())
    if not listed.ok:
        raise click.ClickException(listed.err_msg or "Unable to list secrets")
    existing: Set[str] = set()
    dst_listed = dst.client.secret.list_secrets(ListSecretsRequest())
    if dst_listed.ok:
        existing = {s.name for s in dst_listed.secrets}
    copied, skipped = [], []
    for secret in listed.secrets:
        if secret.name.startswith("BETA9_") and (
            "_URL" in secret.name
            or "_PASSWORD" in secret.name
            or "_USERNAME" in secret.name
            or "_DATABASE" in secret.name
        ):
            # Credentials are regenerated on recreate.
            skipped.append(secret.name)
            continue
        value = src.client.secret.get_secret(GetSecretRequest(name=secret.name))
        if not value.ok:
            skipped.append(secret.name)
            continue
        if secret.name in existing:
            if not force:
                skipped.append(secret.name)
                continue
            dst.client.secret.update_secret(
                UpdateSecretRequest(name=secret.name, value=value.secret.value)
            )
        else:
            dst.client.secret.create_secret(
                CreateSecretRequest(name=secret.name, value=value.secret.value)
            )
        copied.append(secret.name)
    return {"copied": copied, "skipped": skipped}


def _copy_databases(src: _Env, dst: _Env) -> Dict[str, List[str]]:
    try:
        src_dbs = src.http.json("GET", "/api/v1/database/{ws}") or []
        dst_dbs = {d["name"] for d in dst.http.json("GET", "/api/v1/database/{ws}") or []}
    except (requests.RequestException, GatewayHTTPError):
        return {"created": [], "skipped": ["(gateway has no database routes)"]}
    created, skipped = [], []
    for db in src_dbs:
        if db["name"] in dst_dbs:
            skipped.append(db["name"])
            continue
        dst.http.json(
            "POST",
            "/api/v1/database/{ws}",
            json={"kind": db["kind"], "name": db["name"]},
            timeout=660,
        )
        created.append(db["name"])
    return {"created": created, "skipped": skipped}


def _copy_apps(
    src: _Env, dst: _Env, only: Optional[List[str]], skip_existing: bool
) -> List[Dict[str, Any]]:
    src_latest = src.latest_deployments()
    dst_latest = dst.latest_deployments() if skip_existing else {}
    results = []
    for name, deployment in sorted(src_latest.items()):
        if only and name not in only:
            continue
        if deployment.get("stub_type") == "pod/deployment" and _is_database(
            src, deployment["stub_id"]
        ):
            continue  # handled by _copy_databases
        if name in dst_latest:
            results.append({"name": name, "status": "skipped", "reason": "exists"})
            continue
        try:
            stub = src.http.json("GET", f"/api/v1/stub/{{ws}}/{deployment['stub_id']}")
            config = stub_config(stub)
            request = stub_request_from_config(stub, config)
            object_id = (
                _copy_object(src, dst, deployment["stub_id"]) if request.get("object_id") else ""
            )
            request["object_id"] = object_id or ""
            created = dst.http.json("POST", "/api/v1/gateway/stubs", json=request, timeout=600)
            if not created.get("ok"):
                raise click.ClickException(created.get("errMsg") or "stub creation failed")
            deployed = dst.client.gateway.deploy_stub(
                DeployStubRequest(stub_id=created["stubId"], name=name)
            )
            if not deployed.ok:
                raise click.ClickException(deployed.err_msg or "deploy failed")
            results.append(
                {
                    "name": name,
                    "status": "deployed",
                    "deployment_id": deployed.deployment_id,
                    "version": deployed.version,
                }
            )
        except Exception as exc:  # keep going; report per app
            results.append({"name": name, "status": "failed", "reason": str(exc)})
    return results


def _is_database(env: _Env, stub_id: str) -> bool:
    try:
        cfg = stub_config(env.http.json("GET", f"/api/v1/stub/{{ws}}/{stub_id}"))
    except (requests.RequestException, GatewayHTTPError):
        return False
    serving = cfg.get("serving") or {}
    return bool((serving.get("database") or {}).get("kind"))


def _run(
    source: str,
    target: str,
    only: Optional[List[str]],
    secrets: bool,
    databases: bool,
    skip_existing: bool,
    force_secrets: bool,
    format: str,
):
    if source == target:
        raise click.ClickException("Source and target contexts must differ.")
    src, dst = _Env(source), _Env(target)
    try:
        report: Dict[str, Any] = {"source": src.workspace_id, "target": dst.workspace_id}
        if secrets:
            report["secrets"] = _copy_secrets(src, dst, force_secrets)
        if databases:
            report["databases"] = _copy_databases(src, dst)
        report["apps"] = _copy_apps(src, dst, only, skip_existing)
    finally:
        src.close()
        dst.close()

    if format == "json" or terminal.json_output():
        terminal.print_json(report)
        return
    for app in report["apps"]:
        line = (
            f"{app['name']}: {app['status']}"
            + (f" v{app['version']}" if app.get("version") else "")
            + (f" ({app['reason']})" if app.get("reason") else "")
        )
        (terminal.success if app["status"] == "deployed" else terminal.detail)(line)
    if "secrets" in report:
        terminal.detail(
            f"secrets copied: {len(report['secrets']['copied'])}, skipped: {len(report['secrets']['skipped'])}"
        )
    if "databases" in report:
        terminal.detail(
            f"databases created: {report['databases']['created']}, skipped: {report['databases']['skipped']}"
        )


@workspace.command(
    name="duplicate",
    help="Create the source workspace's apps, secrets and databases in the target context.",
)
@click.argument("source")
@click.argument("target")
@click.option("--only", multiple=True, help="Copy only these app names (repeatable).")
@click.option("--secrets/--no-secrets", default=True, show_default=True)
@click.option("--databases/--no-databases", default=True, show_default=True)
@click.option("--format", type=click.Choice(("table", "json")), default="table", show_default=True)
def duplicate(
    source: str, target: str, only: List[str], secrets: bool, databases: bool, format: str
):
    _run(
        source,
        target,
        list(only) or None,
        secrets,
        databases,
        skip_existing=False,
        force_secrets=True,
        format=format,
    )


@workspace.command(
    name="sync",
    help="Bring the target up to date: deploy apps missing there, add missing secrets and databases.",
)
@click.argument("source")
@click.argument("target")
@click.option("--only", multiple=True, help="Sync only these app names (repeatable).")
@click.option("--secrets/--no-secrets", default=True, show_default=True)
@click.option("--databases/--no-databases", default=True, show_default=True)
@click.option(
    "--overwrite-secrets", is_flag=True, help="Replace secrets that already exist in the target."
)
@click.option("--format", type=click.Choice(("table", "json")), default="table", show_default=True)
def sync(
    source: str,
    target: str,
    only: List[str],
    secrets: bool,
    databases: bool,
    overwrite_secrets: bool,
    format: str,
):
    _run(
        source,
        target,
        list(only) or None,
        secrets,
        databases,
        skip_existing=True,
        force_secrets=overwrite_secrets,
        format=format,
    )


@workspace.command(
    name="diff", help="Show apps, secrets and databases that differ between two contexts."
)
@click.argument("source")
@click.argument("target")
def diff(source: str, target: str):
    src, dst = _Env(source), _Env(target)
    try:
        s_apps, d_apps = src.latest_deployments(), dst.latest_deployments()
        s_secrets = {s.name for s in src.client.secret.list_secrets(ListSecretsRequest()).secrets}
        d_secrets = {s.name for s in dst.client.secret.list_secrets(ListSecretsRequest()).secrets}
    finally:
        src.close()
        dst.close()
    terminal.print_json(
        {
            "apps_only_in_source": sorted(set(s_apps) - set(d_apps)),
            "apps_only_in_target": sorted(set(d_apps) - set(s_apps)),
            "secrets_only_in_source": sorted(s_secrets - d_secrets),
            "secrets_only_in_target": sorted(d_secrets - s_secrets),
        }
    )
