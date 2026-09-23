"""
`beam infra`: `.beam/beam.yaml` (a template manifest plus required `secrets`)
planned and applied against the workspace. Deletes need `--confirm-destructive`.
"""

import json
import os
import sys
from typing import Any, Dict, List

import click
import yaml

from .. import terminal
from ..channel import GatewayHTTPError, ServiceClient
from ..clients.gateway import ListDeploymentsRequest
from ..clients.secret import CreateSecretRequest, ListSecretsRequest
from . import extraclick
from .extraclick import ClickCommonGroup
from .mcp import LIVE_PATHS
from .stubconfig import stub_config
from .template import (
    TemplateError,
    create_database_step,
    manifest_service,
    memory_mb,
    plan_steps,
    run_deploy_step,
    validate_manifest,
)

DEFAULT_PATH = os.path.join(".beam", "beam.yaml")
EXIT_CHANGES = 2


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@common.group(name="infra", help="Describe a workspace in .beam/beam.yaml and reconcile it.")
def infra():
    pass


# --- live state ---------------------------------------------------------------------


def _live_state(service: ServiceClient) -> Dict[str, Any]:
    res = service.gateway.list_deployments(ListDeploymentsRequest(limit=1000))
    if not res.ok:
        raise click.ClickException(res.err_msg or "Unable to list deployments")
    latest: Dict[str, Any] = {}
    for d in res.deployments:
        if d.active and (d.name not in latest or d.version > latest[d.name].version):
            latest[d.name] = d
    apps: Dict[str, Dict[str, Any]] = {}
    for name, d in latest.items():
        cfg = stub_config(service.http.json("GET", f"/api/v1/stub/{{ws}}/{d.stub_id}"))
        db = (cfg.get("serving") or {}).get("database") or {}
        apps[name] = {
            "stub_id": d.stub_id,
            "stub_type": d.stub_type,
            "config": cfg,
            "database": db.get("kind", ""),
        }
    secrets = service.secret.list_secrets(ListSecretsRequest())
    return {"apps": apps, "secrets": {s.name for s in secrets.secrets} if secrets.ok else set()}


def _desired_config(svc: Dict[str, Any]) -> Dict[str, Any]:
    """Live-comparable view of a manifest service (dotted config paths)."""
    out: Dict[str, Any] = {}
    res = svc.get("resources") or {}
    if res.get("cpu") is not None:
        out["runtime.cpu"] = int(float(res["cpu"]) * 1000)
    if res.get("memory") is not None:
        out["runtime.memory"] = memory_mb(res["memory"])
    if res.get("gpu"):
        out["runtime.gpus"] = [res["gpu"]]
    rep = svc.get("replicas") or {}
    if rep.get("min") is not None:
        out["autoscaler.min_containers"] = int(rep["min"])
    if rep.get("max") is not None:
        out["autoscaler.max_containers"] = int(rep["max"])
    if svc.get("keep_warm_seconds") is not None:
        out["keep_warm_seconds"] = int(svc["keep_warm_seconds"])
    if svc.get("env"):
        out["env"] = sorted(f"{k}={v}" for k, v in svc["env"].items())
    return out


def _live_value(cfg: Dict[str, Any], path: str) -> Any:
    if path == "env":
        env = cfg.get("env") or []
        # References became secret bindings at deploy; compare plain env keys only.
        return sorted(env)
    cur: Any = cfg
    for part in path.split("."):
        cur = (cur or {}).get(part) if isinstance(cur, dict) else None
    return cur


# --- plan ----------------------------------------------------------------------------


def compute_plan(manifest: Dict[str, Any], live: Dict[str, Any]) -> Dict[str, Any]:
    desired = manifest.get("services") or {}
    plan: Dict[str, Any] = {
        "create": [],
        "update": [],
        "redeploy": [],
        "delete": [],
        "secrets_missing": [],
        "unchanged": [],
    }
    for name, svc in desired.items():
        if name not in live["apps"]:
            plan["create"].append({"service": name, "kind": svc["kind"]})
            continue
        if svc["kind"] == "database":
            plan["unchanged"].append(name)
            continue
        cfg = live["apps"][name]["config"]
        drift = {}
        for path, want in _desired_config(svc).items():
            have = _live_value(cfg, path)
            if path == "env":
                # Flag env only when keys are neither plain nor secret-bound.
                bound = {s.get("env_name") or s["name"] for s in cfg.get("secrets") or []}
                have_keys = {e.split("=", 1)[0] for e in have} | bound
                want_keys = {e.split("=", 1)[0] for e in want}
                if want_keys - have_keys:
                    drift[path] = {"have": sorted(have_keys), "want": sorted(want_keys)}
                continue
            if json.dumps(have, sort_keys=True) != json.dumps(want, sort_keys=True):
                drift[path] = {"have": have, "want": want}
        if not drift:
            plan["unchanged"].append(name)
        elif all(p in LIVE_PATHS for p in drift):
            plan["update"].append(
                {
                    "service": name,
                    "fields": {p: d["want"] for p, d in drift.items()},
                    "drift": drift,
                }
            )
        else:
            plan["redeploy"].append({"service": name, "drift": drift})
    for name in live["apps"]:
        if name not in desired:
            plan["delete"].append({"service": name, "stub_id": live["apps"][name]["stub_id"]})
    for secret in manifest.get("secrets") or []:
        if secret not in live["secrets"]:
            plan["secrets_missing"].append(secret)
    plan["has_changes"] = any(
        plan[k] for k in ("create", "update", "redeploy", "delete", "secrets_missing")
    )
    return plan


def _print_plan(plan: Dict[str, Any]) -> None:
    for item in plan["create"]:
        terminal.print(f"+ create   {item['service']} ({item['kind']})")
    for item in plan["update"]:
        terminal.print(
            f"~ update   {item['service']}: "
            + ", ".join(f"{p} {d['have']} → {d['want']}" for p, d in item["drift"].items())
        )
    for item in plan["redeploy"]:
        terminal.print(
            f"~ redeploy {item['service']}: "
            + ", ".join(f"{p} {d['have']} → {d['want']}" for p, d in item["drift"].items())
        )
    for item in plan["delete"]:
        terminal.print(f"- delete   {item['service']}  (requires --confirm-destructive)")
    for name in plan["secrets_missing"]:
        terminal.print(f"+ secret   {name}  (value from ${name} in your environment)")
    if not plan["has_changes"]:
        terminal.success("No changes. The workspace matches the manifest.")
    else:
        terminal.detail(f"{len(plan['unchanged'])} unchanged")


def _load(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise click.ClickException(
            f"{path} not found. Run `infra init` to create it from the workspace."
        )
    with open(path) as f:
        manifest = yaml.safe_load(f) or {}
    validate_manifest(manifest)
    return manifest


# --- commands ----------------------------------------------------------------------


@infra.command(
    name="init",
    help="Write .beam/beam.yaml describing the workspace's current apps, databases and secret names.",
)
@click.option("--path", default=DEFAULT_PATH, show_default=True)
@click.option("--force", is_flag=True, help="Overwrite an existing file.")
@extraclick.pass_service_client
def init(service: ServiceClient, path: str, force: bool):
    if os.path.exists(path) and not force:
        raise click.ClickException(
            f"{path} exists; use --force to overwrite or `infra pull` to refresh."
        )
    _write_from_live(service, path)


@infra.command(
    name="pull", help="Refresh .beam/beam.yaml from the workspace (keeps your name/description)."
)
@click.option("--path", default=DEFAULT_PATH, show_default=True)
@extraclick.pass_service_client
def pull(service: ServiceClient, path: str):
    _write_from_live(service, path)


def _write_from_live(service: ServiceClient, path: str) -> None:
    live = _live_state(service)
    services = {name: manifest_service(app["config"]) for name, app in sorted(live["apps"].items())}
    existing: Dict[str, Any] = {}
    if os.path.exists(path):
        with open(path) as f:
            existing = yaml.safe_load(f) or {}
    manifest = {
        "name": existing.get("name") or os.path.basename(os.getcwd()),
        "description": existing.get("description") or "Beam workspace definition",
        "services": services,
        "secrets": sorted(live["secrets"]),
    }
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        yaml.safe_dump(manifest, f, sort_keys=False)
    if terminal.json_output():
        terminal.print_json(
            {"path": path, "services": len(services), "secrets": len(live["secrets"])}
        )
    else:
        terminal.success(
            f"Wrote {path} ({len(services)} services, {len(live['secrets'])} secret names)"
        )


@infra.command(
    name="plan",
    help="Show what `apply` would change. Exit 2 when there are changes with --detailed-exit-code.",
)
@click.option("--path", default=DEFAULT_PATH, show_default=True)
@click.option(
    "--detailed-exit-code", is_flag=True, help="Exit 0 = no changes, 2 = changes, 1 = error."
)
@extraclick.pass_service_client
def plan(service: ServiceClient, path: str, detailed_exit_code: bool):
    manifest = _load(path)
    result = compute_plan(manifest, _live_state(service))
    if terminal.json_output():
        terminal.print_json(result)
    else:
        _print_plan(result)
    if detailed_exit_code and result["has_changes"]:
        sys.exit(EXIT_CHANGES)


@infra.command(name="apply", help="Reconcile the workspace with .beam/beam.yaml.")
@click.option("--path", default=DEFAULT_PATH, show_default=True)
@click.option("--yes", "-y", is_flag=True, help="Skip the confirmation prompt.")
@click.option(
    "--confirm-destructive", is_flag=True, help="Allow deleting apps that are not in the manifest."
)
@extraclick.pass_service_client
def apply(service: ServiceClient, path: str, yes: bool, confirm_destructive: bool):
    manifest = _load(path)
    live = _live_state(service)
    result = compute_plan(manifest, live)
    if not result["has_changes"]:
        if terminal.json_output():
            terminal.print_json({"applied": [], "plan": result})
        else:
            terminal.success("No changes.")
        return
    if not terminal.json_output():
        _print_plan(result)
    if result["delete"] and not confirm_destructive:
        terminal.warn("Deletions are skipped without --confirm-destructive.")
    if not yes and not terminal.confirm("Apply these changes?", default=False):
        terminal.error("Cancelled.", code="CANCELLED")

    context = extraclick.selected_context()
    applied: List[Dict[str, Any]] = []

    for name in result["secrets_missing"]:
        value = os.getenv(name)
        if not value:
            applied.append(
                {
                    "secret": name,
                    "status": "skipped",
                    "reason": f"${name} not set in the environment",
                }
            )
            continue
        res = service.secret.create_secret(CreateSecretRequest(name=name, value=value))
        applied.append(
            {
                "secret": name,
                "status": "created" if res.ok else "failed",
                "reason": res.err_msg or None,
            }
        )

    steps = {s["service"]: s for s in plan_steps(manifest)}
    for item in result["create"] + result["redeploy"]:
        step = steps[item["service"]]
        try:
            outcome = (
                create_database_step(service, step)
                if step["kind"] == "database"
                else run_deploy_step(step, context)
            )
            applied.append({"service": item["service"], **outcome})
        except TemplateError as exc:
            applied.append({"service": item["service"], "status": "failed", "error": exc.message})

    for item in result["update"]:
        stub_id = live["apps"][item["service"]]["stub_id"]
        try:
            service.http.json(
                "PATCH", f"/api/v1/stub/{{ws}}/{stub_id}/config", json={"fields": item["fields"]}
            )
            applied.append(
                {"service": item["service"], "status": "updated", "fields": sorted(item["fields"])}
            )
        except GatewayHTTPError as exc:
            applied.append({"service": item["service"], "status": "failed", "error": exc.message})

    if confirm_destructive:
        for item in result["delete"]:
            deleted = _delete_app(service, item["service"])
            applied.append(
                {"service": item["service"], "status": "deleted" if deleted else "failed"}
            )

    if terminal.json_output():
        terminal.print_json({"applied": applied})
    else:
        for a in applied:
            label = a.get("service") or a.get("secret")
            line = f"{label}: {a['status']}" + (
                f" ({a.get('error') or a.get('reason')})"
                if a.get("error") or a.get("reason")
                else ""
            )
            if a["status"] == "failed":
                terminal.error(line, exit=False)
            else:
                terminal.success(line)
    if any(a["status"] == "failed" for a in applied):
        sys.exit(1)


def _delete_app(service: ServiceClient, name: str) -> bool:
    from ..clients.gateway import DeleteDeploymentRequest, StringList

    res = service.gateway.list_deployments(
        ListDeploymentsRequest(filters={"name": StringList([name])}, limit=100)
    )
    ok = True
    for d in res.deployments:
        if d.name == name:
            r = service.gateway.delete_deployment(DeleteDeploymentRequest(id=d.id))
            ok = ok and r.ok
    return ok
