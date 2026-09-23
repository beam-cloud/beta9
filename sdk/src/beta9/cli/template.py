"""
`template`: multi-service templates as YAML manifests.

    name: fastapi-postgres
    description: FastAPI + Postgres + Redis
    services:
      db:    {kind: database, engine: postgres}
      cache: {kind: database, engine: redis}
      api:
        kind: repo                      # repo | image | database
        repo: https://github.com/org/app
        branch: main
        path: .                         # subdirectory with the app
        entrypoint: app.py:handler      # python entrypoint, or omit for Dockerfile/command
        command: ["uvicorn", "app:app", "--port", "8000"]
        ports: [8000]
        disks: {data: /var/lib/app}      # durable disks, name -> mount path (10Gi each)
        resources: {cpu: 1, memory: 2Gi, gpu: A10G}
        replicas: {min: 0, max: 3}
        secrets: [OPENAI_API_KEY]
        env:
          DATABASE_URL: ${{db.db.DATABASE_URL}}
          REDIS_URL: ${{db.cache.REDIS_URL}}
          SESSION_KEY: ${{secret(32)}}
      worker:
        kind: image
        image: ghcr.io/org/worker:1.2
        command: ["python", "worker.py"]
        env: {API_URL: "${{app.api.URL}}"}

The orchestrator orders services by their `${{db.*}}` / `${{app.*}}` references
and runs each through the same paths the CLI already has: databases through
the gateway's database routes, image and repo services through `deploy`.
References themselves are expanded by the gateway. Deployed services are
grouped into a stack named after the manifest.

`template import` converts a docker-compose.yml into this format.
"""

import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
from typing import Any, Dict, List, Optional, Tuple

import click
import requests
import yaml

from .. import terminal
from ..channel import GatewayHTTPError, ServiceClient
from ..config import get_settings
from ..clients.gateway import ListDeploymentsRequest, StringList
from ..references import validate_env
from . import extraclick
from .extraclick import ClickCommonGroup, cli_command, parse_last_json
from .stubconfig import stub_config

REF_RE = re.compile(r"\$\{\{\s*(db|app)\.([^.}\s]+)\.[^}]*\}\}")
DATABASE_IMAGES = {
    "postgres": "postgres",
    "postgresql": "postgres",
    "redis": "redis",
    "valkey": "redis",
    "mysql": "mysql",
    "mariadb": "mysql",
    "mongo": "mongo",
    "mongodb": "mongo",
}


class TemplateError(click.ClickException):
    pass


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@common.group(name="template", help="Deploy multi-service templates from a YAML manifest.")
def template():
    pass


# --- manifest -----------------------------------------------------------------


def load_manifest(source: str) -> Dict[str, Any]:
    """Read a manifest from a path, URL, or a name in the configured templates catalog."""
    if source.startswith(("http://", "https://")):
        text = _fetch(source)
    elif os.path.exists(source):
        with open(source) as f:
            text = f.read()
    else:
        catalog = get_settings().templates_url
        if not catalog:
            raise TemplateError(
                f"{source} is not a file or URL, and no templates catalog is configured"
            )
        text = _fetch(f"{catalog}/{source}.yaml")
    try:
        manifest = yaml.safe_load(text) or {}
    except yaml.YAMLError as exc:
        raise TemplateError(f"Invalid YAML: {exc}")
    validate_manifest(manifest)
    return manifest


def _fetch(url: str) -> str:
    try:
        r = requests.get(url, timeout=20)
    except requests.RequestException as exc:
        raise TemplateError(f"Failed to fetch {url}: {exc}")
    if r.status_code != 200:
        raise TemplateError(f"Template not found at {url} (HTTP {r.status_code})")
    return r.text


def validate_manifest(manifest: Dict[str, Any]) -> None:
    if not isinstance(manifest.get("services"), dict) or not manifest["services"]:
        raise TemplateError("Manifest needs a non-empty `services` mapping")
    problems: List[str] = []
    for name, svc in manifest["services"].items():
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{1,31}", name):
            problems.append(
                f"{name}: service names are lowercase letters, digits and dashes (2-32 chars)"
            )
        kind = (svc or {}).get("kind")
        if kind not in ("repo", "image", "database"):
            problems.append(f"{name}: kind must be repo, image or database")
        if kind == "database" and (
            svc.get("engine") not in ("postgres", "redis", "mysql", "mongo")
        ):
            problems.append(f"{name}: engine must be postgres, redis, mysql or mongo")
        if kind == "repo" and not svc.get("repo") and not svc.get("path"):
            problems.append(
                f"{name}: repo services need `repo` (git URL) or `path` (local directory)"
            )
        if kind == "image" and not svc.get("image"):
            problems.append(f"{name}: image services need `image`")
        for disk_name, mount in _disks(svc):
            if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,31}", disk_name) or not str(mount).startswith(
                "/"
            ):
                problems.append(f"{name}: disks map a short name to an absolute mount path")
        env = svc.get("env") or {}
        if not isinstance(env, dict):
            problems.append(f"{name}: env must be a mapping")
        else:
            problems.extend(
                f"{name}.env.{p}" for p in validate_env({k: str(v) for k, v in env.items()})
            )
            for value in env.values():
                for m in REF_RE.finditer(str(value)):
                    if m.group(2) not in manifest["services"]:
                        problems.append(f"{name}: env references unknown service {m.group(2)!r}")
    if problems:
        raise TemplateError("Invalid manifest:\n" + "\n".join(problems))


def _disks(svc: Dict[str, Any]) -> List[Tuple[str, str]]:
    """`disks: {name: /mount}` or `disks: [{name, mount_path, size?}]`."""
    disks = svc.get("disks") or {}
    if isinstance(disks, dict):
        return [(str(k), str(v)) for k, v in disks.items()]
    return [
        (str(d.get("name")), str(d.get("mount_path")) + (f":{d['size']}" if d.get("size") else ""))
        for d in disks
    ]


def ordered_services(manifest: Dict[str, Any]) -> List[str]:
    """Databases first, then services in dependency order (Kahn's algorithm)."""
    services = manifest["services"]
    deps: Dict[str, set] = {name: set() for name in services}
    for name, svc in services.items():
        for value in (svc.get("env") or {}).values():
            for m in REF_RE.finditer(str(value)):
                if m.group(2) in services and m.group(2) != name:
                    deps[name].add(m.group(2))
    order: List[str] = []
    remaining = dict(deps)
    while remaining:
        ready = sorted(
            [n for n, d in remaining.items() if not d - set(order)],
            key=lambda n: (services[n].get("kind") != "database", n),
        )
        if not ready:
            raise TemplateError(
                f"Circular references between services: {', '.join(sorted(remaining))}"
            )
        for n in ready:
            order.append(n)
            remaining.pop(n)
    return order


def plan_steps(manifest: Dict[str, Any], prefix: str = "") -> List[Dict[str, Any]]:
    steps = []
    for name in ordered_services(manifest):
        svc = manifest["services"][name]
        app_name = f"{prefix}{name}" if prefix else name
        step: Dict[str, Any] = {"service": name, "app_name": app_name, "kind": svc["kind"]}
        if svc["kind"] == "database":
            step["engine"] = svc["engine"]
            step["always_on"] = bool(svc.get("always_on"))
        else:
            step["command"] = _deploy_command(app_name, svc, prefix)
            if svc["kind"] == "repo":
                step["source"] = svc.get("repo") or svc.get("path")
                step["path"] = svc.get("path", ".")
                step["branch"] = svc.get("branch")
        steps.append(step)
    return steps


def _deploy_command(app_name: str, svc: Dict[str, Any], prefix: str) -> List[str]:
    args = ["deploy", "--name", app_name, "--json"]
    if svc.get("entrypoint") and svc["kind"] == "repo" and ":" in svc["entrypoint"]:
        args.append(svc["entrypoint"])
    if svc["kind"] == "image":
        args += ["--image", svc["image"]]
    if svc.get("dockerfile"):
        args += ["--dockerfile", svc["dockerfile"]]
    if svc.get("command"):
        cmd = (
            svc["command"] if isinstance(svc["command"], list) else shlex.split(str(svc["command"]))
        )
        for part in cmd:
            args += ["--entrypoint", part]
    for port in svc.get("ports") or []:
        args += ["--port", str(port)]
    for disk_name, mount in _disks(svc):
        args += ["--disk", f"{disk_name}:{mount}"]
    res = svc.get("resources") or {}
    if res.get("cpu") is not None:
        args += ["--cpu", str(res["cpu"])]
    if res.get("memory"):
        args += ["--memory", str(res["memory"])]
    if res.get("gpu"):
        args += ["--gpu", str(res["gpu"])]
    if res.get("gpu_count"):
        args += ["--gpu-count", str(res["gpu_count"])]
    for secret in svc.get("secrets") or []:
        args += ["--secrets", secret]
    for key, value in (svc.get("env") or {}).items():
        args += ["--env", f"{key}={_prefixed_reference(str(value), prefix)}"]
    replicas = svc.get("replicas") or {}
    if replicas.get("min") is not None:
        args += ["--min-replicas", str(replicas["min"])]
    if replicas.get("max") is not None:
        args += ["--max-replicas", str(replicas["max"])]
    if svc.get("keep_warm_seconds") is not None:
        args += ["--keep-warm-seconds", str(svc["keep_warm_seconds"])]
    if svc.get("pool"):
        args += ["--pool", svc["pool"]]
    return args


def _prefixed_reference(value: str, prefix: str) -> str:
    """When services are deployed under a prefix, references must follow."""
    if not prefix:
        return value
    return REF_RE.sub(
        lambda m: m.group(0).replace(
            f"{m.group(1)}.{m.group(2)}.", f"{m.group(1)}.{prefix}{m.group(2)}.", 1
        ),
        value,
    )


def memory_mb(value: Any) -> int:
    """Accept 512, "512", "2Gi" or "2G"."""
    text = str(value).strip().lower()
    for suffix, factor in (("gi", 1024), ("g", 1024), ("mi", 1), ("m", 1)):
        if text.endswith(suffix):
            return int(float(text[: -len(suffix)]) * factor)
    return int(float(text))


def manifest_service(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """A manifest service describing an existing stub config; secret values are never included."""
    db = (cfg.get("serving") or {}).get("database") or {}
    autoscaler = cfg.get("autoscaler") or {}
    if db.get("kind"):
        return {
            "kind": "database",
            "engine": db["kind"],
            "always_on": bool(autoscaler.get("min_containers")),
        }
    runtime = cfg.get("runtime") or {}
    svc: Dict[str, Any] = {"kind": "image", "image": runtime.get("image_id", "")}
    resources = {
        "cpu": (runtime.get("cpu") or 0) / 1000 or None,
        "memory": runtime.get("memory"),
        "gpu": (runtime.get("gpus") or [None])[0],
    }
    if any(resources.values()):
        svc["resources"] = {k: v for k, v in resources.items() if v}
    if cfg.get("entrypoint"):
        svc["command"] = cfg["entrypoint"]
    if cfg.get("handler"):
        svc["entrypoint"] = cfg["handler"]
    if cfg.get("ports"):
        svc["ports"] = cfg["ports"]
    env = dict(e.split("=", 1) if "=" in e else (e, "") for e in cfg.get("env") or [])
    plain_secrets = []
    for secret in cfg.get("secrets") or []:
        if secret.get("env_name"):
            env[secret["env_name"]] = f"${{{{secret.{secret['name']}}}}}"
        else:
            plain_secrets.append(secret["name"])
    if env:
        svc["env"] = env
    if plain_secrets:
        svc["secrets"] = plain_secrets
    if autoscaler:
        svc["replicas"] = {
            "min": autoscaler.get("min_containers", 0),
            "max": autoscaler.get("max_containers", 1),
        }
    if cfg.get("keep_warm_seconds") is not None:
        svc["keep_warm_seconds"] = cfg["keep_warm_seconds"]
    return svc


# --- execution ------------------------------------------------------------------


SECRET_REF_RE = re.compile(r"\$\{\{\s*secret\.([A-Za-z0-9_]+)\s*\}\}")


def missing_secrets(service: ServiceClient, manifest: Dict[str, Any]) -> List[str]:
    """Workspace secrets the manifest binds that do not exist yet."""
    wanted: set = set()
    for svc in manifest["services"].values():
        wanted.update(svc.get("secrets") or [])
        for value in (svc.get("env") or {}).values():
            wanted.update(SECRET_REF_RE.findall(str(value)))
    if not wanted:
        return []
    from ..clients.secret import ListSecretsRequest

    res = service.secret.list_secrets(ListSecretsRequest())
    existing = {s.name for s in res.secrets} if res.ok else set()
    return sorted(wanted - existing)


def create_database_step(service: ServiceClient, step: Dict[str, Any]) -> Dict[str, Any]:
    existing = service.http.json("GET", "/api/v1/database/{ws}") or []
    if any(d.get("name") == step["app_name"] for d in existing):
        return {"status": "exists"}
    try:
        info = service.http.json(
            "POST",
            "/api/v1/database/{ws}",
            json={
                "kind": step["engine"],
                "name": step["app_name"],
                "always_on": step.get("always_on", False),
            },
            timeout=660,
        )
    except GatewayHTTPError as exc:
        raise TemplateError(f"{step['app_name']}: {exc.message}")
    return {
        "status": "created",
        "deployment_id": info.get("deployment_id"),
        "connection_string_secret": info.get("connection_string_secret"),
    }


def _checkout(step: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    """Return (working directory, temp dir to remove)."""
    source = step["source"]
    if not str(source).startswith(("http://", "https://", "git@")):
        sub = step.get("path") or "."
        path = os.path.abspath(
            source
            if sub == "." or os.path.abspath(sub) == os.path.abspath(source)
            else os.path.join(source, sub)
        )
        if not os.path.isdir(path):
            raise TemplateError(f"{step['app_name']}: {path} is not a directory")
        return path, None
    tmp = tempfile.mkdtemp(prefix="beta9-template-")
    cmd = ["git", "clone", "--depth", "1"]
    if step.get("branch"):
        cmd += ["--branch", step["branch"]]
    cmd += [source, os.path.join(tmp, "repo")]
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        shutil.rmtree(tmp, ignore_errors=True)
        detail = exc.stderr.strip() if isinstance(exc, subprocess.CalledProcessError) else str(exc)
        raise TemplateError(f"{step['app_name']}: clone failed: {detail}")
    return os.path.join(tmp, "repo", step.get("path", ".") or "."), tmp


def run_deploy_step(step: Dict[str, Any], context: str) -> Dict[str, Any]:
    cwd, tmp = _checkout(step) if step["kind"] == "repo" else (os.getcwd(), None)
    try:
        command = cli_command() + ["--json", "--context", context] + step["command"]
        env = {**os.environ, "BETA9_JSON": "1", "BETA9_NO_INPUT": "1"}
        proc = subprocess.run(
            command, cwd=cwd, env=env, capture_output=True, text=True, timeout=3600
        )
        result = parse_last_json(proc.stdout)
        if (
            proc.returncode != 0
            or not isinstance(result, dict)
            or result.get("status") == "failed"
            or "error" in result
        ):
            detail = (result or {}).get("error") if isinstance(result, dict) else None
            raise TemplateError(
                f"{step['app_name']}: deploy failed: {detail or proc.stderr.strip()[-800:] or proc.stdout.strip()[-800:]}"
            )
        return {
            "status": "deployed",
            "deployment_id": result.get("deployment_id"),
            "version": result.get("version"),
            "invoke_url": result.get("invoke_url"),
        }
    finally:
        if tmp:
            shutil.rmtree(tmp, ignore_errors=True)


def deploy_template(
    service: ServiceClient,
    manifest: Dict[str, Any],
    context: str,
    prefix: str,
    only: Optional[List[str]],
) -> List[Dict[str, Any]]:
    results = []
    for step in plan_steps(manifest, prefix):
        if only and step["service"] not in only:
            continue
        try:
            outcome = (
                create_database_step(service, step)
                if step["kind"] == "database"
                else run_deploy_step(step, context)
            )
        except TemplateError as exc:
            results.append({**step, "status": "failed", "error": exc.message})
            break  # later services depend on this one
        results.append({**step, **outcome})
    stack = f"{prefix}{manifest.get('name') or 'template'}".rstrip("-")
    group_into_stack(service, stack, [r["app_name"] for r in results if r["status"] != "failed"])
    return results


def group_into_stack(service: ServiceClient, name: str, app_names: List[str]) -> None:
    """Add the apps to the stack `name`, creating it if needed; membership only."""
    apps = (
        service.http.json("GET", "/api/v1/app/{ws}/latest", params={"limit": 200}).get("data") or []
    )
    ids = [a["id"] for a in apps if a["name"] in app_names]
    if not ids:
        return
    stacks = service.http.json("GET", "/api/v1/stack/{ws}") or []
    existing = next((s for s in stacks if s["name"] == name), None)
    if existing is None:
        service.http.json(
            "POST", "/api/v1/stack/{ws}", json={"name": name, "spec": {"appIds": ids}}
        )
        return
    spec = existing.get("spec") or {}
    spec["appIds"] = list(dict.fromkeys((spec.get("appIds") or []) + ids))
    service.http.json(
        "PUT", f"/api/v1/stack/{{ws}}/{existing['id']}", json={"name": name, "spec": spec}
    )


# --- commands ---------------------------------------------------------------------


@template.command(name="plan", help="Show the ordered steps a template would run.")
@click.argument("source")
@click.option("--prefix", default="", help="Prefix every service name (e.g. `pr-42-`).")
def plan(source: str, prefix: str):
    manifest = load_manifest(source)
    steps = plan_steps(manifest, prefix)
    if terminal.json_output():
        terminal.print_json({"name": manifest.get("name"), "steps": steps})
        return
    terminal.header(manifest.get("name") or source, manifest.get("description", ""))
    for i, step in enumerate(steps, 1):
        if step["kind"] == "database":
            terminal.print(f"{i}. {step['app_name']}: create {step['engine']} database")
        else:
            src = f" from {step['source']}" if step.get("source") else ""
            terminal.print(
                f"{i}. {step['app_name']}: {terminal.cli_name()} {' '.join(shlex.quote(a) for a in step['command'])}{src}"
            )


@template.command(name="deploy", help="Deploy every service in a template, in dependency order.")
@click.argument("source")
@click.option("--prefix", default="", help="Prefix every service name (e.g. `pr-42-`).")
@click.option("--only", multiple=True, help="Deploy only these services (repeatable).")
@click.option("--yes", "-y", is_flag=True, help="Skip the confirmation prompt.")
@extraclick.pass_service_client
def deploy(service: ServiceClient, source: str, prefix: str, only: List[str], yes: bool):
    manifest = load_manifest(source)
    steps = plan_steps(manifest, prefix)
    if missing := missing_secrets(service, manifest):
        cli = extraclick.command_hint()
        terminal.error(
            f"Create these secrets first: {', '.join(missing)}",
            hint=" · ".join(f"{cli} secret create {name} <value>" for name in missing),
            code="MISSING_SECRETS",
        )
    names = ", ".join(s["app_name"] for s in steps if not only or s["service"] in only)
    if not yes and not terminal.confirm(f"Deploy {len(steps)} service(s) ({names})?", default=True):
        terminal.error("Cancelled.", code="CANCELLED")
    results = deploy_template(
        service, manifest, extraclick.selected_context(), prefix, list(only) or None
    )
    if terminal.json_output():
        terminal.print_json({"name": manifest.get("name"), "results": results})
    else:
        for r in results:
            line = (
                f"{r['app_name']}: {r['status']}"
                + (f" ({r['error']})" if r.get("error") else "")
                + (f" {r['invoke_url']}" if r.get("invoke_url") else "")
            )
            if r["status"] == "failed":
                terminal.error(line, exit=False)
            else:
                terminal.success(line)
    if any(r["status"] == "failed" for r in results):
        sys.exit(1)


def _safe_name(n: Any) -> str:
    return re.sub(r"[^a-z0-9-]", "-", str(n).lower()).strip("-")[:32] or "service"


@template.command(name="import", help="Convert a docker-compose.yml into a template manifest.")
@click.argument("source", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    default=None,
    help="Write the manifest here instead of stdout.",
)
def import_manifest(source: str, output: Optional[str]):
    manifest = import_compose(source)
    validate_manifest(manifest)
    text = yaml.safe_dump(manifest, sort_keys=False)
    if output:
        with open(output, "w") as f:
            f.write(text)
        terminal.success(f"Wrote {output}")
    else:
        click.echo(text)


def import_compose(compose_file: str) -> Dict[str, Any]:
    with open(compose_file) as f:
        compose = yaml.safe_load(f) or {}
    services: Dict[str, Any] = {}
    compose_services = compose.get("services") or {}
    safe_name = _safe_name
    # Databases first; env mapping points at them.
    for name, svc in compose_services.items():
        base = str(svc.get("image") or "").split("/")[-1].split(":")[0]
        if base in DATABASE_IMAGES:
            services[safe_name(name)] = {"kind": "database", "engine": DATABASE_IMAGES[base]}
    for name, svc in compose_services.items():
        safe = safe_name(name)
        if safe in services:
            continue
        image = str(svc.get("image") or "")
        out: Dict[str, Any] = {}
        if svc.get("build"):
            build = svc["build"]
            out["kind"] = "repo"
            out["path"] = build if isinstance(build, str) else build.get("context", ".")
            if isinstance(build, dict) and build.get("dockerfile"):
                out["dockerfile"] = build["dockerfile"]
        else:
            out["kind"] = "image"
            out["image"] = image
        if svc.get("command"):
            out["command"] = (
                svc["command"]
                if isinstance(svc["command"], list)
                else shlex.split(str(svc["command"]))
            )
        ports = []
        for p in svc.get("ports") or []:
            container = str(p).split(":")[-1].split("/")[0]
            if container.isdigit():
                ports.append(int(container))
        if ports:
            out["ports"] = ports
        env = svc.get("environment") or {}
        if isinstance(env, list):
            env = dict(item.split("=", 1) if "=" in item else (item, "") for item in env)
        mapped = {}
        for key, value in env.items():
            value = str(value)
            for dep in svc.get("depends_on") or []:
                dep_safe = safe_name(dep)
                if (
                    dep_safe in services
                    and services[dep_safe]["kind"] == "database"
                    and (dep in value or re.search(r"(?i)(database_url|redis_url|db_url|dsn)", key))
                ):
                    field = (
                        "REDIS_URL" if services[dep_safe]["engine"] == "redis" else "DATABASE_URL"
                    )
                    value = f"${{{{db.{dep_safe}.{field}}}}}"
                    break
            mapped[key] = value
        if mapped:
            out["env"] = mapped
        mounts = [
            str(v).split(":")[1] if ":" in str(v) else str(v) for v in svc.get("volumes") or []
        ]
        if mounts:
            out["disks"] = {f"disk{i}" if i else "data": m for i, m in enumerate(mounts)}
        services[safe] = out
    return {
        "name": os.path.basename(os.path.dirname(os.path.abspath(compose_file)))
        or "compose-import",
        "description": f"Imported from {os.path.basename(compose_file)}",
        "services": services,
    }


@template.command(
    name="export", help="Save existing apps as a template (secret values are never included)."
)
@click.argument("apps", nargs=-1, required=True)
@click.option("--name", default="exported", show_default=True)
@click.option("--output", "-o", type=click.Path(), default=None)
@extraclick.pass_service_client
def export(service: ServiceClient, apps: List[str], name: str, output: Optional[str]):
    services: Dict[str, Any] = {}
    for app in apps:
        res = service.gateway.list_deployments(
            ListDeploymentsRequest(filters={"name": StringList([app])}, limit=50)
        )
        active = [d for d in res.deployments if d.active and d.name == app]
        if not active:
            raise TemplateError(f"No active deployment named {app!r}")
        latest = max(active, key=lambda d: d.version)
        stub = service.http.json("GET", f"/api/v1/stub/{{ws}}/{latest.stub_id}")
        services[app] = manifest_service(stub_config(stub))
    text = yaml.safe_dump({"name": name, "services": services}, sort_keys=False)
    if output:
        with open(output, "w") as f:
            f.write(text)
        terminal.success(f"Wrote {output}")
    else:
        click.echo(text)
