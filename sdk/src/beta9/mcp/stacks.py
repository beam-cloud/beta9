"""Plan and apply stacks using existing MCP tools and stack.spec checkpoints."""

import hashlib
import json
import math
import os
import re
import secrets
import shutil
import string
import subprocess
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .tools import (
    WAIT_DEFAULT,
    WAIT_MAX,
    LocalTools,
    RemoteToolError,
    Tool,
    _clamp,
    deploy_definition,
    home_or_above,
    text_result,
)

SOURCE_MAX_FILES = 100_000
SOURCE_MAX_BYTES = 1 << 30
SOURCE_BLOCK_BYTES = 1 << 20
SOURCE_IGNORED_DIRS = {".git", "__pycache__", ".pytest_cache"}
APPLY_LEASE_SECONDS = 180
APPLY_POLL_SECONDS = 3
TASK_ACTIVE_STATUSES = {"pending", "running", "retry"}
# Steps whose deploy job or migration task may still change the services.
IN_FLIGHT_STATUSES = {"running", "submitted"}
SERVICE_FIELDS = {"type", "deploy", "depends_on", "health_path", "health_port"}
JOB_FIELDS = {"job_id", "status", "deployment_id", "stub_id", "task_id", "log_cursor"}
STEP_VIEW_FIELDS = (
    "status",
    "deployment_id",
    "task_id",
    "job_id",
    "error",
    "kept",
    "attempt",
    "retired",
    "retire_error",
)
# Characters of a health check's answer, and log lines of a step in flight, worth showing.
HEALTH_TEXT_MAX = 300
PROGRESS_LINES = 8
# wait_deployment errors that no amount of waiting fixes.
READINESS_CONFIG_ERRORS = {"INVALID_ARGS", "UNSUPPORTED_PROTOCOL", "NO_HTTP_PORT"}
SECRET_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
SECRET_ALPHABET = string.ascii_letters + string.digits
DATABASE_OPTIONS = {
    "name",
    "kind",
    "size",
    "cpu",
    "memory",
    "pool",
    "always_on",
    "snapshot_id",
    "username",
    "database",
}
# MiB per unit, as applications' memory is read.
MEMORY_UNITS = {
    "": 1,
    "m": 1,
    "mb": 1,
    "mi": 1,
    "mib": 1,
    "g": 1000,
    "gb": 1000,
    "gi": 1024,
    "gib": 1024,
}
SERVICE_REFERENCE = re.compile(r"\$\{\{(?:db|app)\.([^.}]+)\.[^}]+\}\}")


class ReadinessConfigError(RuntimeError):
    """The health check is misconfigured, so waiting longer cannot pass it."""


def digest(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _check_source_path(path: Path) -> None:
    if path.is_symlink():
        raise ValueError(
            f"source symlink cannot be fingerprinted safely: {path}; "
            "use a self-contained build directory"
        )


def source_state(directory: str) -> Dict[str, Any]:
    root = Path(directory).expanduser().resolve()
    if not root.is_dir():
        raise ValueError(f"missing source directory: {root}")

    fingerprint = hashlib.sha256()
    file_count = 0
    total_bytes = 0

    for parent, directories, files in os.walk(root):
        directories[:] = sorted(set(directories) - SOURCE_IGNORED_DIRS)
        for name in directories:
            _check_source_path(Path(parent) / name)

        for name in sorted(files):
            path = Path(parent) / name
            _check_source_path(path)
            if not path.is_file():
                raise ValueError(f"source is not a regular file: {path}")

            content = hashlib.sha256()
            with path.open("rb") as source:
                for block in iter(lambda: source.read(SOURCE_BLOCK_BYTES), b""):
                    total_bytes += len(block)
                    if total_bytes > SOURCE_MAX_BYTES:
                        raise ValueError("source exceeds 1 GiB; narrow the build directory")
                    content.update(block)

            fingerprint.update(str(path.relative_to(root)).encode() + b"\0")
            fingerprint.update((path.stat().st_mode & 0o777).to_bytes(2, "big"))
            fingerprint.update(content.digest())
            file_count += 1
            if file_count > SOURCE_MAX_FILES:
                raise ValueError("source contains over 100000 files; narrow the build directory")

    revision = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True, capture_output=True, timeout=10
    )
    return {
        "directory": str(root),
        "fingerprint": fingerprint.hexdigest(),
        "revision": revision.stdout.strip() if revision.returncode == 0 else None,
    }


def _copy_source(directory: str, destination: Path, fingerprint: str) -> None:
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if destination.exists():
        return

    with tempfile.TemporaryDirectory(prefix=".copy-", dir=destination.parent) as temporary:
        copied = Path(temporary) / "source"
        shutil.copytree(
            directory,
            copied,
            symlinks=True,
            ignore=shutil.ignore_patterns(*SOURCE_IGNORED_DIRS),
        )
        if source_state(str(copied))["fingerprint"] != fingerprint:
            raise ValueError("source changed while copying; create a new plan")

        try:
            copied.rename(destination)
        except OSError:
            if not destination.is_dir():
                raise


def _snapshot_source(tools: LocalTools, source: Dict[str, Any]) -> None:
    destination = tools.job_dir / "sources" / source["fingerprint"]
    _copy_source(source["directory"], destination, source["fingerprint"])
    if source_state(str(destination))["fingerprint"] != source["fingerprint"]:
        raise ValueError("cached source changed; remove its snapshot and create a new plan")

    source["snapshot_directory"] = str(destination)


def definitions(tools: LocalTools) -> List[Tool]:
    from . import compose

    return [
        (compose.definition(), compose.handler(lambda: tools.job_dir / "compose")),
        (
            {
                "name": "stack_plan",
                "description": (
                    "Validate a desired multi-service stack and return a reviewable plan; "
                    "stack_apply provisions it. Provisions nothing itself. For a "
                    "docker-compose project, start from stack_from_compose. Service names are "
                    "workspace-wide app names. Services reach each other only through "
                    "references in env, never by service name or localhost: "
                    "${{db.NAME.DATABASE_URL}} (also REDIS_URL, HOST, PORT, USERNAME, PASSWORD, "
                    "DATABASE), ${{app.NAME.URL}} or ${{app.NAME.URL.<port>}} (public https URL "
                    "of a port), ${{app.NAME.TCP.<port>}} (host:443 of a port on the TCP "
                    "gateway; the client must use TLS with that host as SNI; "
                    "${{app.NAME.HOST.<port>}} and ${{app.NAME.PORT.<port>}} are its halves "
                    "for separate host and port settings), and "
                    "${{secret.NAME}}. Secret and credential references must be the whole "
                    "value. References add depends_on automatically. Applications run "
                    "continuously (min_replicas 1) unless min_replicas or keep_warm_seconds says "
                    "otherwise. Omitted ports come from the image's EXPOSE. Source directories "
                    "are fingerprinted. Replanning a stack keeps applications and jobs whose "
                    "spec and source are unchanged and that still run or have completed."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "Stack name."},
                        "redeploy": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": (
                                "Services to deploy or rerun even if unchanged, e.g. to pull an "
                                "image tag that moved."
                            ),
                        },
                        "spec": {
                            "type": "object",
                            "properties": {
                                "version": {"const": 1},
                                "secrets": {
                                    "type": "object",
                                    "description": (
                                        "Workspace secrets shared by services, created with a "
                                        "random value when missing and never shown: "
                                        '{"NAME": {"length": 32, "alphabet": "0123456789abcdef"}} '
                                        "(both optional; the default is 32 letters and digits). "
                                        "Reference as ${{secret.NAME}}."
                                    ),
                                    "additionalProperties": {"type": "object"},
                                },
                                "services": {
                                    "type": "object",
                                    "additionalProperties": {
                                        "type": "object",
                                        "properties": {
                                            "type": {
                                                "enum": ["application", "database", "job"],
                                                "description": (
                                                    "application (default) is a deployment, job "
                                                    "a one-off run such as a migration, database "
                                                    "a managed postgres or redis."
                                                ),
                                            },
                                            "deploy": {
                                                "type": "object",
                                                "description": (
                                                    "The deploy tool's options (name defaults to "
                                                    "the service key). ports: [] is a worker. "
                                                    "tcp: true only for a server with no HTTP "
                                                    "port. Databases take kind (postgres or "
                                                    "redis), size (e.g. 10Gi), always_on, and "
                                                    "cpu and memory in application units "
                                                    '(cpu: 0.5, memory: "2Gi").'
                                                ),
                                            },
                                            "depends_on": {
                                                "type": "array",
                                                "items": {"type": "string"},
                                            },
                                            "health_path": {
                                                "type": "string",
                                                "description": (
                                                    "Side-effect-free GET path answering 2xx "
                                                    "when ready. Without one, and for workers "
                                                    "and tcp apps, readiness only checks that "
                                                    "the container runs."
                                                ),
                                            },
                                            "health_port": {
                                                "type": "integer",
                                                "description": "Port serving health_path; defaults to the first port.",
                                            },
                                        },
                                    },
                                },
                            },
                            "required": ["version", "services"],
                        },
                    },
                    "required": ["name", "spec"],
                },
            },
            lambda args: plan(tools, args),
        ),
        (
            {
                "name": "stack_apply",
                "description": (
                    "Advance a reviewed plan service by service for up to wait_seconds; call "
                    "again until it reports the stack applied. Preserves successful steps, "
                    "checks source changes, and checkpoints in stack.spec. Jobs are never "
                    "blindly rerun after an uncertain outcome. A newer plan replaces one "
                    "stopped on a failed or unready service. Returns each service's status, "
                    "with recent logs for one still building or starting."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "plan_id": {"type": "string"},
                        "wait_seconds": {
                            "type": "integer",
                            "description": (
                                f"Keep stepping this long (default {WAIT_DEFAULT}, max "
                                f"{WAIT_MAX}); 0 takes one step."
                            ),
                        },
                    },
                    "required": ["plan_id"],
                },
            },
            lambda args: apply(tools, args),
        ),
        (
            {
                "name": "list_stacks",
                "description": (
                    "Stacks: named groups of apps shown together on the dashboard board, with "
                    "each one's revision (update_stack's expected_revision) and, for a stack "
                    "applied from a spec, the status of the apply and of each service. "
                    "stack_status shows one stack in full."
                ),
                "inputSchema": {"type": "object", "properties": {}},
            },
            lambda args: list_summaries(tools),
        ),
        (
            {
                "name": "stack_status",
                "description": (
                    "Each service's status, deployment or task, and error, with recent logs for "
                    "one still building or starting."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "spec": {
                            "type": "boolean",
                            "description": "Include the applied spec and the full checkpoint.",
                        },
                    },
                    "required": ["name"],
                },
            },
            lambda args: status(tools, args),
        ),
        (
            {
                "name": "stack_resolve",
                "description": (
                    "Resolve a failed or uncertain service after inspecting its logs and state "
                    "(for a migration, its task logs and the database). Record evidence, then "
                    "either accept the verified result or explicitly permit one retry of the "
                    "planned source. Never use retry without checking its effects."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "plan_id": {"type": "string"},
                        "service": {"type": "string"},
                        "resolution": {"type": "string", "enum": ["complete", "retry"]},
                        "evidence": {"type": "string", "minLength": 1},
                    },
                    "required": ["plan_id", "service", "resolution", "evidence"],
                    "additionalProperties": False,
                },
            },
            lambda args: resolve(tools, args),
        ),
    ]


def _find_stack(tools: LocalTools, name: str) -> Optional[Dict[str, Any]]:
    stacks = tools.remote("list_stacks", {}).get("items", [])
    matches = [stack for stack in stacks if stack["name"] == name]
    if len(matches) > 1:
        raise ValueError("multiple stacks have this name; resolve the duplicate before applying")

    return matches[0] if matches else None


def list_summaries(tools: LocalTools) -> Dict[str, Any]:
    """The workspace's stacks without their specs and checkpoints, which the gateway's
    list_stacks returns whole."""
    items = []
    for stack in tools.remote("list_stacks", {}).get("items", []):
        item = {key: stack[key] for key in ("name", "id", "revision", "apps") if key in stack}
        operation = stack.get("spec", {}).get("operation", {})
        if operation:
            item["status"] = operation.get("status")
            item["services"] = {
                name: step.get("status") for name, step in operation.get("services", {}).items()
            }
        items.append(item)
    return text_result(f"{len(items)} stacks", items=items)


def status(tools: LocalTools, args: Dict[str, Any]) -> Dict[str, Any]:
    current = _find_stack(tools, args["name"])
    if current is None:
        raise ValueError("stack not found")

    view = _view(tools, current, live=True, full=bool(args.get("spec")))
    return text_result("Stack state", **view)


def _view(
    tools: LocalTools, current: Dict[str, Any], live: bool = False, full: bool = False
) -> Dict[str, Any]:
    """The stack as an agent acts on it: each service's outcome and, when `live`, what
    a service still in flight is doing now."""
    operation = current.get("spec", {}).get("operation", {})
    services = {}
    for name, step in operation.get("services", {}).items():
        view = {key: step[key] for key in STEP_VIEW_FIELDS if step.get(key)}
        if isinstance(step.get("health"), dict) and step.get("status") != "complete":
            view["health"] = _compact_health(step["health"])
        if step.get("status") == "starting" and step.get("accepted_at"):
            view["starting_for_seconds"] = round(time.time() - step["accepted_at"])
        if live:
            view.update(_progress(tools, step))
        services[name] = view

    view = {key: current[key] for key in ("name", "id", "revision", "apps") if key in current}
    view.update(plan_id=operation.get("plan_id"), status=operation.get("status"), services=services)
    if full:
        view["spec"] = current.get("spec", {})
    return view


def _progress(tools: LocalTools, step: Dict[str, Any]) -> Dict[str, Any]:
    """The newest output of a step in flight: its build log, or the logs of its starting
    deployment or running task."""
    try:
        if step.get("status") == "running" and step.get("job_id"):
            job = tools.deploy_status({"job_id": step["job_id"], "wait_seconds": 0})
            lines = job.get("structuredContent", {}).get("logs", [])[-PROGRESS_LINES:]
            return {"build_logs": lines} if lines else {}
        target = {"starting": "deployment_id", "submitted": "task_id"}.get(step.get("status"))
        if target and step.get(target):
            listed = tools.remote("logs", {target: step[target], "tail": PROGRESS_LINES})
            lines = [_log_line(item) for item in listed.get("items", [])]
            return {"recent_logs": lines} if lines else {}
    except (RemoteToolError, RuntimeError, ValueError):
        pass
    return {}


def _log_line(item: Dict[str, Any]) -> str:
    message = str(item.get("message", "")).rstrip()
    if len(message) > HEALTH_TEXT_MAX:
        message = message[:HEALTH_TEXT_MAX] + "..."
    return f"{item['stream']}: {message}" if item.get("stream") else message


def _compact_health(health: Dict[str, Any]) -> Dict[str, Any]:
    """A readiness result small enough to checkpoint and show: the answer's status and
    the start of its body, not the whole response."""
    answer = health.get("health") if isinstance(health.get("health"), dict) else {}
    compact = {
        key: health[key]
        for key in ("ready", "check", "containers", "code", "status")
        if key in health
    }
    if "status" in answer:
        compact["status"] = answer["status"]
    for key, value in (
        ("error", health.get("error")),
        ("answer", answer.get("body") or answer.get("error") or health.get("answer")),
    ):
        if value:
            text = value if isinstance(value, str) else json.dumps(value)
            compact[key] = text if len(text) <= HEALTH_TEXT_MAX else text[:HEALTH_TEXT_MAX] + "..."
    return compact


def _prepare_services(
    tools: LocalTools, services: Dict[str, Any]
) -> Tuple[List[str], Dict[str, Any], List[str]]:
    from .compose import resolve_image

    order: List[str] = []
    visiting: List[str] = []
    sources: Dict[str, Any] = {}
    warnings: List[str] = []
    deploy_options = set(deploy_definition("beam", tools.cwd)["inputSchema"]["properties"])
    registry = tools.registry() if hasattr(tools, "registry") else None

    def visit(service: str) -> None:
        if service in order:
            return
        if service in visiting:
            cycle = " -> ".join(visiting[visiting.index(service) :] + [service])
            raise ValueError(
                f"dependency cycle: {cycle}. A reference needs its target deployed first, so "
                "drop one of them and add it with set_env after the stack is applied"
            )
        if service not in services:
            raise ValueError(f"unknown dependency: {service}")

        visiting.append(service)
        node = services[service]
        if not isinstance(node, dict) or set(node) - SERVICE_FIELDS:
            raise ValueError(f"service fields are type, deploy, depends_on, health_path: {service}")

        kind = node.get("type", "application")
        if kind not in ("application", "database", "job"):
            raise ValueError(f"unsupported service type: {service}")

        deploy = node.setdefault("deploy", {})
        allowed = DATABASE_OPTIONS if kind == "database" else deploy_options
        if not isinstance(deploy, dict) or set(deploy) - allowed:
            raise ValueError(f"unsupported deploy options for {service}")

        dependencies = set(node.get("depends_on", []))
        for value in deploy.get("env", {}).values():
            # The gateway resolves an app's references to itself while creating it.
            referenced = set(SERVICE_REFERENCE.findall(str(value))) - {service}
            dependencies.update(referenced & services.keys())
        node["depends_on"] = sorted(dependencies)
        for dependency in node["depends_on"]:
            visit(dependency)

        if "name" in deploy and deploy["name"] != service:
            raise ValueError("deploy.name must match service key")
        deploy["name"] = service

        if kind == "database":
            if deploy.get("kind") not in ("postgres", "redis"):
                raise ValueError(
                    "stack database kinds are postgres and redis; durability qualification is separate"
                )
            _database_resources(service, deploy)
        elif not deploy.get("image"):
            directory = deploy.get("directory") or tools.cwd
            place = home_or_above(directory)
            if place:
                raise ValueError(
                    f"{service}: set deploy.directory, the project to build: {directory} is {place}"
                )
            sources[service] = source_state(directory)
            deploy["directory"] = sources[service]["directory"]

        if kind != "database" and registry is not None:
            try:
                resolved, notes = resolve_image(
                    deploy,
                    deploy.get("directory") or tools.cwd,
                    registry,
                    infer_ports=kind == "application",
                )
            except Exception:  # Best effort: deploy's own defaults still apply.
                resolved, notes = deploy, []
            deploy.update(resolved)
            warnings.extend(f"{service}: {note}" for note in notes)

        if kind == "application":
            warning = _prepare_application(service, node, deploy)
            if warning:
                warnings.append(warning)

        visiting.remove(service)
        order.append(service)

    for service in services:
        visit(service)

    return order, sources, warnings


def _database_resources(service: str, deploy: Dict[str, Any]) -> None:
    """Databases take application units (cpu: 0.5, memory: "2Gi"). The gateway wants
    millicores and MiB, which a cpu of 100 or more and an integer memory already are."""
    if deploy.get("cpu") is not None:
        cpu = str(deploy["cpu"]).strip().lower()
        try:
            value = float(cpu[:-1]) / 1000 if cpu.endswith("m") else float(cpu)
        except ValueError:
            raise ValueError(f"{service}: cpu is in cores, e.g. 0.5 or 2") from None
        deploy["cpu"] = round(value if value >= 100 else value * 1000)
    if isinstance(deploy.get("memory"), str):
        match = re.fullmatch(r"(\d+(?:\.\d+)?)([a-z]*)", deploy["memory"].strip().lower())
        if not match or match.group(2) not in MEMORY_UNITS:
            raise ValueError(f"{service}: memory is like 512Mi or 2Gi")
        deploy["memory"] = math.ceil(float(match.group(1)) * MEMORY_UNITS[match.group(2)])


def _prepare_application(
    service: str, node: Dict[str, Any], deploy: Dict[str, Any]
) -> Optional[str]:
    """Validate an application's health check and defaults; returns a warning, if any."""
    # Compose services run continuously, and a dependency that scales to zero
    # stalls its callers on a cold start; scaling to zero is opt-in.
    if "min_replicas" not in deploy and "keep_warm_seconds" not in deploy:
        deploy["min_replicas"] = 1

    ports = deploy.get("ports")
    health_port = node.get("health_port")
    if deploy.get("tcp") or ports == []:
        if node.get("health_path") or health_port is not None:
            raise ValueError(
                f"{service}: health checks use HTTP, which a tcp or portless app does not serve; "
                "omit health_path to check its running container, or keep tcp false when one "
                "port speaks HTTP and reach the other ports with ${{app.NAME.TCP.<port>}}"
            )
        return None
    if not node.get("health_path"):
        return (
            f"{service}: no health_path, so readiness only checks that its container runs; "
            "set a side-effect-free GET path that answers 2xx once it serves"
        )
    if health_port is None and ports and len(ports) > 1:
        node["health_port"] = ports[0]
    elif health_port is not None and (
        not isinstance(health_port, int) or (ports and health_port not in ports)
    ):
        raise ValueError(f"{service}: health_port must be one of its ports {ports}")
    return None


def _prepare_secrets(spec: Dict[str, Any]) -> Dict[str, Any]:
    declared = spec.get("secrets") or {}
    if not isinstance(declared, dict):
        raise ValueError("secrets maps NAME to {length, alphabet}")
    prepared = {}
    for name, options in declared.items():
        options = {} if options is None else options
        if not SECRET_NAME.fullmatch(name) or not isinstance(options, dict):
            raise ValueError(
                f"secret {name!r}: names are env-safe and options are {{length, alphabet}}"
            )
        if set(options) - {"length", "alphabet"}:
            raise ValueError(f"secret {name}: options are length and alphabet")
        length = options.get("length", 32)
        alphabet = options.get("alphabet", SECRET_ALPHABET)
        if not isinstance(length, int) or not 8 <= length <= 512:
            raise ValueError(f"secret {name}: length must be an integer from 8 to 512")
        if not isinstance(alphabet, str) or len(set(alphabet)) < 2:
            raise ValueError(f"secret {name}: alphabet needs at least two distinct characters")
        prepared[name] = {"length": length, "alphabet": alphabet}
    return prepared


def _ensure_secrets(tools: LocalTools, declared: Dict[str, Any]) -> List[str]:
    """Create each missing stack secret with a random value; existing ones are kept."""
    if not declared:
        return []
    existing = {item["name"] for item in tools.remote("list_secrets", {}).get("items", [])}
    created = []
    for name, options in declared.items():
        if name in existing:
            continue
        value = "".join(secrets.choice(options["alphabet"]) for _ in range(options["length"]))
        tools.remote("create_secret", {"name": name, "value": value})
        created.append(name)
    return created


def _existing_services(
    tools: LocalTools, services: Dict[str, Any], current: Optional[Dict[str, Any]]
) -> Tuple[Dict[str, bool], List[str]]:
    existing = {}
    reusable = []

    for service, node in services.items():
        apps = tools.remote("list_apps", {"name": service}).get("items", [])
        if not any(app["name"] == service for app in apps):
            continue
        if not current or service not in current.get("apps", []):
            operation = (current or {}).get("spec", {}).get("operation", {})
            status = operation.get("services", {}).get(service, {}).get("status")
            if status in ("failed", "uncertain"):
                raise ValueError(
                    f"{service} exists but plan {operation['plan_id']} left it {status}; "
                    "call stack_resolve for it first"
                )
            raise ValueError(f"service name belongs to an app outside this stack: {service}")

        existing[service] = True
        previous = current.get("spec", {}).get("desired", {}).get("services", {})
        if node.get("type") == "database" and previous.get(service) == node:
            reusable.append(service)

    return existing, reusable


def _unchanged_services(
    tools: LocalTools,
    services: Dict[str, Any],
    sources: Dict[str, Any],
    current: Optional[Dict[str, Any]],
    redeploy: List[str],
) -> List[str]:
    """Applications and jobs a new plan can leave alone: the same spec and source as the
    step that completed them and, for an application, that revision still active."""
    if not current:
        return []
    previous = current.get("spec", {}).get("desired", {}).get("services", {})
    steps = current.get("spec", {}).get("operation", {}).get("services", {})
    kept = []
    for service, node in services.items():
        step = steps.get(service, {})
        kind = node.get("type", "application")
        if (
            kind == "database"
            or service in redeploy
            or previous.get(service) != node
            or step.get("status") != "complete"
            or (service in sources and step.get("fingerprint") != sources[service]["fingerprint"])
        ):
            continue
        if kind == "job":
            if step.get("task_id"):
                kept.append(service)
            continue
        if not step.get("deployment_id"):
            continue
        listed = tools.remote("list_deployments", {"name": service, "active": True, "limit": 100})
        if any(
            item.get("deployment_id") == step["deployment_id"] for item in listed.get("items", [])
        ):
            kept.append(service)
    return kept


def plan(tools: LocalTools, args: Dict[str, Any]) -> Dict[str, Any]:
    name = str(args["name"]).strip()
    spec = args["spec"]
    if (
        not name
        or spec.get("version") != 1
        or not isinstance(spec.get("services"), dict)
        or not spec["services"]
    ):
        raise ValueError("name and spec {version:1, services:{...}} are required")

    if set(spec) - {"version", "secrets", "services"}:
        raise ValueError("spec fields are version, secrets and services")
    redeploy = [str(service) for service in args.get("redeploy") or []]
    if set(redeploy) - set(spec["services"]):
        raise ValueError("redeploy names services of this spec")
    declared_secrets = _prepare_secrets(spec)
    services = json.loads(json.dumps(spec["services"]))
    order, sources, warnings = _prepare_services(tools, services)
    current = _find_stack(tools, name)
    existing, reusable = _existing_services(tools, services, current)
    kept = _unchanged_services(tools, services, sources, current, redeploy)
    for service in kept:
        sources.pop(service, None)
    for source in sources.values():
        _snapshot_source(tools, source)

    desired: Dict[str, Any] = {"version": 1, "services": services}
    if declared_secrets:
        desired["secrets"] = declared_secrets
    planned = {
        "name": name,
        "desired": desired,
        "order": order,
        "sources": sources,
        "base_revision": current["revision"] if current else None,
        "existing": existing,
        "reusable": reusable,
        "kept": kept,
    }

    plan_id = digest(planned)
    folder = tools.job_dir / "plans"
    folder.mkdir(mode=0o700, exist_ok=True)
    with open(
        folder / f"{plan_id}.json", "w", opener=lambda path, flags: os.open(path, flags, 0o600)
    ) as output:
        json.dump(planned, output)

    changing = [service for service in order if service not in kept and service not in reusable]
    message = "Review this plan, then call stack_apply with plan_id. " + (
        f"Applying deploys {', '.join(changing)}, in order" if changing else "Nothing changes"
    )
    if kept:
        message += f"; it keeps {', '.join(kept)}, unchanged (redeploy rebuilds any of them)"
    if reusable:
        message += f"; existing databases {', '.join(reusable)} are kept"
    message += (
        ". Changed jobs rerun, so migrations must be idempotent; removed services are retained."
    )
    return text_result(message, plan_id=plan_id, warnings=warnings, **planned)


def _load_plan(tools: LocalTools, plan_id: str) -> Dict[str, Any]:
    if not re.fullmatch(r"[0-9a-f]{64}", plan_id):
        raise ValueError("invalid plan_id")

    planned = json.loads((tools.job_dir / "plans" / f"{plan_id}.json").read_text())
    if digest(planned) != plan_id:
        raise ValueError("saved plan changed; create a new plan")

    for service, source in planned["sources"].items():
        snapshot = source.get("snapshot_directory")
        if not snapshot or source_state(snapshot)["fingerprint"] != source["fingerprint"]:
            raise ValueError(f"source snapshot changed; create a new plan for {service}")

    return planned


def _operation_state(
    current: Dict[str, Any], planned: Dict[str, Any], plan_id: str
) -> Dict[str, Any]:
    state = current.get("spec", {}).get("operation", {})
    if state.get("plan_id") == plan_id:
        return state
    if state.get("status") == "applying":
        # A plan stopped on a failed or unready service gives way to a newer
        # one, so a fix never waits on resolving the broken attempt.
        if state.get("lease_until", 0) > time.time():
            raise ValueError("another plan is applying")
        for name, step in state.get("services", {}).items():
            if step.get("status") in IN_FLIGHT_STATUSES:
                raise ValueError(
                    f"another plan is applying {name} ({step['status']}); call stack_apply "
                    f"with plan {state.get('plan_id')} until it settles, then apply this plan"
                )
    if planned["base_revision"] and current["revision"] != planned["base_revision"]:
        raise ValueError("stack changed; create a new plan")
    if not planned["base_revision"] and current.get("spec", {}).get("desired"):
        raise ValueError("stack was created by another plan; create a new plan")

    # An unchanged database keeps its progress, failures included: it is never
    # recreated, and an unresolved outcome still needs stack_resolve.
    previous = state.get("services", {})
    reusable = {
        name: previous[name]
        for name in planned.get("reusable", [])
        if name in previous and planned["desired"]["services"][name].get("type") == "database"
    }
    kept = {
        name: {**previous[name], "kept": True}
        for name in planned.get("kept", [])
        if name in previous
    }
    return {"plan_id": plan_id, "status": "applying", "services": {**reusable, **kept}}


@dataclass
class StackService:
    tools: LocalTools
    name: str
    node: Dict[str, Any]
    state: Dict[str, Any]

    @property
    def kind(self) -> str:
        return self.node.get("type", "application")

    def submit(self, planned: Dict[str, Any], key: str) -> Dict[str, Any]:
        if self.kind == "database":
            if planned["existing"].get(self.name):
                raise ValueError("database already exists; use explicit database operations")
            return self.tools.database_job(self.node["deploy"], key)

        options = {**self.node["deploy"], "idempotency_key": key, "wait_seconds": 0}
        if self.name in planned["sources"]:
            source = planned["sources"][self.name]
            directory = self.tools.job_dir / "builds" / digest(key)
            # The CLI writes build files; keep the reviewed snapshot immutable.
            _copy_source(source["snapshot_directory"], directory, source["fingerprint"])
            options["directory"] = str(directory)
            self.state["fingerprint"] = source["fingerprint"]

        return self.tools.deploy(options, operation="run" if self.kind == "job" else "deploy")

    def readiness(self) -> Dict[str, Any]:
        revision = {"deployment_id": self.state["deployment_id"]}
        if self.kind == "database":
            return self.tools.remote("database_readiness", {"name": self.name, **revision})
        if self.node.get("health_path"):
            check = {**revision, "path": self.node["health_path"], "wait_seconds": 5}
            if self.node.get("health_port"):
                check["port"] = self.node["health_port"]
            try:
                return {**self.tools.remote("wait_deployment", check), "ready": True}
            except RemoteToolError as exc:
                if exc.payload.get("code") in READINESS_CONFIG_ERRORS:
                    raise ReadinessConfigError(exc.payload.get("error") or str(exc)) from exc
                raise

        deployment = self.tools.remote("get_deployment", revision)
        containers = self.tools.remote("api", {"path": "/api/v1/container/{ws}", "method": "GET"})
        running = [
            item["container_id"]
            for item in containers["body"]
            if item["stub_id"] == deployment["stub_id"] and item["status"] == "RUNNING"
        ]
        return {"ready": bool(running), "check": "running_process", "containers": running}

    def check_task(self) -> None:
        task_id = self.state["task_id"]
        task = self.tools.remote("get_task", {"task_id": task_id})
        status = str(task["status"]).lower()
        if status == "complete":
            self.state["status"] = "complete"
        elif status in TASK_ACTIVE_STATUSES:
            self.state["status"] = "submitted"
        else:
            self.state.update(
                status="failed",
                error=f"Task {task_id} {status}: {task.get('failure_reason') or 'inspect task logs'}",
            )

    def advance(self, planned: Dict[str, Any], plan_id: str) -> None:
        state = self.state
        # Older plans used disposable readiness jobs. Recheck without recreating the database.
        if self.kind == "database" and state.pop("health_job_id", None):
            state["status"] = "starting"
            state.pop("error", None)
        if state["status"] in ("failed", "uncertain"):
            raise ValueError(
                f"{self.name} requires stack_resolve or a corrected plan: {state.get('error')}"
            )

        if state.get("job_id"):
            result = self.tools.deploy_status(
                {
                    "job_id": state["job_id"],
                    "wait_seconds": 0,
                    "log_cursor": state.get("log_cursor", 0),
                }
            )
        else:
            key = f"stack:{plan_id}:{self.name}"
            if state.get("attempt", 0):
                key += f":attempt:{state['attempt']}"
            result = self.submit(planned, key)

        job = result.get("structuredContent", {})
        state.update({key: value for key, value in job.items() if key in JOB_FIELDS})
        if result.get("isError"):
            error = job.get("error") or result["content"][0]["text"].split("\n\n", 1)[0]
            # A deploy or run that was never accepted left nothing a retry would repeat;
            # a database create may have happened even though its job failed.
            accepted = state.get("deployment_id") or state.get("task_id")
            failed = self.kind != "database" and not accepted
            state.update(status="failed" if failed else "uncertain", error=error)
            return
        if job["status"] == "running":
            return
        if self.kind == "job":
            self.check_task()
            return

        state.setdefault("accepted_at", time.time())
        try:
            health = self.readiness()
        except ReadinessConfigError as exc:
            state.update(status="failed", error=f"health check cannot pass: {exc}")
            return
        except RuntimeError as exc:
            payload = exc.payload if isinstance(exc, RemoteToolError) else {"error": str(exc)}
            health = {**payload, "ready": False}
        state.update(
            status="complete" if health["ready"] else "starting", health=_compact_health(health)
        )
        if self.kind == "database" and health["ready"]:
            state["readiness"] = "verified_connection"
        if self.kind == "application" and health["ready"]:
            self.retire_previous()

    def retire_previous(self) -> None:
        """Stop the app's older active revisions once the planned one is ready: a stack runs
        one revision per app, and an always-on revision left behind keeps running, and
        consuming its queues, until stopped."""
        try:
            listed = self.tools.remote(
                "list_deployments", {"name": self.name, "active": True, "limit": 100}
            )
            revisions = [item for item in listed.get("items", []) if item.get("name") == self.name]
            current = next(
                (r for r in revisions if r.get("deployment_id") == self.state["deployment_id"]),
                None,
            )
            if current is None:
                return
            for revision in revisions:
                if revision.get("active") and revision.get("version", 0) < current["version"]:
                    self.tools.remote(
                        "stop_deployment", {"deployment_id": revision["deployment_id"]}
                    )
                    self.state.setdefault("retired", []).append(revision["deployment_id"])
        except (RemoteToolError, RuntimeError) as exc:
            self.state["retire_error"] = f"older revisions may still run: {exc}"


def apply(tools: LocalTools, args: Dict[str, Any]) -> Dict[str, Any]:
    """Step the plan until it settles (applied, blocked on a service, or applied by
    another call) or wait_seconds pass."""
    plan_id = str(args["plan_id"])
    deadline = time.monotonic() + _clamp(args.get("wait_seconds"), WAIT_DEFAULT)
    request = getattr(tools, "request", None)
    cancelled = getattr(request, "cancelled", None) or threading.Event()
    progress = getattr(request, "progress", None)
    previous = None
    steps = 0
    while True:
        message, current, pending = _apply_step(tools, plan_id)
        steps += 1
        if pending is None or cancelled.is_set() or time.monotonic() >= deadline:
            return text_result(message, **_view(tools, current, live=pending is not None))
        if progress is not None:
            progress(steps, {"service": pending[0], "status": pending[1]})
        if pending == previous:
            cancelled.wait(max(0.0, min(APPLY_POLL_SECONDS, deadline - time.monotonic())))
        previous = pending


def _apply_step(
    tools: LocalTools, plan_id: str
) -> Tuple[str, Dict[str, Any], Optional[Tuple[str, str]]]:
    """One bounded step: its message, the stack, and the service and status still in
    flight, if any."""
    planned = _load_plan(tools, plan_id)
    current = _find_stack(tools, planned["name"])
    operation = (current or {}).get("spec", {}).get("operation", {})
    if operation.get("plan_id") != plan_id:
        for service, source in planned["sources"].items():
            actual = source_state(source["directory"])
            if any(actual[field] != source[field] for field in actual):
                raise ValueError(f"source changed; create a new plan for {service}")

    if current is None:
        if planned["base_revision"]:
            raise ValueError("stack was deleted; create a new plan")
        tools.remote("create_stack", {"name": planned["name"]})
        current = _find_stack(tools, planned["name"])
        if current is None:
            raise RuntimeError("created stack could not be read")

    state = _operation_state(current, planned, plan_id)
    if state.get("status") == "complete":
        return "Stack already complete", current, None
    if state.get("lease_until", 0) > time.time():
        return "Another apply call is progressing this stack", current, None

    state["lease_until"] = time.time() + APPLY_LEASE_SECONDS
    state["owner"] = uuid.uuid4().hex
    current = tools.remote(
        "update_stack",
        {
            "name": planned["name"],
            "expected_revision": current["revision"],
            "spec": {"desired": planned["desired"], "operation": state},
        },
    )

    added = []
    try:
        _ensure_secrets(tools, planned["desired"].get("secrets", {}))
        for service in planned["order"]:
            step = state["services"].setdefault(service, {"status": "pending"})
            if step["status"] == "complete":
                continue

            node = planned["desired"]["services"][service]
            resource = StackService(tools, service, node, step)
            resource.advance(planned, plan_id)
            # A service joins once it exists, ready or not, so a corrected
            # plan can redeploy it.
            if step.get("deployment_id") or step.get("task_id") or step["status"] == "complete":
                added.append(service)
            break

        if all(
            state["services"].get(name, {}).get("status") == "complete" for name in planned["order"]
        ):
            state["status"] = "complete"
    finally:
        state.pop("lease_until", None)
        state.pop("owner", None)
        current = _save_operation(tools, planned["name"], current, state, added)

    for name in planned["order"]:
        step = state["services"].get(name, {})
        status = step.get("status", "pending")
        error = f" ({step['error']})" if step.get("error") else ""
        if status == "failed":
            message = (
                f"{name} failed{error}; fix it and plan again, or inspect it and call stack_resolve"
            )
            return message, current, None
        if status == "uncertain":
            message = (
                f"{name} is uncertain{error}; inspect it, then call stack_resolve or apply a "
                "corrected plan"
            )
            return message, current, None
        if status != "complete":
            since = step.get("accepted_at")
            elapsed = f" ({round(time.time() - since)}s)" if status == "starting" and since else ""
            return f"{name} is {status}{elapsed}; call stack_apply again", current, (name, status)
    return "Stack applied", current, None


def resolve(tools: LocalTools, args: Dict[str, Any]) -> Dict[str, Any]:
    planned = _load_plan(tools, str(args["plan_id"]))
    current = _find_stack(tools, planned["name"])
    state = (current or {}).get("spec", {}).get("operation", {})
    service = args["service"]
    step = state.get("services", {}).get(service, {})
    node = planned["desired"]["services"].get(service, {})
    resolution = args["resolution"]
    evidence = str(args.get("evidence", "")).strip()

    if state.get("plan_id") != args["plan_id"] or state.get("lease_until", 0) > time.time():
        raise ValueError("plan is not current or an apply call is still running")
    if not node or step.get("status") not in ("failed", "uncertain"):
        raise ValueError("only failed or uncertain services need resolution")
    if resolution not in ("complete", "retry") or not evidence:
        raise ValueError("resolution and evidence from inspecting the service are required")
    if step.get("task_id"):
        task = tools.remote("get_task", {"task_id": step["task_id"]})
        if str(task.get("status", "")).lower() in TASK_ACTIVE_STATUSES:
            raise ValueError("migration task is still active; wait or cancel it first")

    history = list(step.get("resolutions", []))
    history.append(
        {
            "resolution": resolution,
            "evidence": evidence,
            "at": time.time(),
            "job_id": step.get("job_id"),
            "task_id": step.get("task_id"),
            "previous_status": step["status"],
        }
    )
    if resolution == "retry":
        step = {"status": "pending", "attempt": step.get("attempt", 0) + 1}
    else:
        step["status"] = "complete"
        step.pop("error", None)

    step["resolutions"] = history
    state["services"][service] = step
    added = [service] if resolution == "complete" else []
    current = _save_operation(tools, planned["name"], current, state, added)
    return text_result("Resolution recorded; continue with stack_apply", **_view(tools, current))


def _save_operation(
    tools: LocalTools,
    name: str,
    current: Dict[str, Any],
    state: Dict[str, Any],
    added: List[str],
) -> Dict[str, Any]:
    return tools.remote(
        "update_stack",
        {
            "name": name,
            "expected_revision": current["revision"],
            "add": added,
            "spec": {"operation": state},
        },
    )
