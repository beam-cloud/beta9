"""Plan and apply stacks using existing MCP tools and stack.spec checkpoints."""

import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .tools import LocalTools, Tool, deploy_definition, text_result

SOURCE_MAX_FILES = 100_000
SOURCE_MAX_BYTES = 1 << 30
SOURCE_BLOCK_BYTES = 1 << 20
SOURCE_IGNORED_DIRS = {".git", "__pycache__", ".pytest_cache"}
APPLY_LEASE_SECONDS = 180
TASK_ACTIVE_STATUSES = {"pending", "running", "retry"}
SERVICE_FIELDS = {"type", "deploy", "depends_on", "health_path"}
JOB_FIELDS = {"job_id", "status", "deployment_id", "stub_id", "task_id", "log_cursor"}
DATABASE_OPTIONS = {
    "name",
    "kind",
    "size",
    "cpu",
    "memory",
    "pool",
    "always_on",
    "snapshot_id",
    "restore_from",
    "restore_time",
    "username",
    "database",
}
SERVICE_REFERENCE = re.compile(r"\$\{\{(?:db|app)\.([^.}]+)\.[^}]+\}\}")


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
    return [
        (
            {
                "name": "stack_plan",
                "description": (
                    "Validate a version-1 desired stack and return a reviewable dependency plan. "
                    "services maps application names to type (application/database/job), deploy "
                    "options, depends_on, and health_path. Source directories are fingerprinted. "
                    "Does not provision resources."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {"name": {"type": "string"}, "spec": {"type": "object"}},
                    "required": ["name", "spec"],
                },
            },
            lambda args: plan(tools, args),
        ),
        (
            {
                "name": "stack_apply",
                "description": (
                    "Advance a reviewed plan by one bounded step; repeat until complete. "
                    "Preserves successful steps, checks source changes, and checkpoints in "
                    "stack.spec. Jobs are never blindly rerun after an uncertain outcome."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {"plan_id": {"type": "string"}},
                    "required": ["plan_id"],
                },
            },
            lambda args: apply(tools, args),
        ),
        (
            {
                "name": "stack_status",
                "description": "Read the persisted desired state, checkpoint, and per-service results.",
                "inputSchema": {
                    "type": "object",
                    "properties": {"name": {"type": "string"}},
                    "required": ["name"],
                },
            },
            lambda args: status(tools, args),
        ),
        (
            {
                "name": "stack_resolve",
                "description": (
                    "Resolve a failed or uncertain migration after inspecting task logs and "
                    "database state. Record evidence, then either accept the verified migration "
                    "or explicitly permit one retry. Never use retry without checking its effects."
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


def status(tools: LocalTools, args: Dict[str, Any]) -> Dict[str, Any]:
    current = _find_stack(tools, args["name"])
    if current is None:
        raise ValueError("stack not found")

    return text_result("Stack state", **current)


def _prepare_services(
    tools: LocalTools, services: Dict[str, Any]
) -> Tuple[List[str], Dict[str, Any]]:
    order: List[str] = []
    visiting = set()
    sources: Dict[str, Any] = {}
    deploy_options = set(deploy_definition("beam", tools.cwd)["inputSchema"]["properties"])

    def visit(service: str) -> None:
        if service in order:
            return
        if service in visiting:
            raise ValueError(f"dependency cycle at {service}")
        if service not in services:
            raise ValueError(f"unknown dependency: {service}")

        visiting.add(service)
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
            dependencies.update(set(SERVICE_REFERENCE.findall(str(value))) & services.keys())
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
        elif not deploy.get("image"):
            sources[service] = source_state(deploy.get("directory") or tools.cwd)
            deploy["directory"] = sources[service]["directory"]

        if kind == "application" and deploy.get("ports") != [] and not node.get("health_path"):
            raise ValueError(f"application requires a safe health_path: {service}")

        visiting.remove(service)
        order.append(service)

    for service in services:
        visit(service)

    return order, sources


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
            raise ValueError(f"service name belongs to an app outside this stack: {service}")

        existing[service] = True
        previous = current.get("spec", {}).get("desired", {}).get("services", {})
        if previous.get(service) == node:
            reusable.append(service)

    return existing, reusable


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

    services = json.loads(json.dumps(spec["services"]))
    order, sources = _prepare_services(tools, services)
    current = _find_stack(tools, name)
    existing, reusable = _existing_services(tools, services, current)
    for source in sources.values():
        _snapshot_source(tools, source)

    planned = {
        "name": name,
        "desired": {"version": 1, "services": services},
        "order": order,
        "sources": sources,
        "base_revision": current["revision"] if current else None,
        "existing": existing,
        "reusable": reusable,
    }

    plan_id = digest(planned)
    folder = tools.job_dir / "plans"
    folder.mkdir(mode=0o700, exist_ok=True)
    with open(
        folder / f"{plan_id}.json", "w", opener=lambda path, flags: os.open(path, flags, 0o600)
    ) as output:
        json.dump(planned, output)

    return text_result(
        "Review this plan, then call stack_apply with plan_id. Removed services are retained; "
        "migrations require explicit job services.",
        plan_id=plan_id,
        **planned,
    )


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
        raise ValueError("another plan is applying")
    if planned["base_revision"] and current["revision"] != planned["base_revision"]:
        raise ValueError("stack changed; create a new plan")
    if not planned["base_revision"] and current.get("spec", {}).get("desired"):
        raise ValueError("stack was created by another plan; create a new plan")

    previous = state.get("services", {})
    reusable = {
        name: previous[name]
        for name in planned.get("reusable", [])
        if previous.get(name, {}).get("status") == "complete"
        and planned["desired"]["services"][name].get("type") == "database"
    }
    return {"plan_id": plan_id, "status": "applying", "services": reusable}


def _task_status(tools: LocalTools, task_id: str, step: Dict[str, Any]) -> None:
    task = tools.remote("get_task", {"task_id": task_id})
    status = str(task.get("status", "")).lower()
    if status == "complete":
        step["status"] = "complete"
    elif status not in TASK_ACTIVE_STATUSES:
        step["status"] = "failed"
        step["error"] = (
            f"Task {task_id} {status}: {task.get('failure_reason') or 'inspect task logs'}"
        )


def _database_check(service: str, kind: str, plan_id: str) -> Dict[str, Any]:
    if kind == "postgres":
        image = "postgres:16"
        command = 'exec psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT 1"'
        env = {"DATABASE_URL": "${{db." + service + ".DATABASE_URL}}"}
    else:
        image = "redis:7"
        command = """\
insecure=
case "$REDIS_URL" in
    *ssl_cert_reqs=none*) insecure=--insecure ;;
esac
test "$(redis-cli -u "$REDIS_URL" --sni "$REDIS_HOST" $insecure PING)" = PONG
"""
        env = {
            "REDIS_URL": "${{db." + service + ".REDIS_URL}}",
            "REDIS_HOST": "${{db." + service + ".HOST}}",
        }

    return {
        "name": service + "-check",
        "image": image,
        "entrypoint": ["sh", "-c", command],
        "env": env,
        "cpu": 0.1,
        "memory": "128Mi",
        "idempotency_key": f"stack:{plan_id}:{service}:health",
        "wait_seconds": 0,
    }


def _check_database(
    tools: LocalTools, service: str, kind: str, plan_id: str, step: Dict[str, Any]
) -> None:
    if not step.get("health_job_id"):
        result = tools.deploy(_database_check(service, kind, plan_id), operation="run")
        if result.get("isError"):
            raise ValueError(str(result))
        step["health_job_id"] = result["structuredContent"]["job_id"]

    result = tools.deploy_status({"job_id": step["health_job_id"]})
    if result.get("isError"):
        step["status"] = "failed"
        step["error"] = result
        return

    job = result["structuredContent"]
    if job["status"] == "accepted":
        _task_status(tools, job["task_id"], step)
        if step["status"] == "complete":
            step["readiness"] = "verified_connection"


def _check_application(tools: LocalTools, node: Dict[str, Any], step: Dict[str, Any]) -> None:
    if not node.get("health_path"):
        deployment = tools.remote("get_deployment", {"deployment_id": step["deployment_id"]})
        containers = tools.remote("api", {"path": "/api/v1/container/{ws}", "method": "GET"})
        running = [
            item["container_id"]
            for item in containers["body"]
            if item["stub_id"] == deployment["stub_id"] and item["status"] == "RUNNING"
        ]
        step["status"] = "complete" if running else "starting"
        step["health"] = {"check": "running_process", "containers": running}
        return

    try:
        health = tools.remote(
            "wait_deployment",
            {
                "deployment_id": step["deployment_id"],
                "path": node["health_path"],
                "wait_seconds": 5,
            },
        )
        step["status"] = "complete"
        step["health"] = health
        step.pop("last_health_error", None)
    except RuntimeError as exc:
        step["status"] = "starting"
        step["last_health_error"] = str(exc)


def _advance_service(
    tools: LocalTools, planned: Dict[str, Any], plan_id: str, service: str, step: Dict[str, Any]
) -> None:
    if step["status"] in ("failed", "uncertain"):
        raise ValueError(
            f"{service} requires reconciliation: {step.get('error', 'unknown outcome')}"
        )

    node = planned["desired"]["services"][service]
    kind = node.get("type", "application")
    if not step.get("job_id"):
        key = f"stack:{plan_id}:{service}"
        if step.get("attempt", 0):
            key += f":attempt:{step['attempt']}"
        if kind == "database":
            if planned["existing"].get(service):
                raise ValueError(
                    "database already exists; use explicit database operations for changes"
                )
            result = tools.database_job(node["deploy"], key)
        else:
            options = {**node["deploy"], "idempotency_key": key, "wait_seconds": 0}
            if service in planned["sources"]:
                source = planned["sources"][service]
                directory = tools.job_dir / "builds" / digest(key)
                # The CLI writes ignore/build files; keep the reviewed snapshot immutable.
                _copy_source(source["snapshot_directory"], directory, source["fingerprint"])
                options["directory"] = str(directory)
            result = tools.deploy(options, operation="run" if kind == "job" else "deploy")

    else:
        result = tools.deploy_status(
            {"job_id": step["job_id"], "wait_seconds": 0, "log_cursor": step.get("log_cursor", 0)}
        )

    job = result.get("structuredContent", {})
    # Logs and the CLI response live with the job, not in every stack checkpoint.
    step.update({key: value for key, value in job.items() if key in JOB_FIELDS})
    if result.get("isError"):
        step["status"] = "uncertain"
        step["error"] = result
        return

    if job["status"] == "running":
        return

    if kind == "job":
        step["status"] = "submitted"
        _task_status(tools, job["task_id"], step)
    elif kind == "database":
        _check_database(tools, service, node["deploy"]["kind"], plan_id, step)
    else:
        _check_application(tools, node, step)


def apply(tools: LocalTools, args: Dict[str, Any]) -> Dict[str, Any]:
    plan_id = str(args["plan_id"])
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
        return text_result("Stack already complete", **current)
    if state.get("lease_until", 0) > time.time():
        return text_result("Another apply call is progressing this stack", **current)

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
        for service in planned["order"]:
            step = state["services"].setdefault(service, {"status": "pending"})
            if step["status"] == "complete":
                continue

            _advance_service(tools, planned, plan_id, service, step)
            kind = planned["desired"]["services"][service].get("type", "application")
            if step["status"] == "complete" or (kind == "job" and step.get("task_id")):
                added.append(service)
            break

        if all(
            state["services"].get(name, {}).get("status") == "complete" for name in planned["order"]
        ):
            state["status"] = "complete"
    finally:
        state.pop("lease_until", None)
        state.pop("owner", None)
        current = tools.remote(
            "update_stack",
            {
                "name": planned["name"],
                "expected_revision": current["revision"],
                "add": added,
                "spec": {"operation": state},
            },
        )

    return text_result("Stack progress; call stack_apply again while applying", **current)


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
    if node.get("type") != "job" or step.get("status") not in ("failed", "uncertain"):
        raise ValueError("only failed or uncertain migration jobs need resolution")
    if resolution not in ("complete", "retry") or not evidence:
        raise ValueError("resolution and evidence from inspecting the database are required")
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
    current = tools.remote(
        "update_stack",
        {
            "name": planned["name"],
            "expected_revision": current["revision"],
            "spec": {"operation": state},
        },
    )
    return text_result("Migration resolution recorded; continue with stack_apply", **current)
