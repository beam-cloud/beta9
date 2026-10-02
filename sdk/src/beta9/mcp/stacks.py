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
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .tools import LocalTools, Tool, deploy_definition, text_result

SOURCE_MAX_FILES = 100_000
SOURCE_MAX_BYTES = 1 << 30
SOURCE_BLOCK_BYTES = 1 << 20
SOURCE_IGNORED_DIRS = {".git", "__pycache__", ".pytest_cache"}
APPLY_LEASE_SECONDS = 180
TASK_ACTIVE_STATUSES = {"pending", "running", "retry"}
# Steps whose deploy job or migration task may still change the services.
IN_FLIGHT_STATUSES = {"running", "submitted"}
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
                    "stack.spec. Jobs are never blindly rerun after an uncertain outcome. A "
                    "newer plan replaces one stopped on a failed or unready service."
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
        "Review this plan, then call stack_apply with plan_id. Applying redeploys every "
        "application and reruns every job, so migrations must be idempotent jobs; reusable "
        "databases are kept and removed services are retained.",
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
    return {"plan_id": plan_id, "status": "applying", "services": reusable}


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

        return self.tools.deploy(options, operation="run" if self.kind == "job" else "deploy")

    def readiness(self) -> Dict[str, Any]:
        revision = {"deployment_id": self.state["deployment_id"]}
        if self.kind == "database":
            return self.tools.remote("database_readiness", {"name": self.name, **revision})
        if self.node.get("health_path"):
            return {
                **self.tools.remote(
                    "wait_deployment",
                    {**revision, "path": self.node["health_path"], "wait_seconds": 5},
                ),
                "ready": True,
            }

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
            state.update(status="uncertain", error=result["content"][0]["text"])
            return
        if job["status"] == "running":
            return
        if self.kind == "job":
            self.check_task()
            return

        try:
            health = self.readiness()
        except RuntimeError as exc:
            health = {"ready": False, "error": str(exc)}
        state.update(status="complete" if health["ready"] else "starting", health=health)
        if self.kind == "database" and health["ready"]:
            state["readiness"] = "verified_connection"


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
        status = state["services"].get(name, {}).get("status", "pending")
        if status in ("failed", "uncertain"):
            return text_result(
                f"{name} is {status}; inspect it, then call stack_resolve or apply a corrected plan",
                **current,
            )
        if status != "complete":
            return text_result(f"{name} is {status}; call stack_apply again", **current)
    return text_result("Stack applied", **current)


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
    return text_result("Resolution recorded; continue with stack_apply", **current)


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
