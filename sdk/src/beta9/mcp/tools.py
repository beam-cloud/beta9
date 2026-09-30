"""
Tools that run on the agent's machine: `deploy` ships a project directory with
the CLI as a background job (builds can outlast a client's tool timeout), and
`login` runs the browser sign-in so an agent can onboard a user inside MCP.
"""

import base64
import fcntl
import hashlib
import http.client
import json
import os
import shlex
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from contextlib import nullcontext
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from urllib.parse import urlsplit, urlunsplit

from .. import auth
from ..config import DEFAULT_CONTEXT_NAME, cli_path, context_defaults, get_settings

WAIT_DEFAULT = 20
WAIT_MAX = 55
LOG_TAIL = 40
GENERIC_FAILURE = "Deployment failed"

Handler = Callable[[Dict[str, Any]], Dict[str, Any]]
Tool = Tuple[Dict[str, Any], Handler]  # definition, handler


def text_result(text: str, **structured: Any) -> Dict[str, Any]:
    result: Dict[str, Any] = {"content": [{"type": "text", "text": text}]}
    if structured:
        result["structuredContent"] = structured
    return result


def error_result(text: str) -> Dict[str, Any]:
    return {"content": [{"type": "text", "text": text}], "isError": True}


def _cli_command() -> List[str]:
    path = cli_path()
    return [path] if path else [sys.executable, "-m", "beta9"]


def _clamp(value: Any, default: int) -> int:
    try:
        seconds = int(value) if value is not None else default
    except (TypeError, ValueError):
        seconds = default
    return max(0, min(seconds, WAIT_MAX))


# --- definitions --------------------------------------------------------------------------

STRING = {"type": "string"}
INTEGER = {"type": "integer"}
STRINGS = {"type": "array", "items": STRING}

DATABASE_JOB_DEFINITION: Dict[str, Any] = {
    "name": "create_database_job",
    "description": (
        "Create a managed database in a recoverable local job. Poll deploy_status, then verify "
        "database readiness. Reuse request_key after an interruption to resume the same operation."
    ),
    "inputSchema": {
        "type": "object",
        "required": ["kind", "name", "request_key"],
        "properties": {
            "kind": {"type": "string", "enum": ["postgres", "redis", "mysql", "mongo"]},
            "name": STRING,
            "request_key": {**STRING, "description": "Stable idempotency key for this creation."},
            "always_on": {"type": "boolean"},
            "size": {**STRING, "description": "Disk capacity, e.g. 10Gi."},
            "cpu": {**INTEGER, "description": "CPU millicores; default 1000."},
            "memory": {**INTEGER, "description": "Memory MiB; default 512."},
            "pool": STRING,
            "snapshot_id": {**STRING, "description": "Restore into a new disk from this snapshot."},
            "restore_from": {
                **STRING,
                "description": "Postgres source with retained native backups.",
            },
            "restore_time": {
                **STRING,
                "description": "RFC3339 target within its verified recovery window.",
            },
            "username": {**STRING, "description": "Original Postgres role when restoring."},
            "database": {**STRING, "description": "Original Postgres database when restoring."},
        },
        "additionalProperties": False,
    },
}


def login_definition(product: str) -> Dict[str, Any]:
    return {
        "name": "login",
        "description": (
            f"Start browser sign-in to {product} (creates an account if needed). Show the user "
            "the returned link, then call login_status until signed in. Never ask for a token."
        ),
        "inputSchema": {"type": "object", "properties": {}},
        "annotations": {"title": "Sign in"},
    }


LOGIN_STATUS_DEFINITION: Dict[str, Any] = {
    "name": "login_status",
    "description": "Whether the sign-in started by login is approved; enables the workspace tools when it is.",
    "inputSchema": {
        "type": "object",
        "properties": {"wait_seconds": {**INTEGER, "description": f"Block up to {WAIT_MAX}."}},
    },
    "annotations": {"readOnlyHint": True},
}


def deploy_definition(cli: str, cwd: str) -> Dict[str, Any]:
    return {
        "name": "deploy",
        "description": (
            f"Deploy a project directory from this machine with the {cli} CLI; returns a job to poll "
            "with deploy_status. Give a Dockerfile (./Dockerfile is found automatically), an image, or "
            f"an entrypoint plus the port the server binds; or a {cli}-decorated object as handler "
            "'file.py:name'. Env values may reference apps and databases (${{app.NAME.URL}}, "
            "${{db.NAME.DATABASE_URL}}) and secrets (${{secret.NAME}})."
        ),
        "inputSchema": {
            "type": "object",
            "required": ["name"],
            "properties": {
                "name": {**STRING, "description": "App name: lowercase, dashes."},
                "directory": {**STRING, "description": f"Default: {cwd}"},
                "handler": {
                    **STRING,
                    "description": "file.py:object for a decorated function or Pod.",
                },
                "dockerfile": {**STRING, "description": "Path relative to directory."},
                "image": {**STRING, "description": "Registry image to run instead of building."},
                "entrypoint": STRINGS,
                "ports": {
                    "type": "array",
                    "items": INTEGER,
                    "description": "Ports the server listens on; [] for a worker with no URL. Omit to use the Dockerfile's EXPOSE.",
                },
                "env": {"type": "object", "additionalProperties": STRING},
                "secrets": {**STRINGS, "description": "Workspace secret names to inject."},
                "cpu": {
                    "type": "number",
                    "description": "CPU cores, e.g. 0.5 or 2; must be positive.",
                },
                "memory": {**STRING, "description": "e.g. 2Gi"},
                "gpu": {
                    **STRING,
                    "description": "Discover current choices with capabilities; omit for CPU.",
                },
                "gpu_count": INTEGER,
                "pool": STRING,
                "rollout": {
                    **STRING,
                    "enum": ["auto", "blue-green", "replace"],
                    "description": "auto retains prior revisions; replace stops them before starting the new revision and can cause downtime. Use replace for a single worker/scheduler version.",
                },
                "idempotency_key": {
                    **STRING,
                    "description": "Reuse to recover the same deploy after interruption.",
                },
                "disks": {**STRINGS, "description": "Durable disks NAME:/mount[:SIZE]."},
                "keep_warm_seconds": {**INTEGER, "description": "-1 always on; 0 scale to zero."},
                "min_replicas": INTEGER,
                "max_replicas": INTEGER,
                "tcp": {
                    "type": "boolean",
                    "description": "Raw TCP (SSH, Postgres) instead of HTTP.",
                },
                "wait_seconds": {
                    **INTEGER,
                    "description": f"Wait before returning (default {WAIT_DEFAULT}, max {WAIT_MAX}).",
                },
            },
        },
        "annotations": {"title": "Deploy this directory", "openWorldHint": True},
    }


DEPLOY_STATUS_DEFINITION: Dict[str, Any] = {
    "name": "deploy_status",
    "description": "Progress of a deploy job: status, log lines since log_cursor, and the URL once deployed.",
    "inputSchema": {
        "type": "object",
        "required": ["job_id"],
        "properties": {
            "job_id": STRING,
            "log_cursor": {**INTEGER, "description": "From the previous response."},
            "wait_seconds": {**INTEGER, "description": f"Block for a change, up to {WAIT_MAX}."},
        },
    },
    "annotations": {"readOnlyHint": True},
}

LIST_JOBS_DEFINITION: Dict[str, Any] = {
    "name": "list_deploy_jobs",
    "description": "Recover local deploy job IDs in the selected context.",
    "inputSchema": {"type": "object", "properties": {}},
    "annotations": {"readOnlyHint": True},
}

CANCEL_JOB_DEFINITION: Dict[str, Any] = {
    "name": "cancel_deploy",
    "description": "Cancel a local build; an already accepted deployment is retained for reconciliation.",
    "inputSchema": {
        "type": "object",
        "properties": {"job_id": STRING},
        "required": ["job_id"],
    },
}

HTTP_ARTIFACT_DEFINITION: Dict[str, Any] = {
    "name": "http_artifact",
    "description": (
        "Invoke an exact deployment and stream the complete HTTP response to a local file, "
        "including SSE and binary output. Optional upload_file sends raw bytes. "
        "With _meta.progressToken, progress messages carry JSON {offset, data_base64, path}. "
        "MCP cancellation closes the upstream request and retains the partial artifact. "
        "Does not follow redirects or overwrite files."
    ),
    "inputSchema": {
        "type": "object",
        "properties": {
            "name": STRING,
            "deployment_id": STRING,
            "method": STRING,
            "path": STRING,
            "upload_file": STRING,
            "output_file": STRING,
            "headers": {"type": "object", "additionalProperties": STRING},
            "timeout_seconds": INTEGER,
            "max_bytes": INTEGER,
        },
    },
}


def run_definition(cli: str, cwd: str) -> Dict[str, Any]:
    return {
        **deploy_definition(cli, cwd),
        "name": "run",
        "description": (
            "Build and submit a one-off container with the selected context. Poll deploy_status "
            "for task_id, then get_task for completion; acceptance is not successful execution."
        ),
    }


# One --flag per scalar argument of `deploy`.
DEPLOY_FLAGS = {
    "dockerfile": "--dockerfile",
    "image": "--image",
    "cpu": "--cpu",
    "memory": "--memory",
    "gpu": "--gpu",
    "gpu_count": "--gpu-count",
    "pool": "--pool",
    "rollout": "--rollout",
    "keep_warm_seconds": "--keep-warm-seconds",
    "min_replicas": "--min-replicas",
    "max_replicas": "--max-replicas",
}


# --- deploy jobs --------------------------------------------------------------------------


@dataclass
class DeployJob:
    """One `deploy --json` run in the background; `result` is what the agent sees."""

    id: str
    name: str
    directory: str
    command: List[str]
    started_at: float = field(default_factory=time.time)
    lines: List[str] = field(default_factory=list)  # everything the CLI printed
    json_lines: Set[int] = field(default_factory=set)  # indices of its JSON output
    status: str = "running"  # running | accepted | failed | cancelled | interrupted
    deployed: Dict[str, Any] = field(default_factory=dict)  # deployment_id, stub_id, url, version
    error: str = ""
    done: threading.Event = field(default_factory=threading.Event)

    state_path: Optional[Path] = None
    pid: int = 0
    context_name: str = DEFAULT_CONTEXT_NAME

    @property
    def log_path(self) -> Optional[Path]:
        return self.state_path.with_suffix(".log") if self.state_path else None

    def save(self) -> None:
        if self.state_path is None:
            return

        payload = {
            key: getattr(self, key)
            for key in (
                "id",
                "name",
                "directory",
                "command",
                "started_at",
                "status",
                "deployed",
                "error",
                "pid",
                "context_name",
            )
        }

        if not self.log_path.exists():
            with open(self.log_path, "x", opener=_private_file) as output:
                output.writelines(line + "\n" for line in self.lines)

        temporary = self.state_path.with_suffix(".tmp")
        with open(temporary, "w", opener=_private_file) as output:
            json.dump(payload, output)
            output.flush()
            os.fsync(output.fileno())

        temporary.replace(self.state_path)

    def refresh(self) -> None:
        if self.state_path is None or not self.state_path.exists():
            return

        payload = json.loads(self.state_path.read_text())
        for key, value in payload.items():
            setattr(self, key, value)
        if self.log_path.exists():
            # Ignore an append still in progress; the next poll sees that line.
            content = self.log_path.read_text()
            self.lines = content[: content.rfind("\n") + 1].splitlines()
        if self.status == "running" and self.pid:
            try:
                os.kill(self.pid, 0)
            except ProcessLookupError:
                self.status = "interrupted"
                self.error = "Deployment supervisor exited without a terminal result; reconcile deployments before retrying."
                self.save()
        if self.status != "running":
            self.done.set()

    @classmethod
    def load(cls, path: Path) -> "DeployJob":
        payload = json.loads(path.read_text())
        job = cls(**payload, state_path=path)
        job.refresh()
        return job

    def start(self) -> None:
        if self.state_path is None:
            threading.Thread(target=self._run, daemon=True).start()
            return

        self.save()
        # The supervisor owns the CLI pipe and terminal record independently of
        # the MCP client's lifetime. Job files are private to this context.
        subprocess.Popen(
            [sys.executable, "-m", "beta9.mcp.tools", str(self.state_path)],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )

    def _run(self) -> None:
        # Machine mode: errors are JSON objects and prompts fail instead of blocking.
        env = {**os.environ, "NO_COLOR": "1", "TERM": "dumb", "BETA9_JSON": "1"}
        self.pid = os.getpid()
        self.save()

        try:
            proc = subprocess.Popen(
                self.command,
                cwd=self.directory,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                start_new_session=True,
            )

            threading.Thread(target=self._watch_cancellation, args=(proc,), daemon=True).start()
            log = open(self.log_path, "a", opener=_private_file) if self.log_path else nullcontext()
            with log as output:
                for line in proc.stdout or []:
                    self.lines.append(line.rstrip("\n"))
                    if output:
                        output.write(line.rstrip("\n") + "\n")
                        output.flush()
                if output:
                    os.fsync(output.fileno())
            code = proc.wait()
        except Exception as exc:
            self.error = str(exc)
            self.status = "failed"
            self.save()
            self.done.set()
            return

        payloads, self.json_lines = _json_objects(self.lines)
        errors = [p for p in payloads if p.get("error")]
        deployed = next(
            (p for p in payloads if p.get("deployment_id") or p.get("container_id")), None
        )
        if deployed:
            self.deployed = {
                "deployment_id": deployed.get("deployment_id"),
                "stub_id": deployed.get("stub_id"),
                "url": deployed.get("invoke_url") or deployed.get("url"),
                "version": deployed.get("version"),
                "deployment": deployed,
                "readiness": "unverified",
                "container_id": deployed.get("container_id"),
                "task_id": deployed.get("task_id"),
            }

        if code == 0 and deployed and not errors:
            self.status = "accepted"
        else:
            self.error = self._failure(errors, code)
            self.status = "failed"

        if self.state_path and self.state_path.with_suffix(".cancel").exists():
            self.status = "cancelled"
            self.error = (
                "Build process cancelled; reconcile any accepted deployment before retrying."
            )
        self.save()
        self.done.set()

    def _watch_cancellation(self, process: subprocess.Popen) -> None:
        if self.state_path is None:
            return

        marker = self.state_path.with_suffix(".cancel")
        while process.poll() is None:
            if not marker.exists():
                time.sleep(0.2)
                continue

            try:
                os.killpg(process.pid, signal.SIGTERM)
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass  # The CLI exited between polling and signalling.
            return

    def _failure(self, errors: List[Dict[str, Any]], code: int) -> str:
        """The cause. The CLI prints it before a generic "Deployment failed"."""
        reason = next((e for e in errors if e["error"] != GENERIC_FAILURE), None)
        reason = reason or (errors[-1] if errors else {})
        text = (reason.get("error") or (self.logs() or [f"exit code {code}"])[-1]).rstrip(":")
        details = str(reason.get("details", "")).strip().splitlines()
        if details:  # a build log ends with the failing step
            text += f": {details[-1].strip()}"
        if reason.get("hint"):
            text += f" ({reason['hint']})"
        return text

    def logs(self, cursor: int = 0) -> List[str]:
        """Progress lines from `cursor` on, without the CLI's JSON."""
        _, json_lines = _json_objects(self.lines)
        lines = enumerate(self.lines[cursor : cursor + LOG_TAIL], start=cursor)
        return [line for i, line in lines if line.strip() and i not in json_lines]

    def result(self, cursor: int = 0) -> Dict[str, Any]:
        self.refresh()
        cursor = max(0, min(cursor, len(self.lines)))
        next_cursor = min(len(self.lines), cursor + LOG_TAIL)
        view: Dict[str, Any] = {
            "job_id": self.id,
            "context": self.context_name,
            "has_more_logs": next_cursor < len(self.lines),
            "logs": self.logs(cursor),
            "name": self.name,
            "status": self.status,
            "elapsed_seconds": round(time.time() - self.started_at, 1),
            "log_cursor": next_cursor,
            "log_file": str(self.log_path) if self.log_path else None,
            **self.deployed,
        }
        if self.status == "accepted":
            where = f" at {self.deployed['url']}" if self.deployed.get("url") else ""
            text = (
                f"Deployment accepted for {self.name}{where} (deployment {self.deployed['deployment_id']}). "
                "Readiness is not yet verified; use wait_deployment with an application health path."
            )
            return text_result(text, **view)

        if self.status in ("failed", "cancelled", "interrupted"):
            view["error"] = self.error
            text = f"Deploy of {self.name} failed: {self.error}"
        else:
            text = f"Deploying {self.name} (job {self.id}, {view['elapsed_seconds']}s). Poll deploy_status with log_cursor={view['log_cursor']}."
        if view["logs"]:
            text += "\n\n" + "\n".join(view["logs"])
        result = text_result(text, **view)
        if self.status in ("failed", "cancelled", "interrupted"):
            result["isError"] = True
        return result


def _private_file(path: str, flags: int) -> int:
    return os.open(path, flags, 0o600)


def _json_objects(lines: List[str]) -> Tuple[List[Dict[str, Any]], Set[int]]:
    """The (pretty-printed) JSON objects in CLI output, and the line indices they occupy."""
    objects: List[Dict[str, Any]] = []
    taken: Set[int] = set()
    start: Optional[int] = None
    for index, line in enumerate(lines):
        if start is None and not line.startswith("{"):
            continue
        start = index if start is None else start
        try:
            data = json.loads("\n".join(lines[start : index + 1]))
        except ValueError:
            continue
        if isinstance(data, dict):
            objects.append(data)
            taken.update(range(start, index + 1))
        start = None
    if start is not None:
        taken.update(range(start, len(lines)))
    return objects, taken


# --- the tools ----------------------------------------------------------------------------


class LocalTools:
    """Login tools whenever an auth server is configured; deploy tools once signed in."""

    def __init__(
        self,
        cwd: Optional[str],
        on_login: Callable[[], None],
        signed_in: Callable[[], bool],
        context_name: str = DEFAULT_CONTEXT_NAME,
    ):
        self.cwd: str = cwd or os.getcwd()
        self.on_login: Callable[[], None] = on_login
        self.signed_in: Callable[[], bool] = signed_in
        self.context_name: str = (
            context_name  # login_status saves here; the proxy reconnects with it
        )
        self.jobs: Dict[str, DeployJob] = {}
        self.request = threading.local()

        context = context_defaults(context_name)
        identity = hashlib.sha256(
            f"{context_name}:{context.gateway_host}:{context.token}".encode()
        ).hexdigest()[:24]
        self.job_dir = get_settings().config_path.parent / "mcp-jobs" / identity
        self.job_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.job_dir, 0o700)
        self.login_flow: Optional[auth.DeviceLogin] = None
        self.login_available: bool = auth.login_configured(context_name)

    def available(self) -> List[Tool]:
        """(definition, handler) for every tool offered right now."""
        settings = get_settings()
        tools: List[Tool] = []
        if self.login_available:
            tools += [
                (login_definition(settings.name), self.login),
                (LOGIN_STATUS_DEFINITION, self.login_status),
            ]

        if self.signed_in():
            tools += [
                (deploy_definition(settings.name.lower(), self.cwd), self.deploy),
                (DEPLOY_STATUS_DEFINITION, self.deploy_status),
                (run_definition(settings.name.lower(), self.cwd), self.run),
                (LIST_JOBS_DEFINITION, self.list_jobs),
                (CANCEL_JOB_DEFINITION, self.cancel_job),
                (HTTP_ARTIFACT_DEFINITION, self.http_artifact),
                (DATABASE_JOB_DEFINITION, self.create_database_job),
            ]
            from .stacks import definitions

            tools += definitions(self)

        return tools

    def definitions(self) -> List[Dict[str, Any]]:
        return [definition for definition, _ in self.available()]

    def handler(self, name: str) -> Optional[Handler]:
        return next((h for definition, h in self.available() if definition["name"] == name), None)

    def run(self, args: Dict[str, Any]) -> Dict[str, Any]:
        return self.deploy(args, operation="run")

    def deploy(self, args: Dict[str, Any], *, operation: str = "deploy") -> Dict[str, Any]:
        supported = deploy_definition("beam", self.cwd)["inputSchema"]["properties"]
        unknown = set(args) - set(supported)
        if unknown:
            return error_result("Unsupported deploy options: " + ", ".join(sorted(unknown)))

        name = str(args.get("name") or "").strip()
        if not name:
            return error_result("name is required")

        directory = os.path.abspath(os.path.expanduser(str(args.get("directory") or self.cwd)))
        if not os.path.isdir(directory):
            return error_result(f"directory not found: {directory}")
        if (
            operation == "run"
            and not any(args.get(key) for key in ("handler", "image", "dockerfile"))
            and Path(directory, "Dockerfile").is_file()
        ):
            args = {**args, "dockerfile": "Dockerfile"}

        command = _cli_command() + [operation, "--context", self.context_name, "--json"]
        command += ["--name", name]
        if operation == "run":
            command += ["--detach"]
            if args.get("rollout"):
                return error_result("rollout applies to deployments, not one-off jobs")
        if args.get("handler"):
            command.append(str(args["handler"]))
        for key, flag in DEPLOY_FLAGS.items():
            if args.get(key) not in (None, ""):
                command += [flag, str(args[key])]
        if args.get("entrypoint"):
            entry = args["entrypoint"]
            command += [
                "--entrypoint",
                entry if isinstance(entry, str) else shlex.join(map(str, entry)),
            ]
        if args.get("ports") == [] and operation == "deploy":
            command.append("--no-ports")  # a worker: no URL, even with EXPOSE in the Dockerfile
        for port in args.get("ports") or []:
            command += ["--port", str(int(port))]
        for key, value in (args.get("env") or {}).items():
            command += ["--env", f"{key}={value}"]
        if args.get("secrets"):
            command += ["--secrets", ",".join(map(str, args["secrets"]))]
        for disk in args.get("disks") or []:
            command += ["--disk", str(disk)]
        if args.get("tcp"):
            command.append("--tcp")

        return self.start_command(
            name, directory, command, args.get("idempotency_key"), args.get("wait_seconds")
        )

    def start_command(
        self,
        name: str,
        directory: str,
        command: List[str],
        key: Optional[str] = None,
        wait_seconds: Optional[int] = 0,
    ) -> Dict[str, Any]:
        key = str(key or uuid.uuid4().hex)
        job_id = hashlib.sha256(key.encode()).hexdigest()[:24]
        state_path = self.job_dir / f"{job_id}.json"
        with open(state_path.with_suffix(".lock"), "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if state_path.exists():
                job = DeployJob.load(state_path)
                if job.command != command or job.directory != directory:
                    return error_result(
                        "idempotency_key already belongs to a different deployment request"
                    )
            else:
                job = DeployJob(
                    id=job_id,
                    name=name,
                    directory=directory,
                    command=command,
                    state_path=state_path,
                    context_name=self.context_name,
                )
                job.start()

        self.jobs[job.id] = job
        deadline = time.monotonic() + _clamp(wait_seconds, WAIT_DEFAULT)
        while job.status == "running" and time.monotonic() < deadline:
            time.sleep(0.2)
            job.refresh()

        return job.result()

    def deploy_status(self, args: Dict[str, Any]) -> Dict[str, Any]:
        job_id = str(args.get("job_id") or "")
        if len(job_id) != 24 or any(c not in "0123456789abcdef" for c in job_id):
            return error_result("invalid job_id")
        job = self.jobs.get(job_id)
        if job is None:
            path = self.job_dir / f"{job_id}.json"
            if not path.exists():
                return error_result("unknown job_id in this context")
            job = DeployJob.load(path)
            self.jobs[job_id] = job

        job.refresh()
        cursor = int(args.get("log_cursor") or 0)
        deadline = time.monotonic() + _clamp(args.get("wait_seconds"), 0)
        while job.status == "running" and len(job.lines) <= cursor and time.monotonic() < deadline:
            time.sleep(0.2)
            job.refresh()

        return job.result(cursor)

    def remote(self, name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        from .server import RemoteMCP, context_or_none

        remote = RemoteMCP(context_or_none(self.context_name))
        _, body = remote.call(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": name, "arguments": arguments},
            }
        )
        result = body.get("result", {})
        if "error" in body or result.get("isError"):
            raise RuntimeError(json.dumps(result.get("structuredContent") or body.get("error")))

        return result.get("structuredContent", {})

    def database_job(self, arguments: Dict[str, Any], key: str) -> Dict[str, Any]:
        command = [
            sys.executable,
            "-m",
            "beta9.mcp.tools",
            "create-database",
            self.context_name,
            json.dumps(arguments),
        ]
        return self.start_command(arguments["name"], self.cwd, command, key)

    def create_database_job(self, args: Dict[str, Any]) -> Dict[str, Any]:
        arguments = dict(args)
        key = arguments.pop("request_key", None)
        if not key or not arguments.get("name") or not arguments.get("kind"):
            return error_result("kind, name, and request_key are required")
        return self.database_job(arguments, "database:" + str(key))

    def list_jobs(self, _args: Dict[str, Any]) -> Dict[str, Any]:
        paths = sorted(self.job_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        items = []
        for path in paths[:100]:
            job = DeployJob.load(path)
            items.append({"job_id": job.id, "name": job.name, "status": job.status, **job.deployed})

        return text_result("Local deployment jobs", items=items)

    def cancel_job(self, args: Dict[str, Any]) -> Dict[str, Any]:
        result = self.deploy_status({"job_id": args.get("job_id")})
        if "structuredContent" not in result:
            return result

        job = self.jobs[str(args["job_id"])]
        if job.status == "running":
            job.state_path.with_suffix(".cancel").touch(mode=0o600)

        return text_result(
            "Cancellation requested; use deploy_status to reconcile the outcome.",
            job_id=job.id,
            status=job.status,
        )

    def http_artifact(self, args: Dict[str, Any]) -> Dict[str, Any]:
        from .server import RemoteMCP, context_or_none

        context = context_or_none(self.context_name)
        if context is None:
            return error_result("Not signed in")

        remote = RemoteMCP(context)
        _, message = remote.call(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "get_deployment",
                    "arguments": {k: args[k] for k in ("name", "deployment_id") if k in args},
                },
            }
        )
        result = message.get("result", {})
        if result.get("isError"):
            return result

        deployment = result.get("structuredContent", {})
        if deployment.get("config", {}).get("tcp"):
            return error_result("Use a native TCP client for this deployment")
        address = deployment.get("url")
        if not address:
            return error_result("Deployment has no unambiguous HTTP port")

        target = urlsplit(address.rstrip("/") + "/" + str(args.get("path", "")).lstrip("/"))
        headers = dict(args.get("headers", {}))
        if deployment.get("config", {}).get("authorized") and not any(
            key.lower() == "authorization" for key in headers
        ):
            headers["Authorization"] = f"Bearer {context.token}"
        if target.hostname and target.hostname.endswith(".localhost"):
            headers["Host"] = target.netloc
            target = target._replace(netloc=f"localhost:{target.port or 80}")

        timeout = int(args.get("timeout_seconds", 55))
        maximum = int(args.get("max_bytes", 64 << 20))
        if not 1 <= timeout <= 110 or not 1 <= maximum <= 1 << 30:
            return error_result("timeout_seconds must be 1–110 and max_bytes 1–1073741824")

        upload = (
            open(Path(args["upload_file"]).expanduser(), "rb") if args.get("upload_file") else None
        )
        destination = (
            Path(args["output_file"]).expanduser().absolute()
            if args.get("output_file")
            else Path(tempfile.gettempdir()) / f"beam-response-{uuid.uuid4().hex}"
        )
        count = 0
        digest = hashlib.sha256()
        started = time.monotonic()
        cancelled = getattr(self.request, "cancelled", threading.Event())
        progress = getattr(self.request, "progress", None)
        finished = threading.Event()
        connection_type = (
            http.client.HTTPSConnection if target.scheme == "https" else http.client.HTTPConnection
        )
        connection = connection_type(target.hostname, target.port, timeout=min(10, timeout))

        try:
            connection.connect()
            transport = connection.sock
            transport.settimeout(timeout)

            def interrupt() -> None:
                while not finished.wait(0.1):
                    if cancelled.is_set() or time.monotonic() - started > timeout:
                        try:
                            transport.shutdown(socket.SHUT_RDWR)
                        except OSError:
                            pass
                        return

            threading.Thread(target=interrupt, daemon=True).start()
            if upload:
                headers.setdefault("Content-Length", str(os.fstat(upload.fileno()).st_size))
            route = urlunsplit(("", "", target.path or "/", target.query, ""))
            connection.request(str(args.get("method", "POST")).upper(), route, upload, headers)
            with connection.getresponse() as response:
                with open(
                    destination, "xb", opener=lambda path, flags: os.open(path, flags, 0o600)
                ) as output:
                    while chunk := response.read1(4096):
                        if cancelled.is_set():
                            raise ValueError("Request cancelled; file is incomplete")
                        count += len(chunk)
                        if count > maximum or time.monotonic() - started > timeout:
                            raise ValueError(
                                "Response exceeded the byte or time budget; file is incomplete"
                            )
                        output.write(chunk)
                        output.flush()
                        digest.update(chunk)
                        if progress:
                            progress(
                                count,
                                {
                                    "offset": count - len(chunk),
                                    "data_base64": base64.b64encode(chunk).decode(),
                                    "path": str(destination),
                                },
                            )

                if cancelled.is_set() or time.monotonic() - started > timeout:
                    raise ValueError("Request cancelled or timed out; file is incomplete")
                if response.length not in (None, 0):
                    raise ValueError("Upstream closed before Content-Length bytes arrived")

                value = text_result(
                    "HTTP response saved",
                    path=str(destination),
                    bytes=count,
                    sha256=digest.hexdigest(),
                    status=response.status,
                    headers=dict(response.headers),
                    raw_headers=response.getheaders(),
                    complete=True,
                )
                value["isError"] = response.status >= 400
                return value
        except Exception as exc:
            result = error_result(str(exc))
            result["structuredContent"] = {
                "path": str(destination),
                "complete": False,
                "bytes_received": count,
                "cancelled": cancelled.is_set(),
            }
            return result
        finally:
            finished.set()
            connection.close()
            if upload:
                upload.close()

    def login(self, _args: Dict[str, Any]) -> Dict[str, Any]:
        try:
            flow = self.login_flow = auth.DeviceLogin.start(context_defaults(self.context_name))
        except auth.LoginError as exc:
            return error_result(str(exc))
        opened = flow.open_browser()
        text = (
            ("A browser window was opened. " if opened else "")
            + f"Ask the user to sign in at {flow.verification_uri_complete} "
            + f"(or enter code {flow.user_code} at {flow.verification_uri}), then call login_status."
        )
        return text_result(
            text,
            verification_uri=flow.verification_uri,
            verification_uri_complete=flow.verification_uri_complete,
            user_code=flow.user_code,
            browser_opened=opened,
            expires_in_seconds=int(max(0, flow.expires_at - time.monotonic())),
        )

    def login_status(self, args: Dict[str, Any]) -> Dict[str, Any]:
        flow = self.login_flow
        if flow is None:
            if self.signed_in():
                return text_result(
                    "Signed in; the workspace tools are available.", status="signed_in"
                )
            return error_result("No sign-in in progress; call login first.")
        deadline = time.monotonic() + _clamp(args.get("wait_seconds"), 0)
        try:
            while True:
                context = flow.poll()
                if context is not None:
                    auth.save_login(context, name=self.context_name)
                    self.login_flow = None
                    self.on_login()
                    where = f" to {flow.workspace_name}" if flow.workspace_name else ""
                    return text_result(
                        f"Signed in{where}. The workspace tools are available now.",
                        status="signed_in",
                        workspace=flow.workspace_name,
                    )
                if time.monotonic() + flow.interval > deadline:
                    return text_result(
                        "Still waiting for the user to approve the sign-in.",
                        status="pending",
                        user_code=flow.user_code,
                        verification_uri_complete=flow.verification_uri_complete,
                    )
                time.sleep(flow.interval)
        except auth.LoginError as exc:
            self.login_flow = None
            return error_result(str(exc))


def main() -> None:
    if sys.argv[1] == "create-database":
        tools = LocalTools(
            cwd=None,
            on_login=lambda: None,
            signed_in=lambda: True,
            context_name=sys.argv[2],
        )
        result = tools.remote("create_database", json.loads(sys.argv[3]))
        print(json.dumps(result))
        return

    DeployJob.load(Path(sys.argv[1]))._run()


if __name__ == "__main__":
    main()
