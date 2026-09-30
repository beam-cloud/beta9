"""
Tools that run on the agent's machine: `deploy` ships a project directory with
the CLI as a background job (builds can outlast a client's tool timeout), and
`login` runs the browser sign-in so an agent can onboard a user inside MCP.
"""

import json
import os
import shlex
import shutil
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from .. import auth
from ..config import DEFAULT_CONTEXT_NAME, context_defaults, get_settings

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
    path = shutil.which(get_settings().name.lower())
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
                "cpu": {"type": "number"},
                "memory": {**STRING, "description": "e.g. 2Gi"},
                "gpu": {**STRING, "description": "e.g. A10G; omit for CPU."},
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

# One --flag per scalar argument of `deploy`.
DEPLOY_FLAGS = {
    "dockerfile": "--dockerfile",
    "image": "--image",
    "cpu": "--cpu",
    "memory": "--memory",
    "gpu": "--gpu",
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
    started_at: float = field(default_factory=time.monotonic)
    lines: List[str] = field(default_factory=list)  # everything the CLI printed
    json_lines: Set[int] = field(default_factory=set)  # indices of its JSON output
    status: str = "running"  # running | deployed | failed
    deployed: Dict[str, Any] = field(default_factory=dict)  # deployment_id, stub_id, url, version
    error: str = ""
    done: threading.Event = field(default_factory=threading.Event)

    def start(self) -> None:
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self) -> None:
        # Machine mode: errors are JSON objects and prompts fail instead of blocking.
        env = {**os.environ, "NO_COLOR": "1", "TERM": "dumb", "BETA9_JSON": "1"}
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
            )
            for line in proc.stdout or []:
                self.lines.append(line.rstrip("\n"))
            code = proc.wait()
        except Exception as exc:
            self.error, self.status = str(exc), "failed"
            self.done.set()
            return

        payloads, self.json_lines = _json_objects(self.lines)
        errors = [p for p in payloads if p.get("error")]
        deployed = next((p for p in payloads if p.get("deployment_id")), None)
        if code == 0 and deployed and not errors:
            self.deployed = {
                "deployment_id": deployed.get("deployment_id"),
                "stub_id": deployed.get("stub_id"),
                "url": deployed.get("invoke_url") or deployed.get("url"),
                "version": deployed.get("version"),
            }
            self.status = "deployed"
        else:
            self.error, self.status = self._failure(errors, code), "failed"
        self.done.set()

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
        lines = enumerate(self.lines[cursor:], start=cursor)
        return [line for i, line in lines if line.strip() and i not in self.json_lines][-LOG_TAIL:]

    def result(self, cursor: int = 0) -> Dict[str, Any]:
        view: Dict[str, Any] = {
            "job_id": self.id,
            "name": self.name,
            "status": self.status,
            "elapsed_seconds": round(time.monotonic() - self.started_at, 1),
            "log_cursor": len(self.lines),
            **self.deployed,
        }
        if self.status == "deployed":  # the build log is noise once the URL is known
            where = f" at {self.deployed['url']}" if self.deployed.get("url") else ""
            text = (
                f"Deployed {self.name}{where} (deployment {self.deployed['deployment_id']}). "
                "Wire it with connect_services or set_env; read its logs with `logs`."
            )
            return text_result(text, **view)

        view["logs"] = self.logs(cursor)
        if self.status == "failed":
            view["error"] = self.error
            text = f"Deploy of {self.name} failed: {self.error}"
        else:
            text = f"Deploying {self.name} (job {self.id}, {view['elapsed_seconds']}s). Poll deploy_status with log_cursor={view['log_cursor']}."
        if view["logs"]:
            text += "\n\n" + "\n".join(view["logs"])
        result = text_result(text, **view)
        if self.status == "failed":
            result["isError"] = True
        return result


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
            ]
        return tools

    def definitions(self) -> List[Dict[str, Any]]:
        return [definition for definition, _ in self.available()]

    def handler(self, name: str) -> Optional[Handler]:
        return next((h for definition, h in self.available() if definition["name"] == name), None)

    def deploy(self, args: Dict[str, Any]) -> Dict[str, Any]:
        name = str(args.get("name") or "").strip()
        if not name:
            return error_result("name is required")
        directory = os.path.abspath(os.path.expanduser(str(args.get("directory") or self.cwd)))
        if not os.path.isdir(directory):
            return error_result(f"directory not found: {directory}")

        command = _cli_command() + ["deploy", "--json", "--name", name]
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
        if args.get("ports") == []:
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

        job = DeployJob(id=uuid.uuid4().hex[:8], name=name, directory=directory, command=command)
        self.jobs[job.id] = job
        job.start()
        job.done.wait(_clamp(args.get("wait_seconds"), WAIT_DEFAULT))
        return job.result()

    def deploy_status(self, args: Dict[str, Any]) -> Dict[str, Any]:
        job = self.jobs.get(str(args.get("job_id") or ""))
        if job is None:
            return error_result("unknown job_id; jobs live for the lifetime of this MCP server")
        cursor = int(args.get("log_cursor") or 0)
        deadline = time.monotonic() + _clamp(args.get("wait_seconds"), 0)
        while job.status == "running" and len(job.lines) <= cursor and time.monotonic() < deadline:
            job.done.wait(0.5)
        return job.result(cursor)

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
