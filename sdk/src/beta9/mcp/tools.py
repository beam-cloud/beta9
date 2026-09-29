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
from ..config import DEFAULT_CONTEXT_NAME, get_settings

WAIT_DEFAULT = 20
WAIT_MAX = 55
LOG_TAIL = 40
GENERIC_FAILURE = "Deployment failed"

Handler = Callable[[Dict[str, Any]], Dict[str, Any]]


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


@dataclass
class DeployJob:
    id: str
    name: str
    directory: str
    command: List[str]
    started_at: float = field(default_factory=time.monotonic)
    lines: List[str] = field(default_factory=list)
    json_lines: Set[int] = field(default_factory=set)  # indices of the CLI's JSON output
    status: str = "running"  # running | deployed | failed
    result: Dict[str, Any] = field(default_factory=dict)
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
            self._finish("failed", error=str(exc))
            return

        payloads, self.json_lines = _json_objects(self.lines)
        errors = [p for p in payloads if p.get("error")]
        deployed = next((p for p in payloads if p.get("deployment_id")), None)
        if code == 0 and deployed and not errors:
            self._finish(
                "deployed",
                result={
                    "name": self.name,
                    "deployment_id": deployed.get("deployment_id"),
                    "stub_id": deployed.get("stub_id"),
                    "url": deployed.get("invoke_url") or deployed.get("url"),
                    "version": deployed.get("version"),
                },
            )
            return
        # The CLI reports the cause first and a generic "Deployment failed" last.
        reason = next((p for p in errors if p["error"] != GENERIC_FAILURE), None)
        reason = reason or (errors[-1] if errors else {})
        error = reason.get("error") or (self.view()["logs"] or [f"exit code {code}"])[-1]
        error = error.rstrip(":")
        details = [line for line in str(reason.get("details", "")).splitlines() if line.strip()]
        if details:  # a build log ends with the failing step
            error += f": {details[-1].strip()}"
        if reason.get("hint"):
            error += f" ({reason['hint']})"
        self._finish("failed", error=error)

    def _finish(
        self, status: str, result: Optional[Dict[str, Any]] = None, error: str = ""
    ) -> None:
        self.status, self.result, self.error = status, result or {}, error
        self.done.set()

    def view(self, cursor: int = 0) -> Dict[str, Any]:
        logs = [
            line
            for index, line in enumerate(self.lines[cursor:], start=cursor)
            if line.strip() and index not in self.json_lines
        ]
        view = {
            "job_id": self.id,
            "name": self.name,
            "status": self.status,
            "elapsed_seconds": round(time.monotonic() - self.started_at, 1),
            "log_cursor": len(self.lines),
            "logs": logs[-LOG_TAIL:],
            **self.result,
        }
        if self.error:
            view["error"] = self.error
        return view


class LocalTools:
    """Deploy tools appear once signed in; login tools whenever an auth server is configured."""

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
            context_name  # where login_status saves; the proxy reconnects with it
        )
        self.jobs: Dict[str, DeployJob] = {}
        self.login_flow: Optional[auth.DeviceLogin] = None
        self.login_available: bool = auth.login_configured()

    @property
    def handlers(self) -> Dict[str, Handler]:
        handlers: Dict[str, Handler] = {}
        if self.signed_in():
            handlers.update(deploy=self.deploy, deploy_status=self.deploy_status)
        if self.login_available:
            handlers.update(login=self.login, login_status=self.login_status)
        return handlers

    def has(self, name: str) -> bool:
        return name in self.handlers

    def call(self, name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        return self.handlers[name](arguments)

    def definitions(self) -> List[Dict[str, Any]]:
        cli = get_settings().name.lower()
        string, integer = {"type": "string"}, {"type": "integer"}
        strings = {"type": "array", "items": string}
        defs: List[Dict[str, Any]] = []
        if self.login_available:
            defs += [
                {
                    "name": "login",
                    "description": (
                        f"Start browser sign-in to {get_settings().name} (creates an account if needed). Show the "
                        "user the returned link, then call login_status until signed in. Never ask for a token."
                    ),
                    "inputSchema": {"type": "object", "properties": {}},
                    "annotations": {"title": "Sign in"},
                },
                {
                    "name": "login_status",
                    "description": "Whether the sign-in started by login is approved; enables the workspace tools when it is.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "wait_seconds": {**integer, "description": f"Block up to {WAIT_MAX}."}
                        },
                    },
                    "annotations": {"readOnlyHint": True},
                },
            ]
        if not self.signed_in():
            return defs
        return defs + [
            {
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
                        "name": {**string, "description": "App name: lowercase, dashes."},
                        "directory": {**string, "description": f"Default: {self.cwd}"},
                        "handler": {
                            **string,
                            "description": "file.py:object for a decorated function or Pod.",
                        },
                        "dockerfile": {**string, "description": "Path relative to directory."},
                        "image": {
                            **string,
                            "description": "Registry image to run instead of building.",
                        },
                        "entrypoint": strings,
                        "ports": {
                            "type": "array",
                            "items": integer,
                            "description": "Ports the server listens on; [] for a worker with no URL. Omit to use the Dockerfile's EXPOSE.",
                        },
                        "env": {"type": "object", "additionalProperties": string},
                        "secrets": {**strings, "description": "Workspace secret names to inject."},
                        "cpu": {"type": "number"},
                        "memory": {**string, "description": "e.g. 2Gi"},
                        "gpu": {**string, "description": "e.g. A10G; omit for CPU."},
                        "disks": {**strings, "description": "Durable disks NAME:/mount[:SIZE]."},
                        "keep_warm_seconds": {
                            **integer,
                            "description": "-1 always on; 0 scale to zero.",
                        },
                        "min_replicas": integer,
                        "max_replicas": integer,
                        "tcp": {
                            "type": "boolean",
                            "description": "Raw TCP (SSH, Postgres) instead of HTTP.",
                        },
                        "wait_seconds": {
                            **integer,
                            "description": f"Wait before returning (default {WAIT_DEFAULT}, max {WAIT_MAX}).",
                        },
                    },
                },
                "annotations": {"title": "Deploy this directory", "openWorldHint": True},
            },
            {
                "name": "deploy_status",
                "description": "Progress of a deploy job: status, log lines since log_cursor, and the URL once deployed.",
                "inputSchema": {
                    "type": "object",
                    "required": ["job_id"],
                    "properties": {
                        "job_id": string,
                        "log_cursor": {**integer, "description": "From the previous response."},
                        "wait_seconds": {
                            **integer,
                            "description": f"Block for a change, up to {WAIT_MAX}.",
                        },
                    },
                },
                "annotations": {"readOnlyHint": True},
            },
        ]

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
        flags = {
            "dockerfile": "--dockerfile",
            "image": "--image",
            "cpu": "--cpu",
            "memory": "--memory",
            "gpu": "--gpu",
            "keep_warm_seconds": "--keep-warm-seconds",
            "min_replicas": "--min-replicas",
            "max_replicas": "--max-replicas",
        }
        for key, flag in flags.items():
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
        return self._job_result(job, 0)

    def deploy_status(self, args: Dict[str, Any]) -> Dict[str, Any]:
        job = self.jobs.get(str(args.get("job_id") or ""))
        if job is None:
            return error_result("unknown job_id; jobs live for the lifetime of this MCP server")
        cursor = int(args.get("log_cursor") or 0)
        deadline = time.monotonic() + _clamp(args.get("wait_seconds"), 0)
        while job.status == "running" and len(job.lines) <= cursor and time.monotonic() < deadline:
            job.done.wait(0.5)
        return self._job_result(job, cursor)

    def _job_result(self, job: DeployJob, cursor: int) -> Dict[str, Any]:
        view = job.view(cursor)
        if job.status == "deployed":
            view.pop("logs")  # the build log is noise once the URL is known
            where = f" at {view['url']}" if view.get("url") else ""
            text = (
                f"Deployed {job.name}{where} (deployment {view.get('deployment_id')}). "
                "Wire it with connect_services or set_env; read its logs with `logs`."
            )
        elif job.status == "failed":
            text = f"Deploy of {job.name} failed: {job.error}"
        else:
            text = f"Deploying {job.name} (job {job.id}, {view['elapsed_seconds']}s). Poll deploy_status with log_cursor={view['log_cursor']}."
        if view.get("logs"):
            text += "\n\n" + "\n".join(view["logs"])
        result = text_result(text, **view)
        if job.status == "failed":
            result["isError"] = True
        return result

    def login(self, _args: Dict[str, Any]) -> Dict[str, Any]:
        try:
            flow = self.login_flow = auth.DeviceLogin.start()
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
