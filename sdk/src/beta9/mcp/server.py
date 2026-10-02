"""
Stdio MCP server for agent clients. Workspace tools are forwarded to the
gateway's stateless `/api/v1/mcp` (one POST per message; notifications get a
bodyless 202); `deploy` and `login` run here. With no credentials the server
still starts and offers `login`, then announces the workspace tools once a
token arrives.
"""

import configparser
import json
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from typing import Any, BinaryIO, Dict, Iterable, List, Optional, Tuple

import requests

from ..channel import ServiceClient
from ..config import DEFAULT_CONTEXT_NAME, ConfigContext, context_defaults, get_settings
from .tools import LocalTools, error_result

PROTOCOL_VERSION = "2025-03-26"
REMOTE_TIMEOUT = 120
# Database creation can include the gateway's ten-minute image build.
TOOL_TIMEOUTS = {"create_database": 660}
READ_RETRIES = 3
RETRY_DELAY = 0.3

PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INTERNAL_ERROR = -32603


def log(message: str) -> None:
    sys.stderr.write(f"[{get_settings().name.lower()} mcp] {message}\n")
    sys.stderr.flush()


def context_or_none(name: str) -> Optional[ConfigContext]:
    """The saved context, or its defaults with the environment token; never prompts."""
    context = context_defaults(name)
    if context.token:
        return context
    token = get_settings().api_token
    return replace(context, token=token) if token else None


def _result(msg_id: Any, result: Any) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": msg_id, "result": result}


def _error(msg_id: Any, code: int, message: str) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": msg_id, "error": {"code": code, "message": message}}


def _sdk_version() -> str:
    try:
        from importlib.metadata import version

        return version("beta9")
    except Exception:
        return "0"


class RemoteMCP:
    """The gateway's MCP endpoint for one context; `call` posts one message."""

    def __init__(self, context: ConfigContext):
        client = ServiceClient(context)
        try:
            self.url: str = f"{client.http.base_url}/api/v1/mcp"
        finally:
            client.close()
        self.session: requests.Session = requests.Session()
        self.read_only_tools = set()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {context.token or ''}",
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
                "MCP-Protocol-Version": PROTOCOL_VERSION,
            }
        )

    def call(self, message: Any) -> Tuple[int, Any]:
        params = message.get("params", {}) if isinstance(message, dict) else {}
        name = params.get("name", "")
        timeout = TOOL_TIMEOUTS.get(name, REMOTE_TIMEOUT)
        attempts = READ_RETRIES if name in self.read_only_tools else 1

        for attempt in range(attempts):
            try:
                response = self.session.post(self.url, data=json.dumps(message), timeout=timeout)
                break
            except requests.ConnectionError:
                if attempt == attempts - 1:
                    raise
                time.sleep(RETRY_DELAY * (attempt + 1))
        if response.status_code == 202 or not response.content:
            return response.status_code, None
        try:
            body = response.json()
        except ValueError:
            return response.status_code, None

        # Trust the gateway's catalog, not tool-name prefixes. Unknown calls
        # are never retried: their side effects may already have happened.
        if (
            isinstance(message, dict)
            and message.get("method") == "tools/list"
            and isinstance(body, dict)
        ):
            self.read_only_tools = {
                tool["name"]
                for tool in body.get("result", {}).get("tools", [])
                if tool.get("annotations", {}).get("readOnlyHint") is True
            }
        return response.status_code, body


class StdioProxy:
    def __init__(self, context_name: str = DEFAULT_CONTEXT_NAME, cwd: Optional[str] = None):
        self.context_name: str = context_name
        self.context: Optional[ConfigContext] = None  # the sign-in `remote` uses
        self.remote: Optional[RemoteMCP] = None
        self.connection_error: Optional[str] = None
        self._connect_lock = threading.Lock()
        self.tools: LocalTools = LocalTools(
            cwd=cwd,
            on_login=self._refresh_sign_in,
            signed_in=lambda: self.remote is not None,
            context_name=context_name,
        )
        self._out_lock: threading.Lock = threading.Lock()
        self._request_lock = threading.Lock()
        self._requests: Dict[Any, threading.Event] = {}
        self._stdout: BinaryIO = sys.stdout.buffer
        self._follow_sign_in()

    def run(
        self, stdin: Optional[Iterable[bytes]] = None, stdout: Optional[BinaryIO] = None
    ) -> int:
        self._stdout = stdout or sys.stdout.buffer
        with ThreadPoolExecutor(max_workers=8, thread_name_prefix="mcp") as executor:
            for raw in stdin or sys.stdin.buffer:
                line = raw.strip()
                if not line:
                    continue

                try:
                    message = json.loads(line)
                except ValueError:
                    self._write(_error(None, PARSE_ERROR, "parse error"))
                    continue

                if isinstance(message, list):
                    responses = [r for r in map(self._dispatch, message) if r is not None]
                    self._write(responses or None)
                elif isinstance(message, dict) and message.get("method") == "tools/call":
                    with self._request_lock:
                        self._requests[message.get("id")] = threading.Event()
                    executor.submit(self._respond, message)
                else:
                    self._respond(message)
        return 0

    def _respond(self, message: Any) -> None:
        try:
            self._write(self._dispatch(message))
        finally:
            if isinstance(message, dict) and message.get("method") == "tools/call":
                with self._request_lock:
                    self._requests.pop(message.get("id"), None)

    def notify(self, method: str, params: Optional[Dict[str, Any]] = None) -> None:
        message = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            message["params"] = params
        self._write(message)

    def _write(self, message: Any) -> None:
        if message is None:
            return
        with self._out_lock:
            self._stdout.write((json.dumps(message, separators=(",", ":")) + "\n").encode())
            self._stdout.flush()

    def _dispatch(self, message: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(message, dict) or message.get("jsonrpc") != "2.0":
            return _error(None, INVALID_REQUEST, "invalid request")
        method, msg_id = message.get("method"), message.get("id")
        if method is None:
            return None  # a response to a server-initiated request; none are sent
        notification = msg_id is None

        if method == "notifications/cancelled":
            with self._request_lock:
                cancelled = self._requests.get(message.get("params", {}).get("requestId"))
                if cancelled is not None:
                    cancelled.set()
            return None
        self._refresh_sign_in()

        if method == "initialize":
            return self._initialize(msg_id)
        if method == "ping":
            return _result(msg_id, {})
        if method == "tools/list":
            return _result(msg_id, {"tools": self._tool_list()})
        if method == "tools/call":
            return self._tools_call(msg_id, message.get("params") or {})
        if self.remote is None:
            return (
                None
                if notification
                else _error(msg_id, METHOD_NOT_FOUND, f"{method}: not available before sign-in")
            )
        return self._forward(message)

    def _initialize(self, msg_id: Any) -> Dict[str, Any]:
        product = get_settings().name
        cli = product.lower()
        if self.remote is not None:
            params = {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": f"{cli}-mcp", "version": _sdk_version()},
            }
            _, body = self._remote_call(
                {"jsonrpc": "2.0", "id": msg_id, "method": "initialize", "params": params}
            )
            remote = (
                body.get("result", {}).get("instructions", "") if isinstance(body, dict) else ""
            )
            instructions = (
                f"{remote} Every tool here acts on context {self.context_name}, the workspace "
                f"`whoami` reports. CLI commands run with another `--context` act on a different "
                f"workspace; to switch these tools, run `{cli} mcp install --context NAME` and "
                "restart the client. "
                f"Local tools run on this machine: `deploy` ships a project directory with the "
                f"{cli} CLI (Dockerfile, image, or file:function handler) and returns a job; poll "
                "`deploy_status` until accepted, then verify readiness with wait_deployment "
                "and wire it with connect_services or set_env."
            )
        elif self.connection_error:
            instructions = (
                f"Context {self.context_name} is configured but its gateway is unavailable. "
                "Tool calls retry the connection; do not request a new sign-in for a transport failure."
            )
        elif self.tools.login_available:
            instructions = (
                f"Not signed in to {product}. Call `login`, show the user the link, then call "
                "`login_status` until signed in; the workspace tools appear after that."
            )
        else:
            instructions = f"Not signed in. Run `{cli} config create` in a terminal, then restart this MCP server."
        return _result(
            msg_id,
            {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {"tools": {"listChanged": True}},
                "serverInfo": {"name": cli, "version": _sdk_version()},
                "instructions": instructions.strip(),
            },
        )

    def _tool_list(self) -> List[Dict[str, Any]]:
        tools: List[Dict[str, Any]] = []
        if self.remote is not None:
            _, body = self._remote_call({"jsonrpc": "2.0", "id": "tools", "method": "tools/list"})
            if isinstance(body, dict):
                tools.extend(body.get("result", {}).get("tools", []))
        return tools + self.tools.definitions()

    def _tools_call(self, msg_id: Any, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        name = params.get("name", "")
        handler = self.tools.handler(name)
        if handler is not None:
            with self._request_lock:
                self.tools.request.cancelled = self._requests.get(msg_id, threading.Event())
            token = params.get("_meta", {}).get("progressToken")
            self.tools.request.progress = None
            if token is not None:
                self.tools.request.progress = lambda progress, detail: self.notify(
                    "notifications/progress",
                    {
                        "progressToken": token,
                        "progress": progress,
                        "message": json.dumps(detail),
                    },
                )
            try:
                return _result(msg_id, handler(params.get("arguments") or {}))
            except Exception as exc:
                return _result(msg_id, error_result(f"{name} failed: {exc}"))
            finally:
                self.tools.request.__dict__.clear()
        if self.remote is None:
            if self.connection_error:
                return _result(
                    msg_id,
                    error_result(
                        f"Context {self.context_name}: gateway unavailable: {self.connection_error}. "
                        "Retry when the gateway is reachable; sign-in is not required."
                    ),
                )
            how = (
                "Call `login` first."
                if self.tools.login_available
                else f"Run `{get_settings().name.lower()} config create` first."
            )
            return _result(msg_id, error_result(f"Not signed in; {name} needs a workspace. {how}"))
        return self._forward(
            {"jsonrpc": "2.0", "id": msg_id, "method": "tools/call", "params": params}
        )

    def _connect(self, context: Optional[ConfigContext]) -> None:
        self.connection_error = None
        self.context = context
        if context is None:
            self.remote = None
            return
        try:
            self.remote = RemoteMCP(context)
        except Exception as exc:
            self.remote = None
            self.connection_error = str(exc)
            log(f"workspace unavailable: {exc}")

    def _follow_sign_in(self) -> bool:
        """Act as the context's latest saved sign-in, whether this server, a
        terminal `login` or another agent saved it. True if the tools changed."""
        with self._connect_lock:
            context, signed_in = self.context, self.remote is not None
            try:
                current = context_or_none(self.context_name)
            except configparser.Error as exc:
                # Another program is writing it in place; keep the sign-in in use.
                log(f"config unreadable: {exc}")
                return False
            if current == context and (signed_in or not self.connection_error):
                return False
            self._connect(current)
            return current != context or (self.remote is not None) != signed_in

    def _refresh_sign_in(self) -> None:
        if self._follow_sign_in():
            self.notify("notifications/tools/list_changed")

    def _remote_call(self, message: Any) -> Tuple[int, Any]:
        remote = self.remote
        if remote is None:
            return 0, {"error": {"code": INTERNAL_ERROR, "message": "not signed in"}}
        try:
            status, body = remote.call(message)
        except requests.RequestException as exc:
            return 0, {"error": {"code": INTERNAL_ERROR, "message": f"gateway unreachable: {exc}"}}
        if status == 401:
            # A sign-in saved during the call has already replaced `remote`.
            with self._connect_lock:
                rejected = self.remote is remote
                if rejected:
                    self.remote = None
            if rejected:
                self.notify("notifications/tools/list_changed")
            return status, {
                "error": {"code": INTERNAL_ERROR, "message": "token rejected; sign in again"}
            }
        return status, body

    def _forward(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        status, body = self._remote_call(message)
        msg_id = message.get("id")
        if msg_id is None:
            return None
        if isinstance(body, dict) and "jsonrpc" in body:
            return body
        if isinstance(body, dict) and "error" in body:
            err = body["error"]
            if message.get("method") == "tools/call":
                return _result(msg_id, error_result(err.get("message", "gateway error")))
            return _error(
                msg_id, err.get("code", INTERNAL_ERROR), err.get("message", "gateway error")
            )
        return _error(msg_id, INTERNAL_ERROR, f"unexpected gateway response (HTTP {status})")


def run_stdio(context_name: str = DEFAULT_CONTEXT_NAME, cwd: Optional[str] = None) -> int:
    return StdioProxy(context_name=context_name, cwd=cwd).run()
