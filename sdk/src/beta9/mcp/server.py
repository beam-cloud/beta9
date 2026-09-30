"""
Stdio MCP server for agent clients. Workspace tools are forwarded to the
gateway's stateless `/api/v1/mcp` (one POST per message; notifications get a
bodyless 202); `deploy` and `login` run here. With no credentials the server
still starts and offers `login`, then announces the workspace tools once a
token arrives.
"""

import json
import sys
import threading
from dataclasses import replace
from typing import Any, BinaryIO, Dict, Iterable, List, Optional, Tuple

import requests

from ..channel import ServiceClient
from ..config import DEFAULT_CONTEXT_NAME, ConfigContext, context_defaults, get_settings
from .tools import LocalTools, error_result

PROTOCOL_VERSION = "2025-03-26"
REMOTE_TIMEOUT = 120

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
        self.session.headers.update(
            {
                "Authorization": f"Bearer {context.token or ''}",
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
                "MCP-Protocol-Version": PROTOCOL_VERSION,
            }
        )

    def call(self, message: Any) -> Tuple[int, Any]:
        response = self.session.post(self.url, data=json.dumps(message), timeout=REMOTE_TIMEOUT)
        if response.status_code == 202 or not response.content:
            return response.status_code, None
        try:
            return response.status_code, response.json()
        except ValueError:
            return response.status_code, None


class StdioProxy:
    def __init__(self, context_name: str = DEFAULT_CONTEXT_NAME, cwd: Optional[str] = None):
        self.context_name: str = context_name
        self.remote: Optional[RemoteMCP] = None
        self.tools: LocalTools = LocalTools(
            cwd=cwd,
            on_login=self._on_login,
            signed_in=lambda: self.remote is not None,
            context_name=context_name,
        )
        self._out_lock: threading.Lock = threading.Lock()
        self._stdout: BinaryIO = sys.stdout.buffer
        self._connect()

    def run(
        self, stdin: Optional[Iterable[bytes]] = None, stdout: Optional[BinaryIO] = None
    ) -> int:
        self._stdout = stdout or sys.stdout.buffer
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
            else:
                self._write(self._dispatch(message))
        return 0

    def notify(self, method: str) -> None:
        self._write({"jsonrpc": "2.0", "method": method})

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
                f"{remote} Local tools run on this machine: `deploy` ships a project directory with the "
                f"{cli} CLI (Dockerfile, image, or file:function handler) and returns a job; poll "
                "`deploy_status` until deployed, then wire it with connect_services or set_env."
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
            try:
                return _result(msg_id, handler(params.get("arguments") or {}))
            except Exception as exc:
                return _result(msg_id, error_result(f"{name} failed: {exc}"))
        if self.remote is None:
            how = (
                "Call `login` first."
                if self.tools.login_available
                else f"Run `{get_settings().name.lower()} config create` first."
            )
            return _result(msg_id, error_result(f"Not signed in; {name} needs a workspace. {how}"))
        return self._forward(
            {"jsonrpc": "2.0", "id": msg_id, "method": "tools/call", "params": params}
        )

    def _connect(self) -> None:
        context = context_or_none(self.context_name)
        if context is None:
            self.remote = None
            return
        try:
            self.remote = RemoteMCP(context)
        except Exception as exc:
            self.remote = None
            log(f"workspace unavailable: {exc}")

    def _on_login(self) -> None:
        self._connect()
        if self.remote is not None:
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
            self.remote = None
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
