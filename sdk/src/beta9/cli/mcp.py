"""
`mcp`: register the gateway-hosted MCP server (`/api/v1/mcp`) with agent clients.
The server runs in the control plane; the client connects over HTTP with the
workspace token.
"""

import json
import platform
from pathlib import Path
from typing import Any, Dict, Optional

import click

from .. import terminal
from ..config import get_config_context, get_settings
from . import extraclick
from .extraclick import ClickCommonGroup

CLIENTS = ("cursor", "claude-code", "claude-desktop", "codex", "windsurf")


def _server_name() -> str:
    return get_settings().name.lower()


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@common.group(name="mcp", help="Connect coding agents to the workspace MCP server.")
def mcp():
    pass


def _server(context: Optional[str]) -> Dict[str, str]:
    cfg = get_config_context(context or extraclick.selected_context())
    if not cfg.token:
        terminal.error(
            f"No token for this context; run `{terminal.cli_name()} configure` first.",
            code="NOT_AUTHENTICATED",
        )
    return {"url": f"{cfg.http_url}/api/v1/mcp", "authorization": f"Bearer {cfg.token}"}


def _entry(client: str, server: Dict[str, str]) -> Dict[str, Any]:
    headers = {"Authorization": server["authorization"]}
    if client == "claude-code":
        return {"type": "http", "url": server["url"], "headers": headers}
    if client == "claude-desktop":  # no remote transport with headers; bridge over stdio
        return {
            "command": "npx",
            "args": [
                "-y",
                "mcp-remote",
                server["url"],
                "--header",
                f"Authorization:{server['authorization']}",
            ],
        }
    if client == "windsurf":
        return {"serverUrl": server["url"], "headers": headers}
    return {"url": server["url"], "headers": headers}


def _client_path(client: str, project: bool) -> Path:
    home = Path.home()
    if client == "cursor":
        return Path.cwd() / ".cursor" / "mcp.json" if project else home / ".cursor" / "mcp.json"
    if client == "claude-code":
        return Path.cwd() / ".mcp.json" if project else home / ".claude.json"
    if client == "claude-desktop":
        if platform.system() == "Darwin":
            return (
                home / "Library" / "Application Support" / "Claude" / "claude_desktop_config.json"
            )
        return home / ".config" / "Claude" / "claude_desktop_config.json"
    if client == "codex":
        return home / ".codex" / "config.toml"
    return home / ".codeium" / "windsurf" / "mcp_config.json"


def _merge_json(path: Path, entry: Dict[str, Any]) -> None:
    data: Dict[str, Any] = {}
    if path.exists():
        try:
            data = json.loads(path.read_text() or "{}")
        except json.JSONDecodeError:
            terminal.error(f"{path} is not valid JSON; fix it or pass --print to install by hand.")
    data.setdefault("mcpServers", {})[_server_name()] = entry
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + "\n")


def _codex_block(server: Dict[str, str]) -> str:
    return (
        f'[mcp_servers.{_server_name()}]\nurl = "{server["url"]}"\n'
        f'http_headers = {{ Authorization = "{server["authorization"]}" }}\n'
    )


def _codex_toml(path: Path, server: Dict[str, str]) -> None:
    header = f"[mcp_servers.{_server_name()}]"
    existing = path.read_text() if path.exists() else ""
    if header in existing:
        terminal.detail(f"{path} already has a {header} block; leaving it unchanged.")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(existing.rstrip("\n") + "\n\n" + _codex_block(server))


@mcp.command(name="install", help="Register the workspace MCP server with an agent client.")
@click.option("--client", type=click.Choice(CLIENTS), required=True)
@click.option(
    "--project",
    is_flag=True,
    help="Write project-scoped config in the current directory when the client supports it.",
)
@click.option(
    "--print", "print_only", is_flag=True, help="Print the config snippet instead of writing it."
)
@extraclick.config_context_option
def install(client: str, project: bool, print_only: bool, context: Optional[str]):
    server = _server(context)
    entry = _entry(client, server)
    path = _client_path(client, project)
    if print_only:
        if client == "codex":
            click.echo(_codex_block(server), nl=False)
        else:
            terminal.print_json({"mcpServers": {_server_name(): entry}})
        return

    if client == "codex":
        _codex_toml(path, server)
    else:
        _merge_json(path, entry)

    if terminal.json_output():
        terminal.print_json({"client": client, "path": str(path), "url": server["url"]})
    else:
        terminal.success(f"Registered the {_server_name()} MCP server for {client}")
        terminal.detail(f"{path}")
        terminal.detail("Restart the client to pick it up.")
