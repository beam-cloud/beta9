"""
`mcp`: the workspace MCP server for coding agents. Bare `mcp` serves stdio for
the client that launched it; `mcp install` registers that command with the
agents on this machine. The written config holds no token, so it works before
sign-in and survives token rotation.
"""

import json
import os
import platform
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import click

from .. import terminal
from ..config import DEFAULT_CONTEXT_NAME, get_settings
from . import extraclick
from .extraclick import ClickCommonGroup


@dataclass(frozen=True)
class AgentClient:
    id: str
    label: str
    markers: Sequence[str]  # paths (or a binary) whose presence means "installed"
    config: str
    project_config: Optional[str] = None
    skills_dir: Optional[str] = None
    binary: Optional[str] = None

    def installed(self) -> bool:
        if self.binary and shutil.which(self.binary):
            return True
        return any(Path(m).expanduser().exists() for m in self.markers)

    def config_path(self, project: bool = False) -> Path:
        if project and self.project_config:
            return Path.cwd() / self.project_config
        return Path(self.config).expanduser()


_CLAUDE_DESKTOP = (
    "~/Library/Application Support/Claude/claude_desktop_config.json"
    if platform.system() == "Darwin"
    else "~/.config/Claude/claude_desktop_config.json"
)

CLIENTS: Dict[str, AgentClient] = {
    c.id: c
    for c in [
        AgentClient(
            id="cursor",
            label="Cursor",
            markers=["~/.cursor"],
            config="~/.cursor/mcp.json",
            project_config=".cursor/mcp.json",
            skills_dir="~/.cursor/skills",
        ),
        AgentClient(
            id="claude-code",
            label="Claude Code",
            markers=["~/.claude"],
            config="~/.claude.json",
            project_config=".mcp.json",
            skills_dir="~/.claude/skills",
            binary="claude",
        ),
        AgentClient(
            id="codex",
            label="Codex",
            markers=["~/.codex"],
            config="~/.codex/config.toml",
            skills_dir="~/.codex/skills",
        ),
        AgentClient(
            id="windsurf",
            label="Windsurf",
            markers=["~/.codeium/windsurf", "~/.windsurf"],
            config="~/.codeium/windsurf/mcp_config.json",
        ),
        AgentClient(
            id="claude-desktop",
            label="Claude Desktop",
            markers=["~/Library/Application Support/Claude", "~/.config/Claude"],
            config=_CLAUDE_DESKTOP,
        ),
    ]
}
CLIENT_IDS = tuple(CLIENTS)


def detected_clients() -> List[AgentClient]:
    return [c for c in CLIENTS.values() if c.installed()]


def server_name() -> str:
    return get_settings().name.lower()


def server_command(context: Optional[str]) -> List[str]:
    """Absolute path when known: GUI clients often launch without the shell's PATH."""
    name = server_name()
    command = [shutil.which(name) or name, "mcp"]
    if context and context != DEFAULT_CONTEXT_NAME:
        command += ["--context", context]
    return command


def mcp_entry(command: List[str]) -> Dict[str, Any]:
    return {"command": command[0], "args": command[1:]}


def codex_block(command: List[str]) -> str:
    args = ", ".join(json.dumps(a) for a in command[1:])
    return f"[mcp_servers.{server_name()}]\ncommand = {json.dumps(command[0])}\nargs = [{args}]\n"


def install_client(client: AgentClient, command: List[str], project: bool = False) -> Path:
    path = client.config_path(project)
    path.parent.mkdir(parents=True, exist_ok=True)
    if client.id == "codex":
        existing = path.read_text() if path.exists() else ""
        if f"[mcp_servers.{server_name()}]" not in existing:
            path.write_text(
                (existing.rstrip("\n") + "\n\n" if existing.strip() else "") + codex_block(command)
            )
        return path

    data: Dict[str, Any] = {}
    if path.exists() and path.read_text().strip():
        try:
            data = json.loads(path.read_text())
        except json.JSONDecodeError:
            terminal.error(f"{path} is not valid JSON; fix it or use --print to install by hand.")
    data.setdefault("mcpServers", {})[server_name()] = mcp_entry(command)
    path.write_text(json.dumps(data, indent=2) + "\n")
    return path


def configured(client: AgentClient) -> bool:
    path = client.config_path()
    if not path.exists():
        return False
    text = path.read_text()
    if client.id == "codex":
        return f"[mcp_servers.{server_name()}]" in text
    try:
        return server_name() in (json.loads(text).get("mcpServers") or {})
    except (json.JSONDecodeError, AttributeError):
        return False


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@common.group(
    name="mcp",
    invoke_without_command=True,
    help="""Workspace MCP server for coding agents.

    With no subcommand, serves MCP over stdio (what `mcp install` registers).
    Workspace tools come from the gateway; `deploy` and `login` run here.
    """,
)
@extraclick.config_context_option  # `mcp install --context X` registers `mcp --context X`
@click.pass_context
def mcp(ctx: click.Context, context: Optional[str]):
    if ctx.invoked_subcommand is None:
        ctx.invoke(serve, context=context)


@mcp.command(name="serve", help="Serve MCP over stdio.")
@extraclick.config_context_option
def serve(context: Optional[str]):
    from ..mcp import run_stdio

    raise click.exceptions.Exit(
        run_stdio(context or extraclick.selected_context(), cwd=os.getcwd())
    )


@mcp.command(name="install", help="Register the MCP server with the agent clients on this machine.")
@click.option(
    "--client",
    "clients",
    type=click.Choice(CLIENT_IDS),
    multiple=True,
    help="Repeatable. Default: every client found.",
)
@click.option(
    "--project",
    is_flag=True,
    help="Project-scoped config in the current directory, where supported.",
)
@click.option("--print", "print_only", is_flag=True, help="Print the config instead of writing it.")
@extraclick.config_context_option
def install(clients: Sequence[str], project: bool, print_only: bool, context: Optional[str]):
    command = server_command(context)
    if print_only:
        terminal.print_json({"mcpServers": {server_name(): mcp_entry(command)}})
        click.echo(codex_block(command), nl=False)
        return

    targets = [CLIENTS[c] for c in clients] if clients else detected_clients()
    if not targets:
        terminal.error(
            f"No supported agent found ({', '.join(CLIENT_IDS)}).",
            hint="Pass --client <name>, or --print for the config.",
            code="NOT_FOUND",
        )
        return

    written = {c.id: str(install_client(c, command, project)) for c in targets}
    if terminal.json_output():
        terminal.print_json({"server": server_name(), "command": command, "installed": written})
        return
    for client_id, path in written.items():
        terminal.success(f"{CLIENTS[client_id].label}: {path}")
    terminal.detail("Restart the client to pick it up.")


@mcp.command(
    name="status", help="Agent clients on this machine and whether the server is registered."
)
def status():
    rows = [
        {
            "client": c.id,
            "label": c.label,
            "installed": c.installed(),
            "configured": configured(c),
            "config": str(c.config_path()),
        }
        for c in CLIENTS.values()
    ]
    if terminal.json_output():
        terminal.print_json({"server": server_name(), "clients": rows})
        return
    found = [r for r in rows if r["installed"]]
    for row in found:
        terminal.print(f"  {'✓' if row['configured'] else '·'} {row['label']:<15} {row['config']}")
    if not found:
        terminal.detail("No supported agent clients found.")
