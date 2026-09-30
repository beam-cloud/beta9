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
from typing import Any, Dict, List, Optional, Sequence, Tuple

import click

from .. import terminal
from ..config import DEFAULT_CONTEXT_NAME, cli_path, get_settings
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
    command = [cli_path() or server_name(), "mcp"]
    if context and context != DEFAULT_CONTEXT_NAME:
        command += ["--context", context]
    return command


class ConfigError(Exception):
    pass


def _jsonc_to_json(text: str) -> str:
    """Drop the comments and trailing commas Cursor and Windsurf allow; line numbers survive."""

    def strip(text: str, comments: bool) -> str:
        out: List[str] = []
        i, n = 0, len(text)
        while i < n:
            ch = text[i]
            if ch == '"':
                j = i + 1
                while j < n and text[j] != '"':
                    j += 2 if text[j] == "\\" else 1
                out.append(text[i : j + 1])
                i = j + 1
            elif comments and text.startswith("//", i):
                j = text.find("\n", i)
                i = n if j < 0 else j
            elif comments and text.startswith("/*", i):
                j = text.find("*/", i + 2)
                j = n if j < 0 else j + 2
                out.append("\n" * text.count("\n", i, j))
                i = j
            elif not comments and ch == ",":
                j = i + 1
                while j < n and text[j] in " \t\r\n":
                    j += 1
                if not (j < n and text[j] in "}]"):
                    out.append(ch)
                i += 1
            else:
                out.append(ch)
                i += 1
        return "".join(out)

    return strip(strip(text, comments=True), comments=False)


def load_json_config(path: Path) -> Dict[str, Any]:
    """{} when the file is absent or blank."""
    text = path.read_text() if path.exists() else ""
    if not text.strip():
        return {}
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        try:
            data = json.loads(_jsonc_to_json(text))
        except json.JSONDecodeError as e:
            raise ConfigError(
                f"{path} is not valid JSON (line {e.lineno}, column {e.colno}: {e.msg})."
            ) from None
    if not isinstance(data, dict):
        raise ConfigError(f"{path} must hold a JSON object.")
    return data


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

    data = load_json_config(path)
    servers = data.setdefault("mcpServers", {})
    if not isinstance(servers, dict):
        raise ConfigError(f"{path}: mcpServers must be an object.")
    servers[server_name()] = mcp_entry(command)
    path.write_text(json.dumps(data, indent=2) + "\n")
    return path


def install_clients(
    targets: Sequence[AgentClient], command: List[str], project: bool = False
) -> Tuple[Dict[str, Path], Dict[str, str]]:
    """A config that cannot be edited fails only its own client."""
    written: Dict[str, Path] = {}
    failed: Dict[str, str] = {}
    for client in targets:
        try:
            written[client.id] = install_client(client, command, project)
        except ConfigError as e:
            failed[client.id] = str(e)
    return written, failed


def report_failures(failed: Dict[str, str]) -> None:
    for client_id, reason in failed.items():
        terminal.warn(f"{CLIENTS[client_id].label}: {reason}")
    if failed:
        clients = " ".join(f"--client {c}" for c in failed)
        terminal.detail(
            f"Fix the file, then run `{server_name()} mcp install {clients}`; add --print to see the entry to paste."
        )


def configured(client: AgentClient) -> bool:
    path = client.config_path()
    if not path.exists():
        return False
    if client.id == "codex":
        return f"[mcp_servers.{server_name()}]" in path.read_text()
    try:
        return server_name() in (load_json_config(path).get("mcpServers") or {})
    except (ConfigError, TypeError):
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

    written, failed = install_clients(targets, command, project)
    if terminal.json_output():
        terminal.print_json(
            {
                "server": server_name(),
                "command": command,
                "installed": {k: str(v) for k, v in written.items()},
                "failed": failed,
            }
        )
    else:
        for client_id, path in written.items():
            terminal.success(f"{CLIENTS[client_id].label}: {path}")
        report_failures(failed)
        if written:
            terminal.detail("Restart the client to pick it up.")
    if failed:
        raise click.exceptions.Exit(1)


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
