"""
`setup agent`: install the skill and register the MCP server for every coding
agent on this machine, then say what is left (at most: signing in).
"""

import shutil
from pathlib import Path
from typing import Dict, Optional, Sequence

import click

from .. import terminal
from ..auth import login_configured
from ..config import get_settings
from ..skills import install_skill, skill_name
from . import extraclick
from .extraclick import ClickCommonGroup
from .mcp import CLIENT_IDS, client_by_id, detected_clients, install_client, server_command

UNIVERSAL_SKILLS_DIR = "~/.agents/skills"


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@common.group(name="setup", help="Set up this machine.")
def setup():
    pass


@setup.command(
    name="agent",
    help="""Prepare the coding agents on this machine (Cursor, Claude Code, Codex, Windsurf,
    Claude Desktop): install the skill and register the MCP server for each one found.
    Safe to re-run after installing a new agent.""",
)
@click.option(
    "--client",
    "clients",
    type=click.Choice(CLIENT_IDS),
    multiple=True,
    help="Repeatable. Default: every client found.",
)
@click.option("--skip-skills", is_flag=True, help="Register MCP only.")
@click.option("--skip-mcp", is_flag=True, help="Install skills only.")
@click.option(
    "-y",
    "--yes",
    is_flag=True,
    expose_value=False,
    help="Accepted for symmetry with installers; nothing prompts.",
)
@extraclick.config_context_option
def agent(clients: Sequence[str], skip_skills: bool, skip_mcp: bool, context: Optional[str]):
    from ..mcp.server import context_or_none

    product, cli = get_settings().name, get_settings().name.lower()
    targets = [client_by_id(c) for c in clients] if clients else detected_clients()
    signed_in = context_or_none(context or extraclick.selected_context()) is not None
    report: Dict[str, object] = {
        "cli": shutil.which(cli) or cli,
        "clients": [c.id for c in targets],
        "signed_in": signed_in,
    }
    lines = []

    if not skip_skills:
        dirs = [Path(UNIVERSAL_SKILLS_DIR).expanduser()] + [
            Path(c.skills_dir).expanduser() for c in targets if c.skills_dir
        ]
        report["skills"] = [str(install_skill(d)) for d in dirs]
        labels = ["Universal (.agents)"] + [c.label for c in targets if c.skills_dir]
        lines.append(f"Agent skill `{skill_name()}` — {', '.join(labels)}")

    if not skip_mcp:
        command = server_command(context)
        report["mcp"] = {c.id: str(install_client(c, command)) for c in targets}
        lines.append(
            f"{product} MCP — {', '.join(c.label for c in targets)}"
            if targets
            else f"{product} MCP — no agent clients found; later: {cli} mcp install --client <name>"
        )

    if terminal.json_output():
        terminal.print_json(report)
        return

    terminal.header(f"Setting up {product} for agents")
    terminal.success(f"CLI — {report['cli']}")
    for line in lines:
        terminal.success(line)
    terminal.print("")
    terminal.success("Setup complete")
    terminal.print("\n[bold]Next steps[/bold]")
    step = 1
    if not signed_in:
        how = f"{cli} login" if login_configured() else f"{cli} config create"
        terminal.print(f"  {step}  Run [bold]{how}[/bold] to connect your {product} account")
        step += 1
    terminal.print(
        f"  {step}  Restart your agent and ask it to [bold]'Deploy this application'[/bold]"
    )
    if not signed_in and login_configured():
        terminal.detail(
            "Agents can also sign you in themselves: the MCP server offers a `login` tool."
        )
