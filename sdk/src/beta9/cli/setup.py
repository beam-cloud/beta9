"""
`setup agent`: install the skill and register the MCP server for every coding
agent on this machine, then say what is left (at most: signing in).
"""

from pathlib import Path
from typing import Dict, Optional, Sequence

import click

from .. import terminal
from ..auth import login_configured
from ..config import cli_path, get_settings
from ..skills import install_skill, skill_name
from . import extraclick
from .extraclick import ClickCommonGroup
from .mcp import (
    CLIENT_IDS,
    CLIENTS,
    detected_clients,
    install_clients,
    report_failures,
    server_command,
)

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

    product = get_settings().name
    cli = product.lower()
    targets = [CLIENTS[c] for c in clients] if clients else detected_clients()
    name = context or extraclick.selected_context()
    signed_in = context_or_none(name) is not None
    can_login = login_configured(name)

    # Skills go to the universal directory plus every client that has its own.
    skills = {}
    if not skip_skills:
        skills["Universal (.agents)"] = install_skill(Path(UNIVERSAL_SKILLS_DIR).expanduser())
        for c in targets:
            if c.skills_dir:
                skills[c.label] = install_skill(Path(c.skills_dir).expanduser())
    mcp: Dict[str, Path] = {}
    failed: Dict[str, str] = {}
    if not skip_mcp:
        mcp, failed = install_clients(targets, server_command(context))

    if terminal.json_output():
        terminal.print_json(
            {
                "cli": cli_path() or cli,
                "clients": [c.id for c in targets],
                "signed_in": signed_in,
                "skills": [str(p) for p in skills.values()],
                "mcp": {k: str(v) for k, v in mcp.items()},
                "failed": failed,
            }
        )
        if failed:
            raise click.exceptions.Exit(1)
        return

    terminal.header(f"Setting up {product} for agents")
    terminal.success(f"CLI — {cli_path() or cli}")
    if skills:
        terminal.success(f"Agent skill `{skill_name()}` — {', '.join(skills)}")
    if not skip_mcp:
        if mcp:
            terminal.success(f"{product} MCP — {', '.join(CLIENTS[c].label for c in mcp)}")
        elif not targets:
            terminal.success(
                f"{product} MCP — no agent clients found; later: {cli} mcp install --client <name>"
            )
        report_failures(failed)
    terminal.print("")
    terminal.success("Setup complete" if not failed else "Setup finished with errors")

    terminal.print("\n[bold]Next steps[/bold]")
    steps = []
    if not signed_in:
        how = f"{cli} login" if can_login else f"{cli} config create"
        steps.append(f"Run [bold]{how}[/bold] to connect your {product} account")
    steps.append("Restart your agent and ask it to [bold]'Deploy this application'[/bold]")
    for number, step in enumerate(steps, start=1):
        terminal.print(f"  {number}  {step}")
    if not signed_in and can_login:
        terminal.detail(
            "Agents can also sign you in themselves: the MCP server offers a `login` tool."
        )
    if failed:
        raise click.exceptions.Exit(1)
