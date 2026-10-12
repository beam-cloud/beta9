"""
`setup agent`: install the skill and register the MCP server for every coding
agent on this machine, then say what is left (at most: signing in).
"""

import sys
from contextlib import nullcontext, redirect_stdout
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
    product = get_settings().name
    cli = product.lower()
    name = context or extraclick.selected_context()
    can_login = login_configured(name)
    result = configure_agents(clients, skip_skills, skip_mcp, context)
    skills, mcp, failed = result["skills"], result["mcp"], result["failed"]
    signed_in = result["signed_in"]

    if terminal.json_output():
        terminal.print_json({**result, "skills": list(skills.values())})
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
        elif not result["clients"]:
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


def configure_agents(clients, skip_skills, skip_mcp, context):
    """Configure agent clients without printing a separate completion or JSON document."""
    from ..mcp.server import context_or_none

    targets = [CLIENTS[c] for c in clients] if clients else detected_clients()
    skills = {}
    if not skip_skills:
        skills["Universal (.agents)"] = str(install_skill(Path(UNIVERSAL_SKILLS_DIR).expanduser()))
        for client in targets:
            if client.skills_dir:
                skills[client.label] = str(install_skill(Path(client.skills_dir).expanduser()))
    mcp: Dict[str, Path] = {}
    failed: Dict[str, str] = {}
    if not skip_mcp:
        mcp, failed = install_clients(targets, server_command(context))
    return {
        "cli": cli_path() or get_settings().name.lower(),
        "clients": [client.id for client in targets],
        "signed_in": context_or_none(context or extraclick.selected_context()) is not None,
        "skills": skills,
        "mcp": {key: str(value) for key, value in mcp.items()},
        "failed": failed,
    }


@setup.command(
    name="vm",
    help="Sign in, connect your agents and start a real persistent CPU VM. Safe to resume.",
)
@click.option("--name", default="beam-agent", help="Name of the first VM.")
@click.option("--desktop", is_flag=True, help="Include a browser desktop.")
@click.option("--client", "clients", type=click.Choice(CLIENT_IDS), multiple=True)
@click.option("--skip-agents", is_flag=True, help="Sign in and prepare the VM only.")
@click.option("-y", "--yes", is_flag=True, help="Approve creation without prompting.")
@extraclick.config_context_option
def setup_vm(name, desktop, clients, skip_agents, yes, context):
    from ..auth import DeviceLogin, LoginError, save_login
    from ..channel import GatewayHTTPError, ServiceClient
    from ..config import context_defaults, get_config_context
    from ..deployment.onboarding import prepare_vm
    from ..mcp.server import context_or_none

    selected = context or extraclick.selected_context()
    cli = get_settings().name.lower()
    try:
        if context_or_none(selected) is None:
            flow = DeviceLogin.start(context_defaults(selected))
            with redirect_stdout(sys.stderr) if terminal.json_output() else nullcontext():
                terminal.header("Connect your Beam account")
                terminal.print(f"  {flow.verification_uri_complete}")
                flow.open_browser()
                with terminal.progress("Waiting for sign-in..."):
                    save_login(flow.wait(), selected)
        agents = None
        if not skip_agents:
            with redirect_stdout(sys.stderr) if terminal.json_output() else nullcontext():
                agents = configure_agents(clients, False, False, selected)
            if agents["failed"]:
                report_failures(agents["failed"])
                raise click.ClickException("Agent configuration failed; fix it and resume setup")
            if not terminal.json_output():
                terminal.success("Agent skills and MCP configured")
        if not yes:
            click.confirm(
                "Start your VM (1 CPU, 2 GiB RAM, 10 GiB disk; sleeps after 15 minutes idle)?",
                abort=True,
            )
        with redirect_stdout(sys.stderr) if terminal.json_output() else nullcontext():
            with ServiceClient(get_config_context(selected)) as service:
                with terminal.progress("Preparing your VM and checking the guest..."):
                    result = prepare_vm(
                        service,
                        get_settings().config_path.parent / "onboarding",
                        name,
                        desktop=desktop,
                    )
        if terminal.json_output():
            terminal.print_json(
                {"ready": True, "context": selected, "agents": agents, "vm": result}
            )
        else:
            terminal.success(f"VM {result['name']} is ready — guest command verified")
            terminal.print(f"  {cli} --context {selected} vm ssh {result['name']}")
            terminal.print(f"  {cli} --context {selected} vm terminal {result['name']}")
            if result.get("desktop_url"):
                terminal.print(f"  {cli} --context {selected} vm desktop {result['name']}")
            terminal.detail(
                "Restart your agent to use Beam's VM tools. Re-run this setup to resume the same VM."
            )
    except (LoginError, GatewayHTTPError, RuntimeError, TimeoutError, ValueError, OSError) as exc:
        raise click.ClickException(str(exc)) from exc
