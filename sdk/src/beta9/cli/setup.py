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
@click.option("--name", help="Name of the first VM; defaults to <product>-agent.")
@click.option("--desktop", is_flag=True, help="Include a browser desktop.")
@click.option("--client", "clients", type=click.Choice(CLIENT_IDS), multiple=True)
@click.option("--skip-agents", is_flag=True, help="Sign in and prepare the VM only.")
@click.option("-y", "--yes", is_flag=True, help="Approve creation without prompting.")
@extraclick.config_context_option
def setup_vm(name, desktop, clients, skip_agents, yes, context):
    from ..auth import LoginError
    from ..channel import GatewayHTTPError, ServiceClient
    from ..config import get_config_context
    from ..deployment.onboarding import prepare_vm

    selected = context or extraclick.selected_context()
    cli = get_settings().name.lower()
    name = name or f"{cli}-agent"
    try:
        with redirect_stdout(sys.stderr) if terminal.json_output() else nullcontext():
            selected_context, agents = connect_onboarding(selected, clients, skip_agents, yes)
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
        dashboard = dashboard_link(selected_context, "vm", result["id"])
        if terminal.json_output():
            terminal.print_json(
                {
                    "ready": True,
                    "context": selected,
                    "agents": agents,
                    "vm": result,
                    "dashboard_url": dashboard,
                }
            )
        else:
            terminal.success(f"VM {result['name']} is ready — guest command verified")
            if dashboard:
                terminal.print(f"  Dashboard and logs: {dashboard}")
            terminal.print(f"  {cli} --context {selected} vm ssh {result['name']}")
            terminal.print(f"  {cli} --context {selected} vm terminal {result['name']}")
            if result.get("desktop_url"):
                terminal.print(f"  {cli} --context {selected} vm desktop {result['name']}")
            terminal.detail(
                "Restart your agent to use the VM tools. Re-run this setup to resume the same VM."
            )
    except (LoginError, GatewayHTTPError, RuntimeError, TimeoutError, ValueError, OSError) as exc:
        raise click.ClickException(str(exc)) from exc


def ensure_credit(context, unattended):
    """Use the gateway's credit decision; purchases stay in the product dashboard."""
    import re
    import time
    import webbrowser

    from ..mcp.tools import call_remote

    summary = call_remote(context, "whoami", {})
    credit = summary.get("credit")
    if not credit or credit.get("ok"):
        return
    message = credit.get("message") or "This workspace needs credits before compute can start."
    match = re.search(r"https?://[^\s<>]+", message)
    link = match.group(0).rstrip(".,)") if match else ""
    if unattended or not sys.stdin.isatty() or not link:
        raise click.ClickException(message)
    terminal.header(f"Fund workspace {summary.get('workspace_name', '')}")
    terminal.print(message)
    if not click.confirm(
        "Open the credits page? You choose the amount and approve checkout in the browser"
    ):
        raise click.Abort()
    webbrowser.open(link, new=2)
    terminal.print(
        "Complete checkout in the browser. This terminal will continue when credits are confirmed."
    )
    deadline = time.monotonic() + 900
    with terminal.progress("Waiting for credits"):
        while time.monotonic() < deadline:
            if call_remote(context, "whoami", {}).get("credit", {}).get("ok"):
                terminal.success("Credits confirmed")
                return
            time.sleep(3)
    raise click.ClickException(
        "Credits are still pending. Resume this command after checkout completes."
    )


def connect_onboarding(selected, clients, skip_agents, unattended):
    from ..auth import DeviceLogin, save_login
    from ..config import context_defaults, get_config_context
    from ..mcp.server import context_or_none

    if context_or_none(selected) is None:
        flow = DeviceLogin.start(context_defaults(selected))
        terminal.header(f"Connect your {get_settings().name} account")
        terminal.print(f"  {flow.verification_uri_complete}")
        flow.open_browser()
        with terminal.progress("Waiting for sign-in..."):
            save_login(flow.wait(), selected)
    context = get_config_context(selected)
    if context.auth_url and (not context.app_url_template or not context.vm_url_template):
        context = provider_links(context)
        save_login(context, selected)
    ensure_credit(context, unattended)
    agents = None if skip_agents else configure_agents(clients, False, False, selected)
    if agents and agents["failed"]:
        report_failures(agents["failed"])
        raise click.ClickException("Agent configuration failed; fix it and resume setup")
    if agents:
        terminal.success("Agent skills and MCP configured")
    return context, agents


def provider_links(context):
    """Optional UI hints; a missing extension never blocks self-hosted installs."""
    from dataclasses import replace
    import requests

    try:
        response = requests.get(
            context.auth_url.rstrip("/") + "/cli/metadata", timeout=10, allow_redirects=False
        )
        if response.status_code != 200:
            return context
        hints = response.json()
        if not isinstance(hints, dict):
            return context
        fields = {}
        for name in ("app_url_template", "vm_url_template"):
            value = hints.get(name)
            if isinstance(value, str) and len(value) <= 2048:
                fields[name] = value
        return replace(context, **fields)
    except (requests.RequestException, ValueError):
        return context


def dashboard_link(context, kind, external_id):
    from urllib.parse import quote, urlsplit

    field = f"{kind}_url_template"
    template = getattr(context, field, None)
    if (
        not template
        and urlsplit(context.http_url).hostname == urlsplit(get_settings().api_url).hostname
    ):
        template = getattr(get_settings(), field, "")
    try:
        link = (template or "").format(**{f"{kind}_id": quote(external_id, safe="")})
        parsed = urlsplit(link)
    except (KeyError, ValueError):
        return ""
    if (
        parsed.scheme not in ("http", "https")
        or not parsed.hostname
        or parsed.username
        or parsed.password
    ):
        return ""
    return link


def workload_options(fn):
    fn = click.option("--name", help="Name of the sample app.")(fn)
    fn = click.option("--client", "clients", type=click.Choice(CLIENT_IDS), multiple=True)(fn)
    fn = click.option("--skip-agents", is_flag=True, help="Prepare the workload only.")(fn)
    fn = click.option(
        "-y", "--yes", is_flag=True, help="Approve compute creation without prompting."
    )(fn)
    return extraclick.config_context_option(fn)


@setup.command(
    name="inference", help="Sign in, fund the workspace and deploy a verified hello-world endpoint."
)
@workload_options
def setup_inference(name, clients, skip_agents, yes, context):
    setup_example("inference", name, clients, skip_agents, yes, context)


@setup.command(
    name="functions", help="Sign in, fund the workspace and run a verified cloud function."
)
@workload_options
def setup_functions(name, clients, skip_agents, yes, context):
    setup_example("functions", name, clients, skip_agents, yes, context)


@setup.command(
    name="sandboxes", help="Sign in, fund the workspace and run a verified container sandbox."
)
@workload_options
def setup_sandboxes(name, clients, skip_agents, yes, context):
    setup_example("sandboxes", name, clients, skip_agents, yes, context)


def setup_example(kind, name, clients, skip_agents, yes, context):
    from ..auth import LoginError
    from ..channel import GatewayHTTPError, ServiceClient
    from ..deployment.onboarding import prepare_example
    from ..mcp.tools import call_remote

    selected = context or extraclick.selected_context()
    name = name or f"{get_settings().name.lower()}-hello-{kind}"
    try:
        with redirect_stdout(sys.stderr) if terminal.json_output() else nullcontext():
            configured, agents = connect_onboarding(selected, clients, skip_agents, yes)
            if not yes:
                click.confirm(
                    "Run hello world in a sandbox (1 CPU, 256 MiB; stops when finished)?"
                    if kind == "sandboxes"
                    else "Start the hello-world workload (1 CPU, 256 MiB; scales to zero when idle)?",
                    abort=True,
                )
            with ServiceClient(configured) as service:
                message = (
                    "Running your sandbox and checking its output..."
                    if kind == "sandboxes"
                    else "Deploying your sample and checking its result..."
                )
                with terminal.progress(message):
                    result = prepare_example(
                        service,
                        get_settings().config_path.parent / "onboarding",
                        name,
                        kind,
                        lambda tool, arguments: call_remote(configured, tool, arguments),
                    )
        dashboard = dashboard_link(configured, "app", result["app_id"])
        if terminal.json_output():
            terminal.print_json(
                {
                    "ready": True,
                    "context": selected,
                    "agents": agents,
                    "app": result,
                    "dashboard_url": dashboard,
                }
            )
        else:
            terminal.success(
                f"{name} complete — hello-world result verified and sandbox stopped"
                if kind == "sandboxes"
                else f"{name} is ready — hello-world result verified"
            )
            if dashboard:
                terminal.print(f"  Dashboard and logs: {dashboard}")
            if result.get("url"):
                terminal.print(f"  Endpoint: {result['url']}")
    except (LoginError, GatewayHTTPError, RuntimeError, TimeoutError, ValueError, OSError) as exc:
        raise click.ClickException(str(exc)) from exc
