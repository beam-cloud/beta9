"""Account sign-in, coding agent configuration, and workload onboarding."""

import shlex
import sys
from contextlib import nullcontext, redirect_stdout
from pathlib import Path
from typing import Any, Dict, Optional, Sequence, Tuple

import click
from rich.syntax import Syntax

from .. import terminal
from ..auth import login_configured
from ..config import ConfigContext, cli_path, get_settings
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
# The minimum credit purchase, which also covers a first VM.
CHECKOUT_CENTS = 2000


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
    help="Accepted for symmetry with installers. Nothing prompts.",
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
    terminal.success(f"CLI: {cli_path() or cli}")
    if skills:
        terminal.success(f"Agent skill `{skill_name()}`: {', '.join(skills)}")
    if not skip_mcp:
        if mcp:
            terminal.success(f"{product} MCP: {', '.join(CLIENTS[c].label for c in mcp)}")
        elif not result["clients"]:
            terminal.detail(
                f"No coding agents found. Run {cli} mcp install --client <name> after installing one."
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


def configure_agents(
    clients: Sequence[str], skip_skills: bool, skip_mcp: bool, context: Optional[str]
) -> Dict[str, Any]:
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
    help="Sign in, connect your agents and start a persistent CPU VM with a browser desktop. "
    "Run again to resume.",
)
@click.option("--name", help="Name of the first VM. Generated if omitted.")
@click.option("--desktop/--no-desktop", default=True, help="Include a browser desktop.")
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
    try:
        with redirect_stdout(sys.stderr) if terminal.json_output() else nullcontext():
            selected_context, agents = connect_onboarding(selected, clients, skip_agents, yes)
        if not confirm_workload(
            "Start your VM (4 vCPU, 8 GiB RAM, 50 GiB disk, runs until stopped)?", yes
        ):
            return
        with redirect_stdout(sys.stderr) if terminal.json_output() else nullcontext():
            with ServiceClient(get_config_context(selected)) as service:
                with terminal.StepTracker().step("Starting VM", "VM ready"):
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
            terminal.resource(
                result["name"],
                {
                    "ID": result["id"],
                    "Status": result.get("status"),
                    "Terminal": result.get("terminal_url"),
                    "Desktop": result.get("desktop_url"),
                    "Dashboard": dashboard,
                },
            )
            terminal.detail(
                "Restart your agent to use the VM tools. Re-run this setup to resume the same VM."
            )
            command = extraclick.command_hint(selected)
            name = shlex.quote(result["name"])
            terminal.print("")
            for action in ("ssh", "terminal", "logs", "stop"):
                terminal.print(f"  {command} vm {action} {name}", markup=False)
            if result.get("desktop_url"):
                terminal.print(f"  {command} vm desktop {name}", markup=False)
    except click.Abort:
        raise
    except (LoginError, GatewayHTTPError, RuntimeError, TimeoutError, ValueError, OSError) as exc:
        raise click.ClickException(str(exc)) from exc


def confirm_workload(prompt: str, yes: bool) -> bool:
    if yes:
        return True
    with redirect_stdout(sys.stderr) if terminal.json_output() else nullcontext():
        if click.confirm(prompt):
            return True
    if terminal.json_output():
        terminal.print_json({"ready": False, "cancelled": True})
    else:
        terminal.detail("Setup canceled.")
    return False


def ensure_credit(context: ConfigContext, unattended: bool) -> None:
    """Use the gateway's credit decision; Enter opens checkout for the signed-in account."""
    import time
    import webbrowser

    from ..mcp.tools import call_remote

    credit = call_remote(context, "whoami", {}).get("credit")
    if not credit or credit.get("ok"):
        return
    message = credit.get("message") or "Add credits before starting compute."
    if unattended or not sys.stdin.isatty():
        raise click.ClickException(message)
    terminal.header("Add credits")
    terminal.print(credit_reason(message), markup=False)
    click.prompt(
        f"Press enter to add ${CHECKOUT_CENTS // 100} in credits (Ctrl+C to exit)",
        default="",
        show_default=False,
        prompt_suffix=" ",
    )
    link = credit_checkout(context) or credits_page(message)
    if not link:
        raise click.ClickException(message)
    webbrowser.open(link, new=2)
    terminal.detail(f"If it didn't open: {link}")
    deadline = time.monotonic() + 900
    with terminal.progress("Waiting for checkout"):
        while time.monotonic() < deadline:
            if call_remote(context, "whoami", {}).get("credit", {}).get("ok"):
                terminal.success("Added credits")
                return
            time.sleep(3)
    raise click.ClickException("Credits are still pending. Run this command again after checkout.")


def credit_reason(message: str) -> str:
    """The first sentence of the gateway's credit message, without the dashboard link."""
    import re

    reason = re.split(r"[;.]\s", message, maxsplit=1)[0].rstrip(".")
    return reason[:1].upper() + reason[1:] + "."


def credits_page(message: str) -> str:
    import re

    match = re.search(r"https?://[^\s<>]+", message)
    return match.group(0).rstrip(".,)") if match else ""


def credit_checkout(context: ConfigContext) -> str:
    """A Stripe Checkout URL for this context's account, or "" if its auth server has none."""
    import requests

    if not context.auth_url or not context.token:
        return ""
    try:
        response = requests.post(
            context.auth_url.rstrip("/") + "/cli/checkout",
            json={"total_cents_amount": CHECKOUT_CENTS},
            headers={"Authorization": f"Bearer {context.token}"},
            timeout=10,
            allow_redirects=False,
        )
        url = response.json().get("url") if response.status_code == 201 else ""
    except (requests.RequestException, ValueError, AttributeError):
        return ""
    return url if isinstance(url, str) and url.startswith("https://") else ""


def connect_onboarding(
    selected: str, clients: Sequence[str], skip_agents: bool, unattended: bool
) -> Tuple[ConfigContext, Optional[Dict[str, Any]]]:
    from ..auth import DeviceLogin, save_login
    from ..config import context_defaults, get_config_context
    from ..mcp.server import context_or_none

    if context_or_none(selected) is None:
        flow = DeviceLogin.start(context_defaults(selected))
        terminal.header(f"Sign in to {get_settings().name}")
        terminal.print(f"  {flow.verification_uri_complete}")
        flow.open_browser()
        with terminal.progress("Waiting for sign-in"):
            save_login(flow.wait(), selected)
    context = get_config_context(selected)
    if context.auth_url and (not context.app_url_template or not context.vm_url_template):
        context = provider_links(context)
        save_login(context, selected)
    ensure_credit(context, unattended)
    agents = None if skip_agents else configure_agents(clients, False, False, selected)
    if agents and agents["failed"]:
        report_failures(agents["failed"])
        raise click.ClickException("Agent configuration failed. Fix it and resume setup.")
    if agents:
        terminal.success("Agent skills and MCP configured")
    return context, agents


def provider_links(context: ConfigContext) -> ConfigContext:
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


def dashboard_link(context: ConfigContext, kind: str, external_id: str) -> str:
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
    name="inference", help="Sign in and deploy a hello-world endpoint. Checks the response."
)
@workload_options
def setup_inference(name, clients, skip_agents, yes, context):
    setup_example("inference", name, clients, skip_agents, yes, context)


@setup.command(name="functions", help="Sign in and run a hello-world function. Checks the output.")
@workload_options
def setup_functions(name, clients, skip_agents, yes, context):
    setup_example("functions", name, clients, skip_agents, yes, context)


@setup.command(
    name="sandboxes", help="Sign in and run hello world in a sandbox. Stops it when finished."
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
        if not confirm_workload(
            "Run hello world in a sandbox (1 CPU, 256 MiB, stops when finished)?"
            if kind == "sandboxes"
            else "Run hello world (1 CPU, 256 MiB, scales to zero when idle)?",
            yes,
        ):
            return
        with redirect_stdout(sys.stderr) if terminal.json_output() else nullcontext():
            with ServiceClient(configured) as service:
                message = (
                    "Running hello world in a sandbox"
                    if kind == "sandboxes"
                    else "Deploying hello world and checking the output"
                )
                with terminal.progress(message):
                    result = prepare_example(
                        service,
                        get_settings().config_path.parent / "onboarding",
                        name,
                        kind,
                        lambda tool, arguments: call_remote(configured, tool, arguments),
                        on_source=show_example_source if not terminal.json_output() else None,
                        on_output=lambda text: terminal.print(text, markup=False, end=""),
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
                f"{name}: hello world passed. Sandbox stopped."
                if kind == "sandboxes"
                else f"{name}: hello world passed"
            )
            if dashboard:
                terminal.print(f"  Dashboard: {dashboard}", markup=False, soft_wrap=True)
            if result.get("url"):
                terminal.print(f"  Endpoint: {result['url']}", markup=False, soft_wrap=True)
            terminal.print(f"  Code: {result['source_path']}", markup=False, soft_wrap=True)
            if result.get("curl_config"):
                terminal.print("")
                terminal.print(
                    f"curl --config {shlex.quote(result['curl_config'])} "
                    f"-H 'Content-Type: application/json' -d '{{}}' "
                    f"{shlex.quote(result['url'])}",
                    markup=False,
                    soft_wrap=True,
                )
    except click.Abort:
        raise
    except (LoginError, GatewayHTTPError, RuntimeError, TimeoutError, ValueError, OSError) as exc:
        raise click.ClickException(str(exc)) from exc


def show_example_source(path: Path) -> None:
    terminal.detail(str(path))
    terminal.print(
        Syntax(
            path.read_text(),
            "python",
            theme="ansi_dark",
            background_color="default",
            padding=(1, 2),
            word_wrap=True,
        )
    )
