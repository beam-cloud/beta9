from typing import Optional

import click

from .. import terminal
from ..auth import DeviceLogin, LoginError, save_login
from ..config import context_defaults, get_settings
from .extraclick import ClickCommonGroup, selected_context


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@common.command(
    name="login",
    help="""Sign in from your browser; creates an account if you don't have one.

    Opens the sign-in page and waits. With --browserless (or on SSH/CI, detected
    automatically) a link and a short code are printed to use from any device.

    Signing in again under a saved context name renews that context where it
    already points, so one CLI can hold several workspaces or clusters.
    """,
)
@click.option(
    "--browserless",
    "-b",
    is_flag=True,
    help="Print the link and code instead of opening a browser.",
)
@click.option(
    "--environment",
    help="Built-in environment to sign in to (a staging cluster, say) instead of the default.",
)
@click.option(
    "--name",
    "context_name",
    help="Context to save the token under [default: the environment's name, else --context].",
)
def login(browserless: bool, environment: Optional[str], context_name: Optional[str]):
    environments = get_settings().environments
    if environment and environment not in environments:
        terminal.error(
            f"Unknown environment '{environment}'.",
            hint=f"Built in: {', '.join(environments) or 'none'}.",
            code="INVALID_ARGUMENT",
        )
        return
    name = context_name or environment or selected_context()
    target = environments[environment] if environment else context_defaults(name)

    try:
        flow = DeviceLogin.start(target)
    except LoginError as exc:
        terminal.error(str(exc), code=exc.code)
        return

    if not browserless and flow.open_browser():
        terminal.header("Finish signing in from your browser")
        terminal.detail(f"If it didn't open: {flow.verification_uri_complete}")
    else:
        terminal.header("Sign in with one click:")
        terminal.print(f"  {flow.verification_uri_complete}\n")
        terminal.detail(f"Or go to {flow.verification_uri} and enter this code:")
        terminal.print(f"  [bold]{flow.user_code}[/bold]")

    try:
        with terminal.progress("Waiting for sign-in..."):
            context = flow.wait()
    except LoginError as exc:
        terminal.error(str(exc), code=exc.code)
        return

    save_login(context, name=name)
    where = f" to {flow.workspace_name}" if flow.workspace_name else ""
    if terminal.json_output():
        terminal.print_json(
            {
                "context": name,
                "workspace": flow.workspace_name,
                "gateway": f"{context.gateway_host}:{context.gateway_port}",
            }
        )
    else:
        terminal.success(f"Signed in{where}. Saved context '{name}'.")
