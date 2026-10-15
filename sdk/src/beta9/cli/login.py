from typing import Optional

import click

from .. import terminal
from ..auth import DeviceLogin, LoginError, finish_login
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
    help="Sign in to a configured environment, such as staging.",
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
        context = finish_login(flow, name, browserless=browserless)
    except LoginError as exc:
        terminal.error(str(exc), code=exc.code)
        return

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
