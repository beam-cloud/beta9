import click

from .. import terminal
from ..auth import DeviceLogin, LoginError, save_login
from ..config import DEFAULT_CONTEXT_NAME
from .extraclick import ClickCommonGroup


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@common.command(
    name="login",
    help="""Sign in from your browser; creates an account if you don't have one.

    Opens the sign-in page and waits. With --browserless (or on SSH/CI, detected
    automatically) a link and a short code are printed to use from any device.
    """,
)
@click.option(
    "--browserless",
    "-b",
    is_flag=True,
    help="Print the link and code instead of opening a browser.",
)
@click.option(
    "--name",
    "context_name",
    default=DEFAULT_CONTEXT_NAME,
    show_default=True,
    help="Context to save the token under.",
)
def login(browserless: bool, context_name: str):
    try:
        flow = DeviceLogin.start()
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

    save_login(context, name=context_name)
    where = f" to {flow.workspace_name}" if flow.workspace_name else ""
    if terminal.json_output():
        terminal.print_json(
            {
                "context": context_name,
                "workspace": flow.workspace_name,
                "gateway": f"{context.gateway_host}:{context.gateway_port}",
            }
        )
    else:
        terminal.success(f"Signed in{where}. Saved context '{context_name}'.")
