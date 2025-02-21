from typing import Optional

import click

from ..channel import ServiceClient
from ..cli import extraclick
from ..utils import load_module_spec
from .extraclick import ClickCommonGroup, handle_config_override, override_config_options


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@common.command(
    name="serve",
    help="""
    Serve a function.

    HANDLER is in the format of "file:function".
    """,
    epilog="""
      Examples:

        {cli_name} serve app.py:my_func

        {cli_name} serve app.py:my_func
        \b
    """,
)
@click.argument(
    "handler",
    nargs=1,
    required=True,
)
@click.option(
    "--timeout",
    "-t",
    default=0,
    help="The inactivity timeout for the serve instance in seconds. Set to -1 for no timeout. Set to 0 to use default timeout (10 minutes)",
)
@click.option(
    "--url-type",
    help="The type of URL to get back. [default is determined by the server] ",
    type=click.Choice(["host", "path"]),
)
@override_config_options
@extraclick.pass_service_client
@click.pass_context
def serve(
    ctx: click.Context,
    service: ServiceClient,
    handler: str,
    timeout: Optional[int] = None,
    url_type: str = "path",
    **kwargs,
):
    user_obj, module_name, obj_name = load_module_spec(handler, "serve")

    if hasattr(user_obj, "set_handler"):
        user_obj.set_handler(f"{module_name}:{obj_name}")

    if not handle_config_override(user_obj, kwargs):
        return

    user_obj.serve(timeout=int(timeout), url_type=url_type)  # type:ignore
