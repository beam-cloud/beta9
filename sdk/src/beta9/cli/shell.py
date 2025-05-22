import click

from .. import terminal
from ..abstractions.pod import Pod
from ..channel import ServiceClient
from ..cli import extraclick
from ..utils import load_module_spec
from .extraclick import ClickCommonGroup, handle_config_override, override_config_options


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@common.command(
    name="shell",
    help="""
    Connect to a container with the same config as your handler.

    HANDLER is in the format of "file:function".
    """,
    epilog="""
      Examples:

        {cli_name} shell app.py:handler

        {cli_name} shell app.py:my_func
        \b
    """,
)
@click.argument(
    "handler",
    nargs=1,
    required=False,
)
@click.option(
    "--url-type",
    help="The type of URL to get back. [default is determined by the server] ",
    type=click.Choice(["host", "path"]),
)
@click.option(
    "--container-id",
    help="The ID of the container to connect to.",
    type=str,
)
@override_config_options
@extraclick.pass_service_client
@click.pass_context
def shell(
    ctx: click.Context,
    service: ServiceClient,
    handler: str,
    url_type: str = "path",
    container_id: str = None,
    **kwargs,
):
    entrypoint = kwargs["entrypoint"]
    if handler:
        user_obj, module_name, obj_name = load_module_spec(handler, "shell")

        if hasattr(user_obj, "set_handler"):
            user_obj.set_handler(f"{module_name}:{obj_name}")

    elif entrypoint or container_id:
        user_obj = Pod(entrypoint=entrypoint)

    else:
        terminal.error("No handler or entrypoint specified.")

    if not handle_config_override(user_obj, kwargs):
        return

    user_obj.shell(url_type=url_type, container_id=container_id)  # type:ignore
