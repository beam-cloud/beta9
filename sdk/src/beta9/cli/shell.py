import click

from ..channel import ServiceClient
from ..cli import extraclick
from ..utils import load_module_spec
from .extraclick import ClickCommonGroup


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@common.command(
    name="shell",
    help="""
    Connect to a container with the same config as your handler.

    ENTRYPOINT is in the format of "file:function".
    """,
    epilog="""
      Examples:

        {cli_name} shell app.py:handler

        {cli_name} shell app.py:my_func
        \b
    """,
)
@click.argument(
    "entrypoint",
    nargs=1,
    required=True,
)
@click.option(
    "--url-type",
    help="The type of URL to get back. [default is determined by the server] ",
    type=click.Choice(["host", "path"]),
)
@extraclick.pass_service_client
@click.pass_context
def shell(
    ctx: click.Context,
    service: ServiceClient,
    entrypoint: str,
    url_type: str = "path",
):
    user_obj, module_name, obj_name = load_module_spec(entrypoint, "shell")

    if hasattr(user_obj, "set_handler"):
        user_obj.set_handler(f"{module_name}:{obj_name}")

    user_obj.shell(url_type=url_type)  # type:ignore
