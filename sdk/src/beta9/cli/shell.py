import importlib
import os
import sys
from pathlib import Path

import click

from .. import terminal
from ..channel import ServiceClient
from ..cli import extraclick
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
    current_dir = os.getcwd()
    if current_dir not in sys.path:
        sys.path.insert(0, current_dir)

    module_path, obj_name, *_ = entrypoint.split(":") if ":" in entrypoint else (entrypoint, "")
    module_name = module_path.replace(".py", "").replace(os.path.sep, ".")

    if not Path(module_path).exists():
        terminal.error(f"Unable to find file: '{module_path}'")

    if not obj_name:
        terminal.error(
            "Invalid handler function specified. Expected format: beam shell [file.py]:[function]"
        )

    module = importlib.import_module(module_name)

    user_obj = getattr(module, obj_name, None)
    if user_obj is None:
        terminal.error(
            f"Invalid handler function specified. Make sure '{module_path}' contains the function: '{obj_name}'"
        )

    if hasattr(user_obj, "set_handler"):
        user_obj.set_handler(f"{module_name}:{obj_name}")

    user_obj.shell(url_type=url_type)  # type:ignore
