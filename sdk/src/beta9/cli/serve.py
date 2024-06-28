import importlib
import os
import sys
from pathlib import Path
from typing import Optional

import click

from .. import terminal
from ..channel import ServiceClient
from ..cli import extraclick
from .extraclick import ClickCommonGroup


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@common.command(
    name="serve",
    help="""
    Serve a function.

    ENTRYPOINT is in the format of "file:function".
    """,
    epilog="""
      Examples:

        {cli_name} serve app.py:handler

        {cli_name} serve app.py:my_func
        \b
    """,
)
@click.argument(
    "entrypoint",
    nargs=1,
    required=True,
)
@click.option(
    "--timeout",
    "-t",
    default=0,
    help="The inactivity timeout for the serve instance in seconds. Set to -1 for no timeout. Set to 0 to use default timeout (10 minutes)",
)
@extraclick.pass_service_client
@click.pass_context
def serve(
    ctx: click.Context, service: ServiceClient, entrypoint: str, timeout: Optional[int] = None
):
    current_dir = os.getcwd()
    if current_dir not in sys.path:
        sys.path.insert(0, current_dir)

    module_path, func_name, *_ = entrypoint.split(":") if ":" in entrypoint else (entrypoint, "")
    module_name = module_path.replace(".py", "").replace(os.path.sep, ".")

    if not Path(module_path).exists():
        terminal.error(f"Unable to find file: '{module_path}'")

    if not func_name:
        terminal.error(
            "Invalid handler function specified. Expected format: beam serve [file.py]:[function]"
        )

    module = importlib.import_module(module_name)

    user_func = getattr(module, func_name, None)
    if user_func is None:
        terminal.error(
            f"Invalid handler function specified. Make sure '{module_path}' contains the function: '{func_name}'"
        )

    user_func.serve(timeout=int(timeout))  # type:ignore
