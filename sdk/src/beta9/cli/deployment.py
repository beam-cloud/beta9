import importlib
import os
import sys
from pathlib import Path

import click

from .. import terminal
from ..channel import ServiceClient
from ..cli import extraclick
from .extraclick import ClickCommonGroup, ClickManagementGroup


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@common.command(
    name="deploy",
    help="""
    Deploy a new function.

    ENTRYPOINT is in the format of "file:function".
    """,
    epilog="""
      Examples:

        {cli_name} deploy --name my-app app.py:handler

        {cli_name} deploy -n my-app-2 app.py:my_func
        \b
    """,
)
@click.option(
    "--name",
    "-n",
    type=click.STRING,
    help="The name the deployment.",
    required=True,
)
@click.argument(
    "entrypoint",
    nargs=1,
    required=True,
)
@extraclick.pass_service_client
@click.pass_context
def deploy(ctx: click.Context, service: ServiceClient, name: str, entrypoint: str):
    ctx.invoke(create_deployment, name=name, entrypoint=entrypoint)


@click.group(
    name="deployment",
    help="Manage deployments.",
    cls=ClickManagementGroup,
)
def management():
    pass


@management.command(
    name="create",
    help="Create a new deployment.",
    epilog="""
      Examples:

        {cli_name} deploy --name my-app --entrypoint app.py:handler
        \b
    """,
)
@click.option(
    "--name",
    "-n",
    help="The name the deployment.",
    required=True,
)
@click.option(
    "--entrypoint",
    "-e",
    help='The name the entrypoint e.g. "file:function".',
    required=True,
)
@extraclick.pass_service_client
def create_deployment(service: ServiceClient, name: str, entrypoint: str):
    current_dir = os.getcwd()
    if current_dir not in sys.path:
        sys.path.insert(0, current_dir)

    module_path, func_name, *_ = entrypoint.split(":") if ":" in entrypoint else (entrypoint, "")
    module_name = module_path.replace(".py", "").replace(os.path.sep, ".")

    if not Path(module_path).exists():
        terminal.error(f"Unable to find file '{module_path}'")
    if not func_name:
        terminal.error(f"Unable to parse function '{func_name}'")

    module = importlib.import_module(module_name)

    user_func = getattr(module, func_name, None)
    if user_func is None:
        terminal.error(f"Unable to find function '{func_name}'")

    if not user_func.deploy(name=name):  # type:ignore
        terminal.error("Deployment failed ☠️")
