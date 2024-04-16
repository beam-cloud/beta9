import importlib
import os
import sys
from pathlib import Path

import click

from beta9.cli.extraclick import ClickCommonGroup, ClickManagementGroup

from .. import terminal
from .contexts import ServiceClient, get_gateway_service


@click.group(cls=ClickCommonGroup)
@click.pass_context
def common(ctx: click.Context):
    ctx.obj = ctx.with_resource(ServiceClient())


@common.command(
    name="deploy",
    help="""
    Deploy a new function.

    ENTRYPOINT is in the format of "file:function".
    """,
)
@click.option(
    "--name",
    type=click.STRING,
    help="The name the deployment.",
    required=True,
)
@click.argument(
    "entrypoint",
    nargs=1,
    required=True,
)
@click.pass_context
def deploy(ctx: click.Context, name: str, entrypoint: str):
    ctx.invoke(create_deployment, name=name, entrypoint=entrypoint)


@click.group(
    name="deployment",
    help="Manage deployments.",
    cls=ClickManagementGroup,
)
@click.pass_context
def management(ctx: click.Context):
    ctx.obj = ctx.with_resource(get_gateway_service())


@management.command(
    name="create",
    help="Create a new deployment.",
)
@click.option("--name", help="The name the deployment.", required=True)
@click.option("--entrypoint", help='The name the entrypoint e.g. "file:function".', required=True)
def create_deployment(name: str, entrypoint: str):
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

    func = getattr(module, func_name)
    if not func.deploy(name=name):
        terminal.error("Deployment failed ☠️")
