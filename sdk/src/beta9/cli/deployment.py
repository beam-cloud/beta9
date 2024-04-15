import importlib
import os
import sys

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
    help="Deploy a new function.",
)
@click.pass_obj
def deploy(service: ServiceClient):
    # todo: implement a quicker/shorter version of create_deployment()
    pass


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
@click.option("--function", help="The name the entry point and function.", required=True)
def create_deployment(name: str, function: str):
    current_dir = os.getcwd()
    if current_dir not in sys.path:
        sys.path.insert(0, current_dir)

    module_path, func_name = function.split(":")
    module_name = module_path.replace(".py", "").replace(os.path.sep, ".")
    module = importlib.import_module(module_name)

    func = getattr(module, func_name)
    if not func.deploy(name=name):
        terminal.error("Deployment failed ☠️")
