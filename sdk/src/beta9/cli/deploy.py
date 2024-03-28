import importlib
import os
import sys

import click

from .. import terminal
from ..cli.contexts import get_gateway_service
from ..clients.gateway import GatewayServiceStub


@click.group(
    name="deploy",
    help="List and create deployments",
)
@click.pass_context
def cli(ctx: click.Context):
    ctx.obj = ctx.with_resource(get_gateway_service())


@cli.command(
    name="create",
    help="Create a new deployment",
)
@click.option("--name", help="The name the deployment.", required=True)
@click.option("--function", help="The name the entry point and function.", required=True)
@click.pass_obj
def create_deployment(_: GatewayServiceStub, name: str, function: str):
    current_dir = os.getcwd()
    if current_dir not in sys.path:
        sys.path.insert(0, current_dir)

    module_path, func_name = function.split(":")
    module_name = module_path.replace(".py", "").replace(os.path.sep, ".")
    module = importlib.import_module(module_name)

    func = getattr(module, func_name)
    if not func.deploy(name=name):
        terminal.error("Deployment failed ☠️")
