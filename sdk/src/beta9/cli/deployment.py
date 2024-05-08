import importlib
import os
import sys
from pathlib import Path
from typing import Dict

import click
from betterproto import Casing
from rich.table import Column, Table, box

from .. import aio, terminal
from ..channel import ServiceClient
from ..cli import extraclick
from ..clients.gateway import ListDeploymentsRequest, ListDeploymentsResponse, StringList
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


@management.command(
    name="list",
    help="List all deployments.",
    epilog="""
    Examples:

      # List the first 10 deployments
      {cli_name} deployment list --limit 10

      # List deployments that are at version 9
      {cli_name} deployment list --filter version=9

      # List deployments that are not active
      {cli_name} deployment list --filter active=false

      # List deployments and output in JSON format
      {cli_name} deployment list --format json
      \b
    """,
)
@click.option(
    "--limit",
    type=click.IntRange(1, 100),
    default=20,
    help="The number of deployments to fetch.",
)
@click.option(
    "--format",
    type=click.Choice(("table", "json")),
    default="table",
    show_default=True,
    help="Change the format of the output.",
)
@click.option(
    "--filter",
    multiple=True,
    callback=extraclick.filter_values_callback,
    help="Filters deployments. Add this option for each field you want to filter on.",
)
@extraclick.pass_service_client
def list_deployments(
    service: ServiceClient,
    limit: int,
    format: str,
    filter: Dict[str, StringList],
):
    res: ListDeploymentsResponse
    res = aio.run_sync(service.gateway.list_deployments(ListDeploymentsRequest(filter, limit)))

    if not res.ok:
        terminal.error(res.err_msg)

    if format == "json":
        deployments = [d.to_dict(casing=Casing.SNAKE) for d in res.deployments]  # type:ignore
        terminal.print_json(deployments)
        return

    table = Table(
        Column("ID"),
        Column("Name"),
        Column("Active"),
        Column("Version", justify="right"),
        Column("Created At"),
        Column("Updated At"),
        Column("Stub Name"),
        Column("Workspace Name"),
        box=box.SIMPLE,
    )

    for deployment in res.deployments:
        table.add_row(
            deployment.id,
            deployment.name,
            "Yes" if deployment.active else "No",
            str(deployment.version),
            terminal.humanize_date(deployment.created_at),
            terminal.humanize_date(deployment.updated_at),
            deployment.stub_name,
            deployment.workspace_name,
        )

    table.add_section()
    table.add_row(f"[bold]{len(res.deployments)} items")
    terminal.print(table)
