import importlib
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

import click
from betterproto import Casing
from rich.table import Column, Table, box

from .. import terminal
from ..abstractions.mixins import DeployableMixin
from ..channel import ServiceClient
from ..cli import extraclick
from ..clients.gateway import (
    DeleteDeploymentRequest,
    DeleteDeploymentResponse,
    ListDeploymentsRequest,
    ListDeploymentsResponse,
    StartDeploymentRequest,
    StartDeploymentResponse,
    StopDeploymentRequest,
    StopDeploymentResponse,
    StringList,
)
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
    required=False,
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
@click.pass_context
def deploy(ctx: click.Context, name: str, entrypoint: str, url_type: str):
    ctx.invoke(create_deployment, name=name, entrypoint=entrypoint, url_type=url_type)


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
    help="The name of the deployment.",
    required=False,
)
@click.option(
    "--entrypoint",
    "-e",
    help='The name the entrypoint e.g. "file:function".',
    required=True,
)
@click.option(
    "--url-type",
    help="The type of URL to get back. [default is determined by the server] ",
    type=click.Choice(["host", "path"]),
)
@extraclick.pass_service_client
def create_deployment(service: ServiceClient, name: str, entrypoint: str, url_type: str):
    current_dir = os.getcwd()
    if current_dir not in sys.path:
        sys.path.insert(0, current_dir)

    module_path, obj_name, *_ = entrypoint.split(":") if ":" in entrypoint else (entrypoint, "")
    module_name = module_path.replace(".py", "").replace(os.path.sep, ".")

    if not Path(module_path).exists():
        terminal.error(f"Unable to find file: '{module_path}'")

    if not obj_name:
        terminal.error(
            "Invalid handler function specified. Expected format: beam deploy [file.py]:[function]"
        )

    module = importlib.import_module(module_name)

    user_obj: Optional[DeployableMixin] = getattr(module, obj_name, None)
    if user_obj is None:
        terminal.error(
            f"Invalid handler function specified. Make sure '{module_path}' contains the function: '{obj_name}'"
        )

    if hasattr(user_obj, "set_handler"):
        user_obj.set_handler(f"{module_name}:{obj_name}")

    if not user_obj.deploy(name=name, context=service._config, url_type=url_type):  # type: ignore
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
      {cli_name} deployment list --filter active=no

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
    res = service.gateway.list_deployments(ListDeploymentsRequest(filter, limit))

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


@management.command(
    name="stop",
    help="Stop deployments.",
    epilog="""
    Examples:

      # Stop a deployment
      {cli_name} deployment stop 5bd2e248-6d7c-417b-ac7b-0b92aa0a5572

      # Stop multiple deployments
      {cli_name} deployment stop 5bd2e248-6d7c-417b-ac7b-0b92aa0a5572 7b968ad5-c001-4df3-ba05-e99895aa9596
      \b
    """,
)
@click.argument(
    "deployment_ids",
    nargs=-1,
    type=click.STRING,
    required=True,
)
@extraclick.pass_service_client
def stop_deployments(service: ServiceClient, deployment_ids: List[str]):
    for id in deployment_ids:
        res: StopDeploymentResponse
        res = service.gateway.stop_deployment(StopDeploymentRequest(id))

        if not res.ok:
            terminal.error(res.err_msg, exit=False)
            continue

        terminal.print(f"Stopped {id}")


@management.command(
    name="start",
    help="Start an inactive deployment.",
    epilog="""
    Examples:

        # Start a deployment
        {cli_name} deployment start 5bd2e248-6d7c-417b-ac7b-0b92aa0a5572
        """,
)
@click.argument(
    "deployment_id",
    type=click.STRING,
    required=True,
)
@extraclick.pass_service_client
def start_deployment(service: ServiceClient, deployment_id: str):
    res: StartDeploymentResponse
    res = service.gateway.start_deployment(StartDeploymentRequest(id=deployment_id))

    if not res.ok:
        terminal.error(res.err_msg)

    terminal.print(f"Starting deployment: {deployment_id}")


@management.command(
    name="delete",
    help="Delete a deployment.",
    epilog="""
    Examples:

        # Delete a deployment
        {cli_name} deployment delete 5bd2e248-6d7c-417b-ac7b-0b92aa0a5572
        \b
     """,
)
@click.argument(
    "deployment_id",
    nargs=1,
    type=click.STRING,
    required=True,
)
@extraclick.pass_service_client
def delete_deployment(service: ServiceClient, deployment_id: str):
    res: DeleteDeploymentResponse
    res = service.gateway.delete_deployment(DeleteDeploymentRequest(deployment_id))

    if not res.ok:
        terminal.error(res.err_msg)

    terminal.print(f"Deleted {deployment_id}")
