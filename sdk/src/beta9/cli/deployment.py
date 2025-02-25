from typing import Dict, List

import click
from betterproto import Casing
from rich.table import Column, Table, box

from .. import terminal
from ..channel import ServiceClient
from ..cli import extraclick
from ..clients.gateway import (
    DeleteDeploymentRequest,
    DeleteDeploymentResponse,
    ListDeploymentsRequest,
    ListDeploymentsResponse,
    ScaleDeploymentRequest,
    ScaleDeploymentResponse,
    StartDeploymentRequest,
    StartDeploymentResponse,
    StopDeploymentRequest,
    StopDeploymentResponse,
    StringList,
)
from ..utils import load_module_spec
from .extraclick import (
    ClickCommonGroup,
    ClickManagementGroup,
    handle_config_override,
    override_config_options,
)


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@common.command(
    name="deploy",
    help="""
    Deploy a new function.

    HANDLER is in the format of "file:function".
    """,
    epilog="""
      Examples:

        {cli_name} deploy --name my-app app.py:my_func

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
    "handler",
    nargs=1,
    required=False,
)
@click.option(
    "--url-type",
    help="The type of URL to get back. [default is determined by the server] ",
    type=click.Choice(["host", "path"]),
)
@override_config_options
@click.pass_context
def deploy(
    ctx: click.Context,
    name: str,
    handler: str,
    url_type: str,
    **kwargs,
):
    ctx.invoke(
        create_deployment,
        name=name,
        handler=handler,
        url_type=url_type,
        **kwargs,
    )


@click.group(
    name="deployment",
    help="Manage deployments.",
    cls=ClickManagementGroup,
)
def management():
    pass


def _generate_pod_module(name: str, entrypoint: str):
    from beta9.abstractions.pod import Pod

    pod = Pod(
        name=name,
        entrypoint=entrypoint,
    )

    return pod


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
    "--handler",
    help='The name the handler e.g. "file:function" or script to run.',
)
@click.option(
    "--url-type",
    help="The type of URL to get back. [default is determined by the server] ",
    type=click.Choice(["host", "path"]),
)
@override_config_options
@extraclick.pass_service_client
def create_deployment(
    service: ServiceClient,
    name: str,
    handler: str,
    url_type: str,
    **kwargs,
):
    module = None
    entrypoint = kwargs["entrypoint"]
    if handler:
        user_obj, module_name, obj_name = load_module_spec(handler, "deploy")

        if hasattr(user_obj, "set_handler"):
            user_obj.set_handler(f"{module_name}:{obj_name}")

    elif entrypoint:
        user_obj = _generate_pod_module(name, entrypoint)

    else:
        terminal.error("No handler or entrypoint specified")
        return

    if not handle_config_override(user_obj, kwargs):
        return

    if not module and hasattr(user_obj, "generate_deployment_artifacts"):
        user_obj.generate_deployment_artifacts(**kwargs)

    if not user_obj.deploy(name=name, context=service._config, url_type=url_type):  # type: ignore
        terminal.error("Deployment failed ☠️")

    if not module and hasattr(user_obj, "cleanup_deployment_artifacts"):
        user_obj.cleanup_deployment_artifacts()


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

        terminal.success(f"Stopped deployment: {id}")


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


@management.command(
    name="scale",
    help="Scale an active deployment.",
    epilog="""
    Examples:

        # Start a deployment
        {cli_name} deployment scale 5bd2e248-6d7c-417b-ac7b-0b92aa0a5572 --containers 2
        """,
)
@click.argument(
    "deployment_id",
    type=click.STRING,
    required=True,
)
@click.option(
    "--containers",
    type=click.INT,
    required=True,
    help="The number of containers to scale to.",
)
@extraclick.pass_service_client
def scale_deployment(service: ServiceClient, deployment_id: str, containers: int):
    res: ScaleDeploymentResponse
    res = service.gateway.scale_deployment(
        ScaleDeploymentRequest(id=deployment_id, containers=containers)
    )

    if not res.ok:
        terminal.error(res.err_msg)

    terminal.print(f"Scaled deployment {deployment_id} to {containers} containers")
