import click
from betterproto import Casing
from rich.table import Column, Table, box

from .. import terminal
from ..channel import ServiceClient
from ..cli import extraclick
from ..clients.gateway import (
    ListDeploymentsRequest,
    ListDeploymentsResponse,
)
from .extraclick import ClickManagementGroup


@click.group(
    name="deployment",
    help="Manage deployments.",
    cls=ClickManagementGroup,
)
def management():
    pass


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
@extraclick.pass_service_client
def list_deployments(
    service: ServiceClient,
    limit: int,
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
