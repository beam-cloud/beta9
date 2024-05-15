import datetime

import click
from betterproto import Casing
from rich.table import Column, Table, box

from .. import aio, terminal
from ..channel import ServiceClient
from ..cli import extraclick
from ..clients.gateway import ListContainersRequest
from .extraclick import ClickCommonGroup, ClickManagementGroup


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@click.group(
    name="container",
    help="Manage containers.",
    cls=ClickManagementGroup,
)
def management():
    pass


AVAILABLE_LIST_COLUMNS = {
    "container_id": "ID",
    "status": "Status",
    "stub_id": "Stub Id",
    "scheduled_at": "Scheduled At",
}


@management.command(
    name="list",
    help="""
    List all current containers.
    """,
)
@click.option(
    "--format",
    type=click.Choice(("table", "json")),
    default="table",
    show_default=True,
    help="Change the format of the output.",
)
@click.option(
    "--columns",
    type=click.STRING,
    default="container_id,status,stub_id,scheduled_at",
    help="""
      Specify columns to display.
      Available columns: container_id, status, stub_id, scheduled_at
    """,
)
@extraclick.pass_service_client
@click.pass_context
def list_containers(ctx: click.Context, service: ServiceClient, format: str, columns: str):
    res = aio.run_sync(service.gateway.list_containers(ListContainersRequest()))

    if not res.ok:
        terminal.error(res.err_msg)

    if format == "json":
        deployments = [c.to_dict(casing=Casing.SNAKE) for c in res.containers]  # type:ignore
        terminal.print_json(deployments)
        return

    table_cols = []
    desired_columns = columns.split(",")

    for col in desired_columns:
        if col not in AVAILABLE_LIST_COLUMNS:
            terminal.error(f"Invalid column: {col}")
            return

        table_cols.append(Column(AVAILABLE_LIST_COLUMNS[col]))

    table = Table(
        *table_cols,
        box=box.SIMPLE,
    )

    for container in res.containers:
        cols = []

        for dc in desired_columns:
            val = getattr(container, dc)

            if type(val) == datetime.datetime:
                cols.append(terminal.humanize_date(val))
            else:
                cols.append(val)

        table.add_row(
            *cols,
        )

    table.add_section()
    table.add_row(f"[bold]{len(res.containers)} items")
    terminal.print(table)
