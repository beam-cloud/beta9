import datetime
from typing import List

import click
from betterproto import Casing
from rich.table import Column, Table, box

from .. import terminal
from ..abstractions.base.container import Container
from ..channel import ServiceClient
from ..cli import extraclick
from ..clients.gateway import (
    ListContainersRequest,
    StopContainerRequest,
    StopContainerResponse,
)
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
    "stub_id": "Stub ID",
    "deployment_id": "Deployment ID",
    "scheduled_at": "Scheduled At",
    "uptime": "Uptime",
    "worker_id": "Worker ID",
    "machine_id": "Machine ID",
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
    default="container_id,status,stub_id,scheduled_at,deployment_id,uptime",
    help="""
      Specify columns to display.
      Available columns: container_id, status, stub_id, scheduled_at, deployment_id, uptime
    """,
)
@extraclick.pass_service_client
@click.pass_context
def list_containers(ctx: click.Context, service: ServiceClient, format: str, columns: str):
    res = service.gateway.list_containers(ListContainersRequest())
    if not res.ok:
        terminal.error(res.error_msg)

    now = datetime.datetime.now(datetime.timezone.utc)
    if format == "json":
        containers = []
        for c in res.containers:
            container_dict = c.to_dict(casing=Casing.SNAKE)
            container_dict["uptime"] = (
                terminal.humanize_duration(now - c.started_at) if c.started_at else "N/A"
            )
            containers.append(container_dict)
        terminal.print_json(containers)
        return

    user_requested_columns = set(columns.split(","))

    # If admin columns are present on every container, include them.
    add_admin_columns = all(c.worker_id for c in res.containers)
    if add_admin_columns:
        user_requested_columns.update(["worker_id", "machine_id"])

    # Build the ordered list of columns based on the ordering of AVAILABLE_LIST_COLUMNS
    ordered_columns = [
        col for col in AVAILABLE_LIST_COLUMNS.keys() if col in user_requested_columns
    ]

    table_cols = [Column(AVAILABLE_LIST_COLUMNS[col]) for col in ordered_columns]

    if len(res.containers) == 0:
        terminal.print("No containers found.")
        return

    table = Table(*table_cols, box=box.SIMPLE)
    for container in res.containers:
        row = []
        for col in ordered_columns:
            if col == "uptime":
                value = (
                    terminal.humanize_duration(now - container.started_at)
                    if container.started_at
                    else "N/A"
                )
            else:
                value = getattr(container, col)
                if isinstance(value, datetime.datetime):
                    value = terminal.humanize_date(value)
            row.append(value)
        table.add_row(*row)

    table.add_section()
    table.add_row(f"[bold]{len(res.containers)} items")
    terminal.print(table)


@management.command(
    name="stop",
    help="Stop a container.",
)
@click.argument(
    "container_ids",
    nargs=-1,
    required=True,
)
@extraclick.pass_service_client
def stop_container(service: ServiceClient, container_ids: List[str]):
    for container_id in container_ids:
        res: StopContainerResponse
        res = service.gateway.stop_container(StopContainerRequest(container_id=container_id))

        if res.ok:
            terminal.success(f"Stopped container: {container_id}")
        else:
            terminal.error(f"{res.error_msg}", exit=False)


@management.command(
    name="attach",
    help="Attach to a running container.",
)
@click.argument(
    "container_id",
    required=True,
)
@extraclick.pass_service_client
def attach_to_container(_: ServiceClient, container_id: str):
    container = Container(container_id=container_id)
    container.attach(container_id=container_id)
