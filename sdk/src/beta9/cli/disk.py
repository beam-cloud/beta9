from typing import Any, Dict, Iterable

import click
from rich.table import Column, Table, box

from .. import terminal
from ..channel import ServiceClient
from ..clients.disk import (
    DeleteDiskRequest,
    DeleteDiskResponse,
    GetOrCreateDiskRequest,
    GetOrCreateDiskResponse,
    ListDisksRequest,
    ListDisksResponse,
)
from . import extraclick
from .extraclick import ClickManagementGroup


@click.group(
    name="disk",
    help="Manage disks (durable, fixed-size block storage on local SSD/NVMe).",
    cls=ClickManagementGroup,
)
def management():
    pass


def _format_option(func):
    return click.option(
        "--format",
        "format",
        type=click.Choice(("table", "json")),
        default="table",
        show_default=True,
        help="Change the format of the output.",
    )(func)


def _disk_payload(disk) -> Dict[str, Any]:
    return {
        "id": disk.id,
        "name": disk.name,
        "size": disk.size,
        "mount_path": disk.mount_path,
        "created_at": disk.created_at,
        "updated_at": disk.updated_at,
        "workspace_id": disk.workspace_id,
        "workspace_name": disk.workspace_name,
    }


def _print_disk_table(disks: Iterable, *, include_count: bool = True) -> None:
    disks = list(disks)
    table = Table(
        Column("Name"),
        Column("Size"),
        Column("Mount Path"),
        Column("Created At"),
        Column("Workspace Name"),
        box=box.SIMPLE,
    )

    for disk in disks:
        table.add_row(
            disk.name,
            disk.size or "-",
            disk.mount_path or "-",
            terminal.humanize_date(disk.created_at),
            disk.workspace_name,
        )

    if include_count:
        table.add_section()
        table.add_row(f"[bold]{len(disks)} disks")
    terminal.print(table)


@management.command(
    name="list",
    help="List available disks.",
)
@_format_option
@extraclick.pass_service_client
def list_disks(service: ServiceClient, format: str):
    res: ListDisksResponse
    res = service.disk.list_disks(ListDisksRequest())

    if not res.ok:
        terminal.error(res.err_msg)

    if format == "json":
        terminal.print_json({"disks": [_disk_payload(disk) for disk in res.disks]})
        return

    _print_disk_table(res.disks)


@management.command(
    name="create",
    help="Create a new disk.",
)
@click.argument(
    "name",
    type=click.STRING,
    required=True,
)
@click.option(
    "--size",
    type=click.STRING,
    default="10Gi",
    help="The size of the disk (e.g. 10Gi).",
)
@click.option(
    "--mount-path",
    type=click.STRING,
    required=True,
    help="The default mount path for the disk.",
)
@_format_option
@extraclick.pass_service_client
def create_disk(
    service: ServiceClient,
    name: str,
    size: str,
    mount_path: str,
    format: str,
):
    res: GetOrCreateDiskResponse
    res = service.disk.get_or_create_disk(
        GetOrCreateDiskRequest(
            name=name,
            size=size,
            mount_path=mount_path,
        )
    )

    if not res.ok:
        terminal.error(res.err_msg)

    if format == "json":
        terminal.print_json(_disk_payload(res.disk))
        return

    _print_disk_table([res.disk], include_count=False)


@management.command(
    name="delete",
    help="Unregister a disk.",
)
@click.argument(
    "name",
    type=click.STRING,
    required=True,
)
@click.option("-y", "--yes", is_flag=True, help="Skip confirmation.")
@extraclick.pass_service_client
def delete_disk(service: ServiceClient, name: str, yes: bool):
    if not yes:
        terminal.warn(
            "Any apps or services (functions, endpoints, databases, etc) that\n"
            "refer to this disk should be updated before it is unregistered.\n"
            "Existing state snapshots keep their immutable generation references."
        )

        if not terminal.confirm("Are you sure?", default=False):
            return

    res: DeleteDiskResponse
    res = service.disk.delete_disk(DeleteDiskRequest(name=name))

    if not res.ok:
        terminal.error(res.err_msg)

    terminal.success(f"Unregistered disk: {name}")
