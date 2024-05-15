from typing import Dict

import click
from betterproto import Casing
from rich.table import Column, Table, box

from .. import terminal
from ..channel import ServiceClient
from ..cli import extraclick
from ..clients.gateway import ListPoolsRequest, ListPoolsResponse, StringList
from .extraclick import ClickCommonGroup, ClickManagementGroup


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@click.group(
    name="pool",
    help="Manage worker pools.",
    cls=ClickManagementGroup,
)
def management():
    pass


@management.command(
    name="list",
    help="List all worker pools.",
    epilog="""
    Examples:

      # List the first 10 pools
      {cli_name} pool list --limit 10

      # List pools and output in JSON format
      {cli_name} pool list --format json
      \b
    """,
)
@click.option(
    "--limit",
    type=click.IntRange(1, 100),
    default=20,
    help="The number of pools to fetch.",
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
    help="Filters pools. Add this option for each field you want to filter on.",
)
@extraclick.pass_service_client
def list_pools(
    service: ServiceClient,
    limit: int,
    format: str,
    filter: Dict[str, StringList],
):
    res: ListPoolsResponse
    res = service.gateway.list_pools(ListPoolsRequest(filter, limit))

    if not res.ok:
        terminal.error(res.err_msg)

    if format == "json":
        pools = [d.to_dict(casing=Casing.SNAKE) for d in res.pools]  # type:ignore
        terminal.print_json(pools)
        return

    table = Table(
        Column("Name"),
        Column("GPU"),
        Column("Minimum Free GPU"),
        Column("Minimim Free CPU"),
        Column("Minimum Free Memory"),
        Column("Default GPU Count (per worker)"),
        box=box.SIMPLE,
    )

    for pool in res.pools:
        table.add_row(
            pool.name,
            pool.gpu,
            pool.min_free_gpu or "0",
            pool.min_free_cpu or "0",
            pool.min_free_memory or "0",
            pool.default_worker_gpu_count or "0",
        )

    table.add_section()
    table.add_row(f"[bold]{len(res.pools)} items")
    terminal.print(table)
