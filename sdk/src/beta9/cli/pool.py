from typing import Dict

import click
from betterproto import Casing
from rich.table import Table

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

    from rich.columns import Columns
    from rich.panel import Panel

    pool_cards = []
    for pool in res.pools:
        # Config table for pool-specific settings
        config_table = Table(show_header=False, box=None, expand=True)
        config_table.add_row("GPU:", pool.gpu)
        config_table.add_row("Minimum Free GPU:", pool.min_free_gpu or "0")
        config_table.add_row("Minimum Free CPU:", pool.min_free_cpu or "0")
        config_table.add_row("Minimum Free Memory:", pool.min_free_memory or "0")
        config_table.add_row(
            "Default GPU Count (per worker):", pool.default_worker_gpu_count or "0"
        )

        # Pool state table for dynamic health info
        state_table = Table(show_header=False, box=None, expand=True)
        state_table.add_row("Status:", pool.state.status)
        state_table.add_row("Scheduling Latency:", str(pool.state.scheduling_latency))
        state_table.add_row("Free GPU:", str(pool.state.free_gpu))
        state_table.add_row("Free CPU:", str(pool.state.free_cpu))
        state_table.add_row("Free Memory:", str(pool.state.free_memory))
        state_table.add_row("Pending Workers:", str(pool.state.pending_workers))
        state_table.add_row("Available Workers:", str(pool.state.available_workers))
        state_table.add_row("Pending Containers:", str(pool.state.pending_containers))
        state_table.add_row("Running Containers:", str(pool.state.running_containers))
        state_table.add_row("Registered Machines:", str(pool.state.registered_machines))
        state_table.add_row("Pending Machines:", str(pool.state.pending_machines))

        content = Columns([config_table, state_table], equal=True, expand=True)
        card = Panel(content, title=pool.name, border_style="blue")
        pool_cards.append(card)

    terminal.print(Columns(pool_cards, expand=True))
