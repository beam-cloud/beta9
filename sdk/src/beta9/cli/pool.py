import json
import time
from typing import Any, Dict, List, Tuple

import click
from betterproto import Casing
from rich.columns import Columns
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

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


def _create_table(rows: List[Tuple[str, str]]) -> Table:
    table = Table(show_header=False, box=None, expand=True)
    for label, value in rows:
        table.add_row(label, value)
    return table


def _get_pool_renderable(
    service: ServiceClient, limit: int, output_format: str, filters: Dict[str, StringList]
) -> Any:
    """
    Returns a Rich renderable for the pool list
    """
    res: ListPoolsResponse = service.gateway.list_pools(ListPoolsRequest(filters, limit))
    if not res.ok:
        return Text(f"[red]{res.err_msg}")

    if output_format == "json":
        pools = [d.to_dict(casing=Casing.SNAKE) for d in res.pools]  # type: ignore
        return Text(json.dumps(pools, indent=2))
    else:
        name_filters = filters.get("name", StringList())
        pool_cards = []
        for pool in res.pools:
            if name_filters.values and pool.name not in name_filters.values:
                continue

            # Create pool config table
            config_rows = [
                ("GPU:", pool.gpu),
                ("Minimum Free GPU:", pool.min_free_gpu or "0"),
                ("Minimum Free CPU:", pool.min_free_cpu or "0"),
                ("Minimum Free Memory:", pool.min_free_memory or "0"),
                ("Default GPU Count (per worker):", pool.default_worker_gpu_count or "0"),
            ]
            config_table = _create_table(config_rows)

            # Create pool state table
            state_rows = [
                ("Status:", pool.state.status),
                ("Scheduling Latency (ms):", str(pool.state.scheduling_latency)),
                ("Free GPU:", str(pool.state.free_gpu)),
                ("Free CPU (millicores):", str(pool.state.free_cpu)),
                ("Free Memory (MB):", str(pool.state.free_memory)),
                ("Pending Workers:", str(pool.state.pending_workers)),
                ("Available Workers:", str(pool.state.available_workers)),
                ("Pending Containers:", str(pool.state.pending_containers)),
                ("Running Containers:", str(pool.state.running_containers)),
                ("Registered Machines:", str(pool.state.registered_machines)),
                ("Pending Machines:", str(pool.state.pending_machines)),
            ]
            state_table = _create_table(state_rows)

            content = Columns([config_table, state_table], equal=True, expand=True)
            card = Panel(content, title=pool.name, border_style="blue")
            pool_cards.append(card)

        return Columns(pool_cards, expand=True)


@management.command(
    name="list",
    help="List all worker pools.",
    epilog="""
    Examples:

      # List the first 10 pools
      {cli_name} pool list --limit 10

      # List pools and output in JSON format
      {cli_name} pool list --format json
      
      # Continuously refresh and show pool information every 1 second (default)
      {cli_name} pool list --watch

      # Continuously refresh every 2 seconds
      {cli_name} pool list --watch --period 2
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
@click.option(
    "-w",
    "--watch",
    is_flag=True,
    default=False,
    help="Periodically refreshes the output automatically and re-renders the data.",
)
@click.option(
    "--period",
    "-p",
    type=click.FloatRange(min=0.1),
    default=1.0,
    show_default=True,
    help="Refresh interval (in seconds) when in watch mode.",
)
@extraclick.pass_service_client
def list_pools(
    service: ServiceClient,
    limit: int,
    format: str,
    filter: Dict[str, StringList],
    watch: bool,
    period: float,
):
    """
    List all worker pools
    """
    if not watch:
        terminal.print(_get_pool_renderable(service, limit, format, filter))
        return

    with Live(
        _get_pool_renderable(service, limit, format, filter),
        console=terminal._console,
        screen=True,
        refresh_per_second=1 / period,
    ) as live:
        try:
            while True:
                live.update(_get_pool_renderable(service, limit, format, filter))
                time.sleep(period)
        except KeyboardInterrupt:
            pass
