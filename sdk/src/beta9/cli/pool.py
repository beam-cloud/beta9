import json
import shlex
import subprocess
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
from ..clients.gateway import (
    GetHybridPoolJoinCommandRequest,
    HybridPoolConfig,
    ListPoolsRequest,
    ListPoolsResponse,
    StringList,
    UpsertHybridPoolRequest,
)
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


def _hybrid_pool_config(
    name: str,
    gpu: tuple[str, ...] = (),
    gpus: int = 0,
    priority: int = 1000,
    transport: str = "tsnet-restricted",
    fallback: str = "internal",
) -> HybridPoolConfig:
    return HybridPoolConfig(
        name=name,
        gpu=list(gpu),
        gpus=gpus or 0,
        selector=name,
        mode="hybrid",
        transport=transport.replace("-", "_"),
        fallback=fallback,
        priority=priority,
    )


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


@management.command(
    name="upsert",
    help="Create or update a private hybrid worker pool.",
    epilog="""
    Examples:

      # Create a private GPU pool that prefers attached H100 machines
      {cli_name} pool upsert private-gpu --mode hybrid --gpu H100 --priority 1000
      \b
    """,
)
@click.argument("name")
@click.option("--mode", type=click.Choice(("hybrid",)), default="hybrid", show_default=True)
@click.option("--gpu", "gpu", multiple=True, help="GPU type accepted by this private pool.")
@click.option("--gpus", type=click.IntRange(0), default=0, help="Optional desired pool GPU capacity.")
@click.option("--priority", type=int, default=1000, show_default=True)
@click.option(
    "--transport",
    type=click.Choice(("tsnet-restricted", "tsnet_restricted", "local-direct", "local_direct")),
    default="tsnet-restricted",
    show_default=True,
)
@click.option(
    "--fallback",
    type=click.Choice(("internal", "wait", "fail")),
    default="internal",
    show_default=True,
)
@extraclick.pass_service_client
def upsert(
    service: ServiceClient,
    name: str,
    mode: str,
    gpu: tuple[str, ...],
    gpus: int,
    priority: int,
    transport: str,
    fallback: str,
):
    if mode != "hybrid":
        return terminal.error("Only hybrid pools can be upserted from the CLI.")

    res = service.gateway.upsert_hybrid_pool(
        UpsertHybridPoolRequest(
            pool=_hybrid_pool_config(
                name=name,
                gpu=gpu,
                gpus=gpus,
                priority=priority,
                transport=transport,
                fallback=fallback,
            )
        )
    )
    if not res.ok:
        return terminal.error(res.err_msg)
    terminal.success(f"Upserted hybrid pool '{res.pool.name}'")


@management.command(
    name="join-command",
    help="Print the one-command installer for a private hybrid worker pool.",
)
@click.argument("name")
@click.option("--ttl", default="30m", show_default=True, help="Join token lifetime.")
@extraclick.pass_service_client
def join_command(service: ServiceClient, name: str, ttl: str):
    res = service.gateway.get_hybrid_pool_join_command(
        GetHybridPoolJoinCommandRequest(pool_name=name, ttl=ttl)
    )
    if not res.ok:
        return terminal.error(res.err_msg)
    terminal.detail(res.command, crop=False, overflow="ignore")


@management.command(
    name="join",
    help="Join this machine to a private hybrid worker pool.",
    epilog="""
    Examples:

      # Join this machine to a private pool
      {cli_name} pool join private-gpu

      # Print the command without running it
      {cli_name} pool join private-gpu --print-only
      \b
    """,
)
@click.argument("name")
@click.option("--ttl", default="30m", show_default=True, help="Join token lifetime.")
@click.option("--gpu", "gpu", multiple=True, help="GPU type accepted by this private pool.")
@click.option("--gpus", type=click.IntRange(0), default=0, help="Optional desired pool GPU capacity.")
@click.option("--priority", type=int, default=1000, show_default=True)
@click.option(
    "--fallback",
    type=click.Choice(("internal", "wait", "fail")),
    default="internal",
    show_default=True,
)
@click.option(
    "--transport",
    type=click.Choice(("auto", "tsnet-restricted", "tsnet_restricted", "local-direct", "local_direct")),
    default="auto",
    show_default=True,
)
@click.option("--listen", default="", help="Agent listener address, for example 0.0.0.0:0.")
@click.option("--advertise-host", default="", help="Host the gateway should dial for this machine.")
@click.option("--agent-bin", default="", help="Use a specific local beam-agent binary.")
@click.option("--print-only", is_flag=True, help="Only print the generated join command.")
@extraclick.pass_service_client
def join(
    service: ServiceClient,
    name: str,
    ttl: str,
    gpu: tuple[str, ...],
    gpus: int,
    priority: int,
    fallback: str,
    transport: str,
    listen: str,
    advertise_host: str,
    agent_bin: str,
    print_only: bool,
):
    transport = _join_transport(service, transport)
    res = service.gateway.upsert_hybrid_pool(
        UpsertHybridPoolRequest(
            pool=_hybrid_pool_config(
                name=name,
                gpu=gpu,
                gpus=gpus,
                priority=priority,
                transport=transport,
                fallback=fallback,
            )
        )
    )
    if not res.ok:
        return terminal.error(res.err_msg)

    command_res = service.gateway.get_hybrid_pool_join_command(
        GetHybridPoolJoinCommandRequest(pool_name=name, ttl=ttl)
    )
    if not command_res.ok:
        return terminal.error(command_res.err_msg)

    command = _append_join_args(
        command_res.command,
        listen=listen,
        advertise_host=advertise_host,
        agent_bin=agent_bin,
    )
    terminal.detail(command, crop=False, overflow="ignore")
    if print_only:
        return

    raise SystemExit(subprocess.call(command, shell=True))


def _join_transport(service: ServiceClient, transport: str) -> str:
    if transport != "auto":
        return transport
    return "tsnet_restricted"


def _append_join_args(command: str, listen: str = "", advertise_host: str = "", agent_bin: str = "") -> str:
    extra = []
    if listen:
        extra.extend(["--listen", listen])
    if advertise_host:
        extra.extend(["--advertise-host", advertise_host])
    if agent_bin:
        extra.extend(["--agent-bin", agent_bin])
    if not extra:
        return command
    return command + " " + " ".join(shlex.quote(value) for value in extra)
