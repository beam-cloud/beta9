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
from rich.table import Column, Table, box
from rich.text import Text

from .. import terminal
from ..channel import ServiceClient
from ..cli import extraclick
from ..clients.gateway import (
    CreatePoolRequest,
    DeletePoolRequest,
    ExtendPoolCapacityRequest,
    GetPoolJoinCommandRequest,
    LaunchPoolCapacityRequest,
    ListPoolMachinesRequest,
    ListPoolOffersRequest,
    ListPoolsRequest,
    ListPoolsResponse,
    ListPrivatePoolsRequest,
    PoolConfig,
    StringList,
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


def _pool_config(
    name: str,
    gpu: tuple[str, ...] = (),
    gpus: int = 0,
    ttl: str = "",
    max_spend: float = 0,
    provider: tuple[str, ...] = (),
    region: tuple[str, ...] = (),
    min_reliability: float = 0,
    priority: int = 1000,
    transport: str = "tsnet-restricted",
    fallback: str = "internal",
) -> PoolConfig:
    return PoolConfig(
        name=name,
        gpu=list(gpu),
        gpus=gpus or 0,
        ttl=ttl or "",
        max_spend=max_spend or 0,
        providers=list(provider),
        regions=list(region),
        min_reliability=min_reliability or 0,
        reservation_required=bool(
            gpus or ttl or max_spend or provider or region or min_reliability
        ),
        selector=name,
        mode="private",
        transport=transport.replace("-", "_"),
        fallback=fallback,
        priority=priority,
    )


def _get_pool_renderable(
    service: ServiceClient,
    limit: int,
    output_format: str,
    filters: Dict[str, StringList],
) -> Any:
    """
    Returns a Rich renderable for the pool list
    """
    res: ListPoolsResponse = service.gateway.list_pools(
        ListPoolsRequest(filters, limit)
    )
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
                (
                    "Default GPU Count (per worker):",
                    pool.default_worker_gpu_count or "0",
                ),
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
    name="create",
    help="Create a private compute pool.",
    epilog="""
    Examples:

      # Create a private GPU pool that prefers attached H100 machines
      {cli_name} pool create private-gpu --mode private --gpu H100 --priority 1000
      \b
    """,
)
@click.argument("name")
@click.option(
    "--mode", type=click.Choice(("private",)), default="private", show_default=True
)
@click.option(
    "--gpu", "gpu", multiple=True, help="GPU type accepted by this private pool."
)
@click.option(
    "--gpus",
    type=click.IntRange(0),
    default=0,
    help="Optional desired pool GPU capacity.",
)
@click.option("--priority", type=int, default=1000, show_default=True)
@click.option(
    "--transport",
    type=click.Choice(("tsnet-restricted", "tsnet_restricted")),
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
def create(
    service: ServiceClient,
    name: str,
    mode: str,
    gpu: tuple[str, ...],
    gpus: int,
    priority: int,
    transport: str,
    fallback: str,
):
    if mode != "private":
        return terminal.error("Only private pools can be created from the CLI.")

    res = service.gateway.create_pool(
        CreatePoolRequest(
            pool=_pool_config(
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
    terminal.success(f"Created private pool '{res.pool.name}'")


@management.command(
    name="offers", help="List compatible launch offers for a private pool."
)
@click.argument("name")
@click.option(
    "--gpu", "gpu", multiple=True, required=True, help="GPU type to search for."
)
@click.option("--gpus", type=click.IntRange(1), default=1, show_default=True)
@click.option("--ttl", default="1h", show_default=True)
@click.option("--max-spend", type=float, default=0)
@click.option(
    "--provider", multiple=True, help="Restrict to a vendor such as vast or shadeform."
)
@click.option("--region", multiple=True, help="Restrict to a region.")
@click.option("--min-reliability", type=click.FloatRange(0, 1), default=0)
@click.option(
    "--format", type=click.Choice(("table", "json")), default="table", show_default=True
)
@extraclick.pass_service_client
def offers(
    service: ServiceClient,
    name: str,
    gpu: tuple[str, ...],
    gpus: int,
    ttl: str,
    max_spend: float,
    provider: tuple[str, ...],
    region: tuple[str, ...],
    min_reliability: float,
    format: str,
):
    res = service.gateway.list_pool_offers(
        ListPoolOffersRequest(
            pool=_pool_config(
                name=name,
                gpu=gpu,
                gpus=gpus,
                ttl=ttl,
                max_spend=max_spend,
                provider=provider,
                region=region,
                min_reliability=min_reliability,
            )
        )
    )
    if not res.ok:
        return terminal.error(res.err_msg)
    if format == "json":
        terminal.print_json(
            {"offers": [o.to_dict(casing=Casing.SNAKE) for o in res.offers]}
        )
        return

    table = Table(
        Column("Provider"),
        Column("GPU"),
        Column("Count", justify="right"),
        Column("Instance"),
        Column("Region"),
        Column("$/hr", justify="right"),
        Column("Available", justify="right"),
        box=box.SIMPLE,
    )
    for offer in res.offers:
        table.add_row(
            offer.provider,
            offer.gpu,
            str(offer.gpu_count),
            offer.instance_type,
            offer.region or "-",
            f"{offer.hourly_cost_micros / 1_000_000:.4f}",
            str(offer.available),
        )
    terminal.print(table)


@management.command(
    name="launch", help="Launch provider-backed capacity for a private pool."
)
@click.argument("name")
@click.option("--gpu", "gpu", multiple=True, required=True)
@click.option("--gpus", type=click.IntRange(1), required=True)
@click.option("--ttl", required=True)
@click.option("--max-spend", type=float, required=True)
@click.option("--provider", multiple=True)
@click.option("--region", multiple=True)
@click.option("--min-reliability", type=click.FloatRange(0, 1), default=0)
@extraclick.pass_service_client
def launch(
    service: ServiceClient,
    name: str,
    gpu: tuple[str, ...],
    gpus: int,
    ttl: str,
    max_spend: float,
    provider: tuple[str, ...],
    region: tuple[str, ...],
    min_reliability: float,
):
    res = service.gateway.launch_pool_capacity(
        LaunchPoolCapacityRequest(
            pool=_pool_config(
                name=name,
                gpu=gpu,
                gpus=gpus,
                ttl=ttl,
                max_spend=max_spend,
                provider=provider,
                region=region,
                min_reliability=min_reliability,
            )
        )
    )
    if not res.ok:
        return terminal.error(res.err_msg)
    terminal.success(f"Launched capacity for private pool '{res.pool.name}'")


@management.command(name="private", help="List private compute pools.")
@click.option("--limit", type=click.IntRange(1, 100), default=20, show_default=True)
@click.option(
    "--format", type=click.Choice(("table", "json")), default="table", show_default=True
)
@extraclick.pass_service_client
def private_pools(service: ServiceClient, limit: int, format: str):
    res = service.gateway.list_private_pools(ListPrivatePoolsRequest(limit=limit))
    if not res.ok:
        return terminal.error(res.err_msg)
    if format == "json":
        terminal.print_json(
            {"pools": [p.to_dict(casing=Casing.SNAKE) for p in res.pools]}
        )
        return

    table = Table(
        Column("Name"),
        Column("Status"),
        Column("GPUs", justify="right"),
        Column("Spend", justify="right"),
        Column("Source"),
        box=box.SIMPLE,
    )
    for pool in res.pools:
        table.add_row(
            pool.name,
            pool.status,
            str(pool.reserved_gpus),
            f"${pool.committed_spend_micros / 1_000_000:.2f}",
            pool.source,
        )
    terminal.print(table)


@management.command(name="machines", help="List machines joined to a private pool.")
@click.argument("name")
@click.option("--limit", type=click.IntRange(1, 100), default=20, show_default=True)
@click.option(
    "--format", type=click.Choice(("table", "json")), default="table", show_default=True
)
@extraclick.pass_service_client
def machines(service: ServiceClient, name: str, limit: int, format: str):
    res = service.gateway.list_pool_machines(
        ListPoolMachinesRequest(pool_name=name, limit=limit)
    )
    if not res.ok:
        return terminal.error(res.err_msg)
    if format == "json":
        terminal.print_json(
            {"machines": [m.to_dict(casing=Casing.SNAKE) for m in res.machines]}
        )
        return

    table = Table(
        Column("Machine"),
        Column("Status"),
        Column("CPU", justify="right"),
        Column("Memory", justify="right"),
        Column("GPU"),
        Column("GPUs", justify="right"),
        box=box.SIMPLE,
    )
    for machine in res.machines:
        table.add_row(
            machine.id,
            machine.status,
            str(machine.cpu),
            str(machine.memory),
            machine.gpu or "-",
            str(machine.gpu_count),
        )
    terminal.print(table)


@management.command(name="extend", help="Extend private pool capacity.")
@click.argument("name")
@click.option("--ttl", default="")
@click.option("--max-spend", type=float, default=0)
@extraclick.pass_service_client
def extend(service: ServiceClient, name: str, ttl: str, max_spend: float):
    res = service.gateway.extend_pool_capacity(
        ExtendPoolCapacityRequest(name=name, ttl=ttl, max_spend=max_spend)
    )
    if not res.ok:
        return terminal.error(res.err_msg)
    terminal.success(f"Extended private pool '{name}'")


@management.command(name="terminate", help="Terminate and delete a private pool.")
@click.argument("name")
@extraclick.pass_service_client
def terminate(service: ServiceClient, name: str):
    res = service.gateway.delete_pool(DeletePoolRequest(name=name))
    if not res.ok:
        return terminal.error(res.err_msg)
    terminal.success(f"Terminated private pool '{name}'")


@management.command(name="delete", help="Delete a private pool.")
@click.argument("name")
@extraclick.pass_service_client
def delete(service: ServiceClient, name: str):
    res = service.gateway.delete_pool(DeletePoolRequest(name=name))
    if not res.ok:
        return terminal.error(res.err_msg)
    terminal.success(f"Deleted private pool '{name}'")


@management.command(
    name="join-command",
    help="Print the one-command installer for a private agent pool.",
)
@click.argument("name")
@click.option("--ttl", default="30m", show_default=True, help="Join token lifetime.")
@extraclick.pass_service_client
def join_command(service: ServiceClient, name: str, ttl: str):
    res = service.gateway.get_pool_join_command(
        GetPoolJoinCommandRequest(pool_name=name, ttl=ttl)
    )
    if not res.ok:
        return terminal.error(res.err_msg)
    terminal.detail(res.command, crop=False, overflow="ignore")


@management.command(
    name="join",
    help="Join this machine to a private agent pool.",
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
@click.option(
    "--gpu", "gpu", multiple=True, help="GPU type accepted by this private pool."
)
@click.option(
    "--gpus",
    type=click.IntRange(0),
    default=0,
    help="Optional desired pool GPU capacity.",
)
@click.option("--priority", type=int, default=1000, show_default=True)
@click.option(
    "--fallback",
    type=click.Choice(("internal", "wait", "fail")),
    default="internal",
    show_default=True,
)
@click.option(
    "--transport",
    type=click.Choice(("auto", "tsnet-restricted", "tsnet_restricted")),
    default="auto",
    show_default=True,
)
@click.option("--agent-bin", default="", help="Use a specific local beam-agent binary.")
@click.option(
    "--executor",
    type=click.Choice(("worker-container", "local-dev")),
    default=None,
    help="Override the agent executor returned by preflight.",
)
@click.option(
    "--worker-image", default="", help="Worker image for the worker-container executor."
)
@click.option(
    "--max-cpu", default="", help="Maximum CPU cores to advertise from this machine."
)
@click.option(
    "--max-memory", default="", help="Maximum memory to advertise, for example 32Gi."
)
@click.option(
    "--max-gpus", type=click.IntRange(0), default=0, help="Maximum GPUs to advertise."
)
@click.option("--gpu-ids", default="", help="Comma-separated GPU device IDs to expose.")
@click.option(
    "--network-slots",
    type=click.IntRange(0),
    default=0,
    help="Preallocated container network slots.",
)
@click.option(
    "--container-start-concurrency",
    type=click.IntRange(0),
    default=0,
    help="Maximum concurrent container starts.",
)
@click.option(
    "--background/--foreground",
    default=None,
    help="Install the agent as a background service or run it in the foreground.",
)
@click.option(
    "--service-manager",
    type=click.Choice(("auto", "systemd", "launchd")),
    default=None,
    help="Service manager to use for background installs.",
)
@click.option("--service-name", default="", help="Background service name.")
@click.option("--state-dir", default="", help="Agent state directory.")
@click.option(
    "--print-only", is_flag=True, help="Only print the generated join command."
)
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
    agent_bin: str,
    executor: str | None,
    worker_image: str,
    max_cpu: str,
    max_memory: str,
    max_gpus: int,
    gpu_ids: str,
    network_slots: int,
    container_start_concurrency: int,
    background: bool | None,
    service_manager: str | None,
    service_name: str,
    state_dir: str,
    print_only: bool,
):
    if gpu_ids and max_gpus:
        return terminal.error("--gpu-ids and --max-gpus cannot both be set.")

    if not print_only:
        terminal.header("Joining pool", name)
    transport = _join_transport(service, transport)
    res = service.gateway.create_pool(
        CreatePoolRequest(
            pool=_pool_config(
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

    command_res = service.gateway.get_pool_join_command(
        GetPoolJoinCommandRequest(pool_name=name, ttl=ttl)
    )
    if not command_res.ok:
        return terminal.error(command_res.err_msg)

    command = _append_join_args(
        command_res.command,
        agent_bin=agent_bin,
        executor=executor,
        worker_image=worker_image,
        max_cpu=max_cpu,
        max_memory=max_memory,
        max_gpus=max_gpus,
        gpu_ids=gpu_ids,
        network_slots=network_slots,
        container_start_concurrency=container_start_concurrency,
        background=background,
        service_manager=service_manager,
        service_name=service_name,
        state_dir=state_dir,
    )
    if print_only:
        terminal.detail(command, crop=False, overflow="ignore")
        return

    exit_code = subprocess.call(command, shell=True)
    if exit_code == 0:
        terminal.success("Agent is running.")
    raise SystemExit(exit_code)


def _join_transport(service: ServiceClient, transport: str) -> str:
    if transport != "auto":
        return transport
    return "tsnet_restricted"


def _append_join_args(
    command: str,
    agent_bin: str = "",
    executor: str = "",
    worker_image: str = "",
    max_cpu: str = "",
    max_memory: str = "",
    max_gpus: int = 0,
    gpu_ids: str = "",
    network_slots: int = 0,
    container_start_concurrency: int = 0,
    background: bool | None = None,
    service_manager: str | None = None,
    service_name: str = "",
    state_dir: str = "",
) -> str:
    extra = []
    if background is True:
        extra.append("--background")
    elif background is False:
        extra.append("--foreground")
    if service_manager:
        extra.extend(["--service-manager", service_manager])
    if service_name:
        extra.extend(["--service-name", service_name])
    if state_dir:
        extra.extend(["--state-dir", state_dir])
    if agent_bin:
        extra.extend(["--agent-bin", agent_bin])
    if executor:
        extra.extend(["--executor", executor])
    if worker_image:
        extra.extend(["--worker-image", worker_image])
    if max_cpu:
        extra.extend(["--max-cpu", max_cpu])
    if max_memory:
        extra.extend(["--max-memory", max_memory])
    if max_gpus:
        extra.extend(["--max-gpus", str(max_gpus)])
    if gpu_ids:
        extra.extend(["--gpu-ids", gpu_ids])
    if network_slots:
        extra.extend(["--network-slots", str(network_slots)])
    if container_start_concurrency:
        extra.extend(
            ["--container-start-concurrency", str(container_start_concurrency)]
        )
    if not extra:
        return command
    return command + " " + " ".join(shlex.quote(value) for value in extra)
