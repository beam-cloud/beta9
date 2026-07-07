import json
import signal
import shlex
import subprocess
import time
from typing import Any, Dict, List, Optional, Tuple

import click
from betterproto import Casing
from rich.live import Live
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
    Machine,
    ListPoolOffersRequest,
    ListPoolMachinesRequest,
    ListPoolsRequest,
    ListPrivatePoolsRequest,
    PoolOffer,
    Pool as ControlPlanePool,
    PoolConfig,
    PrivatePool,
    StringList,
)
from .extraclick import ClickCommonGroup, ClickManagementGroup
from .machine_format import format_cpu, format_memory, machine_table


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@click.group(
    name="pool",
    help="Manage compute pools.",
    cls=ClickManagementGroup,
)
def management():
    pass


def _pool_config(
    name: str,
    gpu: Tuple[str, ...] = (),
    nodes: int = 0,
    ttl: str = "",
    max_spend: float = 0,
    provider: Tuple[str, ...] = (),
    region: Tuple[str, ...] = (),
    min_reliability: float = 0,
    priority: int = 1000,
    transport: str = "tsnet-restricted",
    fallback: str = "internal",
) -> PoolConfig:
    return PoolConfig(
        name=name,
        gpu=list(gpu),
        nodes=nodes or 0,
        ttl=ttl or "",
        max_spend=max_spend or 0,
        providers=list(provider),
        regions=list(region),
        min_reliability=min_reliability or 0,
        selector=name,
        mode="private",
        transport=transport.replace("-", "_"),
        fallback=fallback,
        priority=priority,
    )


def _managed_pool_config(
    name: str,
    provider: Tuple[str, ...],
    region: Tuple[str, ...],
    gpu: Tuple[str, ...],
    nodes: int,
    ttl: str,
    max_spend: float,
    min_reliability: float,
    offer_id: str = "",
) -> PoolConfig:
    pool = _pool_config(
        name=name,
        gpu=gpu,
        nodes=nodes,
        ttl=ttl,
        max_spend=max_spend,
        provider=provider,
        region=region,
        min_reliability=min_reliability,
    )
    pool.offer_id = offer_id
    return pool


def _get_pool_renderable(
    service: ServiceClient,
    limit: int,
    output_format: str,
    filters: Dict[str, StringList],
    scope: str = "all",
) -> Any:
    """
    Returns a Rich renderable for compute pools.
    """
    private_pools: List[PrivatePool] = []
    control_plane_pools: List[ControlPlanePool] = []
    private_error = ""
    control_plane_error = ""

    if scope in ("all", "private"):
        private_res = service.gateway.list_private_pools(
            ListPrivatePoolsRequest(filters=filters, limit=limit)
        )
        if private_res.ok:
            private_pools = list(private_res.pools)
        else:
            private_error = private_res.err_msg
            if scope == "private":
                return Text(private_error, style="bold red")

    if scope in ("all", "control-plane"):
        control_plane_res = service.gateway.list_pools(
            ListPoolsRequest(filters=filters, limit=limit)
        )
        if control_plane_res.ok:
            control_plane_pools = list(control_plane_res.pools)
        else:
            control_plane_error = control_plane_res.err_msg
            if scope == "control-plane":
                return Text(control_plane_error, style="bold red")

    if scope == "all" and private_error and control_plane_error:
        return Text(private_error or control_plane_error, style="bold red")

    if output_format == "json":
        return Text(
            json.dumps(
                {
                    "private_pools": [_private_pool_dict(pool) for pool in private_pools],
                    "control_plane_pools": [
                        pool.to_dict(casing=Casing.SNAKE) for pool in control_plane_pools
                    ],
                },
                indent=2,
            )
        )

    return _pool_table(private_pools, control_plane_pools)


def _private_pool_dict(pool: PrivatePool) -> Dict[str, Any]:
    data = pool.to_dict(casing=Casing.SNAKE)  # type: ignore
    data.pop("source", None)
    config = data.get("config")
    if isinstance(config, dict):
        config.pop("fallback", None)
    return data


def _pool_table(
    private_pools: List[PrivatePool], control_plane_pools: List[ControlPlanePool]
) -> Table:
    table = Table(
        Column("Pool"),
        Column("Type"),
        Column("Status"),
        Column("Machines", justify="right"),
        Column("Compute"),
        box=box.SIMPLE,
    )
    for pool in private_pools:
        table.add_row(
            pool.name,
            "private",
            _pool_status(pool.status),
            f"{pool.ready_machine_count}/{pool.machine_count}",
            _private_pool_compute(pool),
        )
    if private_pools and control_plane_pools:
        table.add_section()
    for pool in control_plane_pools:
        table.add_row(
            pool.name,
            "managed",
            _pool_status(pool.state.status or ("active" if pool.active else "inactive")),
            f"{pool.state.ready_machines}/{pool.state.registered_machines}",
            _control_plane_pool_details(pool),
        )
    table.add_section()
    table.add_row(f"[bold]{len(private_pools) + len(control_plane_pools)} items")
    return table


def _pool_status(status: str) -> str:
    normalized = (status or "-").lower()
    if normalized == "active":
        return "[green]active[/green]"
    if normalized in ("degraded", "preflight_failed", "disconnected"):
        return f"[yellow]{status}[/yellow]"
    if normalized in ("disabled", "failed"):
        return f"[red]{status}[/red]"
    return status or "-"


def _private_pool_compute(pool: PrivatePool) -> str:
    gpu_types = pool.config.gpu
    node_count = _private_pool_node_count(pool)

    if node_count > 0:
        node_type = ", ".join(gpu_types) if gpu_types else "CPU"
        return f"{_node_label(node_count)} ({node_type})"
    if gpu_types:
        return f"nodes ({', '.join(gpu_types)})"
    return "CPU"


def _private_pool_node_count(pool: PrivatePool) -> int:
    configured = pool.config.nodes or pool.reserved_nodes
    if configured > 0:
        return configured
    return sum(_reservation_node_count(reservation) for reservation in pool.reservations)


def _reservation_node_count(reservation: Any) -> int:
    return reservation.node_count or 1


def _node_label(count: int) -> str:
    return f"{count} node{'s' if count != 1 else ''}"


def _control_plane_pool_details(pool: ControlPlanePool) -> str:
    parts = []
    if pool.state.free_gpu > 0:
        parts.append(f"{pool.state.free_gpu}GPU")
    if pool.state.free_cpu > 0:
        parts.append(format_cpu(pool.state.free_cpu))
    if pool.state.free_memory > 0:
        parts.append(format_memory(pool.state.free_memory))
    return "/".join(parts) if parts else "-"


def _pool_offer_table(offers: List[PoolOffer]) -> Table:
    table = Table(
        Column("Provider"),
        Column("Region"),
        Column("Instance"),
        Column("Nodes", justify="right"),
        Column("Compute"),
        Column("$/hr", justify="right"),
        Column("Available", justify="right"),
        box=box.SIMPLE,
    )
    for offer in offers:
        compute = []
        if offer.gpu:
            compute.append(f"{offer.gpu_count}x {offer.gpu}")
        if offer.cpu_millicores:
            compute.append(format_cpu(offer.cpu_millicores))
        if offer.memory_mb:
            compute.append(format_memory(offer.memory_mb))
        table.add_row(
            offer.provider,
            offer.region_display_name or offer.region,
            offer.display_name or offer.instance_type,
            str(offer.node_count or 1),
            ", ".join(compute) if compute else "-",
            f"${offer.hourly_cost_micros / 1_000_000:.4f}",
            str(offer.available),
        )
    table.add_section()
    table.add_row(f"[bold]{len(offers)} items")
    return table


def _pool_capacity_summary(pool: PrivatePool) -> str:
    reserved = pool.reserved_nodes or sum(_reservation_node_count(r) for r in pool.reservations)
    return f"{pool.ready_machine_count}/{pool.machine_count} ready, {reserved} reserved"


@management.command(
    name="list",
    help="List compute pools.",
    epilog="""
    Examples:

      # List the first 10 pools
      {cli_name} pool list --limit 10

      # List pools and output in JSON format
      {cli_name} pool list --format json

      # List only control-plane pools (admin tokens only)
      {cli_name} pool list --scope control-plane
      
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
    "--scope",
    type=click.Choice(("all", "private", "control-plane")),
    default="all",
    show_default=True,
    help="Pool scope to show.",
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
    scope: str,
    watch: bool,
    period: float,
):
    """
    List compute pools
    """
    if not watch:
        terminal.print(_get_pool_renderable(service, limit, format, filter, scope))
        return

    with Live(
        _get_pool_renderable(service, limit, format, filter, scope),
        console=terminal._console,
        screen=True,
        refresh_per_second=1 / period,
    ) as live:
        try:
            while True:
                live.update(_get_pool_renderable(service, limit, format, filter, scope))
                time.sleep(period)
        except KeyboardInterrupt:
            pass


@management.command(
    name="create",
    help="Create a private compute pool.",
    epilog="""
    Examples:

      # Create a private pool for machines you attach yourself
      {cli_name} pool create web-cpu

      # Add this machine to the pool
      {cli_name} pool join web-cpu

      # Reserve managed Hetzner nodes for the pool
      {cli_name} pool scale web-cpu --provider hetzner --nodes 2 --ttl 6h --max-spend 5
      \b
    """,
)
@click.argument("name")
@click.option("--mode", type=click.Choice(("private",)), default="private", show_default=True)
@click.option("--gpu", "gpu", multiple=True, help="GPU type accepted by this private pool.")
@click.option(
    "--nodes",
    type=click.IntRange(0),
    default=0,
    help="Optional desired node count.",
)
@click.option("--priority", type=int, default=1000, show_default=True)
@click.option(
    "--transport",
    type=click.Choice(("tsnet-restricted", "tsnet_restricted")),
    default="tsnet-restricted",
    show_default=True,
)
@extraclick.pass_service_client
def create(
    service: ServiceClient,
    name: str,
    mode: str,
    gpu: Tuple[str, ...],
    nodes: int,
    priority: int,
    transport: str,
):
    if mode != "private":
        return terminal.error("Only private pools can be created from the CLI.")

    res = service.gateway.create_pool(
        CreatePoolRequest(
            pool=_pool_config(
                name=name,
                gpu=gpu,
                nodes=nodes,
                priority=priority,
                transport=transport,
            )
        )
    )
    if not res.ok:
        return terminal.error(res.err_msg)
    terminal.success(f"Created private pool '{res.pool.name}'")


@management.command(
    name="offers",
    help="List managed node offers for a private pool.",
    epilog="""
    Examples:

      # Show CPU offers from Hetzner
      {cli_name} pool offers --provider hetzner

      # Show offers for a 2-node L4 pool
      {cli_name} pool offers --gpu L4 --nodes 2 --ttl 6h --max-spend 20
      \b
    """,
)
@click.option("--provider", "provider", multiple=True, help="Provider to search.")
@click.option("--region", "region", multiple=True, help="Provider region to search.")
@click.option("--gpu", "gpu", multiple=True, help="GPU type required by the pool.")
@click.option("--nodes", type=click.IntRange(1), default=1, show_default=True)
@click.option("--ttl", default="1h", show_default=True, help="Reservation duration.")
@click.option("--max-spend", type=float, default=1, show_default=True, help="Maximum spend in USD.")
@click.option("--min-reliability", type=click.FloatRange(0, 1), default=0)
@click.option("--format", type=click.Choice(("table", "json")), default="table", show_default=True)
@extraclick.pass_service_client
def offers(
    service: ServiceClient,
    provider: Tuple[str, ...],
    region: Tuple[str, ...],
    gpu: Tuple[str, ...],
    nodes: int,
    ttl: str,
    max_spend: float,
    min_reliability: float,
    format: str,
):
    pool = _managed_pool_config("", provider, region, gpu, nodes, ttl, max_spend, min_reliability)
    res = service.gateway.list_pool_offers(ListPoolOffersRequest(pool=pool))
    if not res.ok:
        return terminal.error(res.err_msg)
    if format == "json":
        terminal.print_json({"offers": [o.to_dict(casing=Casing.SNAKE) for o in res.offers]})
        return
    terminal.print(_pool_offer_table(list(res.offers)))


@management.command(
    name="scale",
    help="Reserve managed nodes for a private pool.",
    epilog="""
    Examples:

      # Reserve two Hetzner CPU nodes for six hours
      {cli_name} pool scale web-cpu --provider hetzner --nodes 2 --ttl 6h --max-spend 5

      # Reserve one GPU node and route a service to it
      {cli_name} pool scale gpu-api --provider shadeform --gpu L4 --nodes 1 --ttl 2h --max-spend 4
      {cli_name} deploy --name api --dockerfile Dockerfile --pool gpu-api
      \b
    """,
)
@click.argument("name")
@click.option("--provider", "provider", multiple=True, help="Provider to reserve from.")
@click.option("--region", "region", multiple=True, help="Provider region to reserve in.")
@click.option("--gpu", "gpu", multiple=True, help="GPU type required by the pool.")
@click.option("--nodes", type=click.IntRange(1), default=1, show_default=True)
@click.option("--ttl", required=True, help="Reservation duration, for example 1h or 6h.")
@click.option("--max-spend", type=float, required=True, help="Maximum spend in USD.")
@click.option("--min-reliability", type=click.FloatRange(0, 1), default=0)
@click.option("--offer-id", default="", help="Reserve a specific offer returned by pool offers.")
@extraclick.pass_service_client
def scale(
    service: ServiceClient,
    name: str,
    provider: Tuple[str, ...],
    region: Tuple[str, ...],
    gpu: Tuple[str, ...],
    nodes: int,
    ttl: str,
    max_spend: float,
    min_reliability: float,
    offer_id: str,
):
    pool = _managed_pool_config(
        name, provider, region, gpu, nodes, ttl, max_spend, min_reliability, offer_id
    )
    res = service.gateway.launch_pool_capacity(LaunchPoolCapacityRequest(pool=pool, nodes=nodes))
    if not res.ok:
        msg = res.err_msg
        if res.required_cents or res.available_cents:
            msg = (
                f"{msg} "
                f"(required ${res.required_cents / 100:.2f}, "
                f"available ${res.available_cents / 100:.2f})"
            )
        return terminal.error(msg)
    terminal.success(f"Scaled private pool '{res.pool.name}' ({_pool_capacity_summary(res.pool)})")


@management.command(name="private", help="List private compute pools.", hidden=True)
@click.option("--limit", type=click.IntRange(1, 100), default=20, show_default=True)
@click.option("--format", type=click.Choice(("table", "json")), default="table", show_default=True)
@extraclick.pass_service_client
def private_pools(service: ServiceClient, limit: int, format: str):
    terminal.print(_get_pool_renderable(service, limit, format, {}, "private"))


@management.command(name="machines", help="List machines joined to private pools.")
@click.argument("name", required=False)
@click.option("--limit", type=click.IntRange(1, 100), default=20, show_default=True)
@click.option("--format", type=click.Choice(("table", "json")), default="table", show_default=True)
@extraclick.pass_service_client
def machines(service: ServiceClient, name: Optional[str], limit: int, format: str):
    machines = _list_pool_machines(service, name, limit)
    if format == "json":
        terminal.print_json(
            {"machines": [m.to_dict(casing=Casing.SNAKE) for m in machines]}  # type: ignore
        )
        return

    terminal.print(machine_table(machines))


def _list_pool_machines(
    service: ServiceClient, pool_name: Optional[str], limit: int
) -> List[Machine]:
    if pool_name:
        return _fetch_pool_machines(service, pool_name, limit)

    pools_res = service.gateway.list_private_pools(ListPrivatePoolsRequest(limit=limit))
    if not pools_res.ok:
        terminal.error(pools_res.err_msg)

    machines: List[Machine] = []
    for pool in pools_res.pools:
        remaining = limit - len(machines)
        if remaining <= 0:
            break
        machines.extend(_fetch_pool_machines(service, pool.name, remaining))
    return machines


def _fetch_pool_machines(service: ServiceClient, pool_name: str, limit: int) -> List[Machine]:
    res = service.gateway.list_pool_machines(
        ListPoolMachinesRequest(pool_name=pool_name, limit=limit)
    )
    if not res.ok:
        terminal.error(res.err_msg)

    for machine in res.machines:
        if not machine.pool_name:
            machine.pool_name = pool_name
    return list(res.machines)


@management.command(name="extend", help="Extend private pool capacity.", hidden=True)
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


@management.command(name="terminate", help="Terminate and delete a private pool.", hidden=True)
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
    res = service.gateway.get_pool_join_command(GetPoolJoinCommandRequest(pool_name=name, ttl=ttl))
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
@click.option("--gpu", "gpu", multiple=True, help="GPU type accepted by this private pool.")
@click.option("--priority", type=int, default=1000, show_default=True)
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
@click.option("--worker-image", default="", help="Worker image for the worker-container executor.")
@click.option("--max-cpu", default="", help="Maximum CPU cores to advertise from this machine.")
@click.option("--max-memory", default="", help="Maximum memory to advertise, for example 32Gi.")
@click.option("--max-gpus", type=click.IntRange(0), default=0, help="Maximum GPUs to advertise.")
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
@click.option("--cache-dir", default="", help="Agent host cache directory.")
@click.option("--print-only", is_flag=True, help="Only print the generated join command.")
@extraclick.pass_service_client
def join(
    service: ServiceClient,
    name: str,
    ttl: str,
    gpu: Tuple[str, ...],
    priority: int,
    transport: str,
    agent_bin: str,
    executor: Optional[str],
    worker_image: str,
    max_cpu: str,
    max_memory: str,
    max_gpus: int,
    gpu_ids: str,
    network_slots: int,
    container_start_concurrency: int,
    background: Optional[bool],
    service_manager: Optional[str],
    service_name: str,
    state_dir: str,
    cache_dir: str,
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
                priority=priority,
                transport=transport,
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
        cache_dir=cache_dir,
    )
    if print_only:
        terminal.detail(command, crop=False, overflow="ignore")
        return

    try:
        exit_code = subprocess.call(command, shell=True)
    except KeyboardInterrupt:
        return
    if _agent_join_interrupted(exit_code):
        return
    if exit_code == 0:
        terminal.success("Agent is running.")
    raise SystemExit(exit_code)


def _join_transport(service: ServiceClient, transport: str) -> str:
    if transport != "auto":
        return transport
    return "tsnet_restricted"


def _agent_join_interrupted(exit_code: int) -> bool:
    return exit_code in (-signal.SIGINT, 128 + signal.SIGINT)


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
    background: Optional[bool] = None,
    service_manager: Optional[str] = None,
    service_name: str = "",
    state_dir: str = "",
    cache_dir: str = "",
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
    if cache_dir:
        extra.extend(["--cache-dir", cache_dir])
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
        extra.extend(["--container-start-concurrency", str(container_start_concurrency)])
    if not extra:
        return command
    extra_args = " ".join(shlex.quote(value) for value in extra)
    if command.strip().startswith("if ") and "; else " in command and "; fi" in command:
        command = command.replace("; else ", " " + extra_args + "; else ", 1)
        return command.replace("; fi", " " + extra_args + "; fi", 1)
    return command + " " + extra_args
