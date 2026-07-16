from datetime import datetime, timezone
from typing import Dict, Optional, Sequence

from rich.table import Column, Table, box

from .. import terminal
from ..clients.gateway import Machine, PoolOffer


def gpu_inventory_table(
    live_gpus: Dict[str, bool],
    supported_gpus: Dict[str, bool],
    cheapest_offers: Dict[str, PoolOffer],
    offers_available: bool = True,
) -> Table:
    """
    One row per GPU type that has any capacity signal: live serverless
    workers, a serverless pool config (scale-to-zero), or an on-demand offer.
    GPU types with no capacity anywhere are summarized in the caption so
    they never stretch the table.
    """
    gpu_types = sorted(set(live_gpus) | set(supported_gpus) | set(cheapest_offers))
    hidden = 0
    rows = []
    for gpu_type in gpu_types:
        live = live_gpus.get(gpu_type, False)
        supported = supported_gpus.get(gpu_type, False)
        offer = cheapest_offers.get(gpu_type)

        if not live and not supported and offer is None:
            hidden += 1
            continue

        if live:
            serverless = "[green]●[/green] ready"
        elif supported:
            serverless = "[cyan]●[/cyan] available"
        else:
            serverless = "[dim]—[/dim]"

        if offer is not None:
            hourly = offer.hourly_cost_micros / 1_000_000
            on_demand = f"${hourly:.2f}/hr"
        elif offers_available:
            on_demand = "[dim]—[/dim]"
        else:
            on_demand = "[dim]?[/dim]"

        rows.append((gpu_type, serverless, on_demand))

    caption = None
    if hidden > 0:
        caption = f"{hidden} more GPU types · no capacity"

    table = Table(
        Column("GPU", no_wrap=True),
        Column("Serverless", no_wrap=True),
        Column("On-demand", justify="right", no_wrap=True),
        box=box.SIMPLE,
        title="GPU inventory",
        title_justify="left",
        title_style=f"bold {terminal.BRAND_COLOR}",
        caption=caption,
        caption_justify="left",
        caption_style="dim",
    )

    for row in rows:
        table.add_row(*row)
    if not rows:
        table.add_row("[dim]—[/dim]", "[dim]no capacity visible[/dim]", "[dim]—[/dim]")

    return table


def machine_table(machines: Sequence[Machine], title: str = "") -> Table:
    table = Table(
        Column("ID", no_wrap=True),
        Column("Pool"),
        Column("State"),
        Column("Capacity", no_wrap=True),
        Column("Load", no_wrap=True),
        Column("Last seen"),
        Column("Agent"),
        box=box.SIMPLE,
        title=title or None,
        title_justify="left",
        title_style=f"bold {terminal.BRAND_COLOR}",
    )
    for machine in machines:
        table.add_row(
            machine.id,
            machine.pool_name or "-",
            machine_status(machine.status),
            machine_capacity(machine),
            machine_load(machine),
            machine_last_keepalive(machine.last_keepalive),
            f"v{machine.agent_version}" if machine.agent_version else "-",
        )

    table.add_section()
    count, suffix = terminal.pluralize(machines, "s")
    table.add_row(f"[bold]{count} machine{suffix}")
    return table


def format_cpu(millicores: int) -> str:
    cores = millicores / 1000
    if cores.is_integer():
        return f"{int(cores)}CPU"
    return f"{cores:.2f}CPU"


def format_memory(memory_mb: int) -> str:
    if memory_mb >= 1024:
        return f"{memory_mb / 1024:.1f}GiB"
    return f"{memory_mb}MiB"


def format_gpu(gpu: str, gpu_count: int = 0) -> str:
    if not gpu:
        return "-"
    if gpu_count == 0:
        return gpu
    return f"{gpu} x {gpu_count}"


def machine_capacity(machine: Machine) -> str:
    parts = []
    if machine.cpu > 0:
        parts.append(format_cpu(machine.cpu))
    if machine.memory > 0:
        parts.append(format_memory(machine.memory))
    if machine.gpu:
        parts.append(format_gpu(machine.gpu, machine.gpu_count))
    return "\n".join(parts) or "-"


def machine_load(machine: Machine) -> str:
    metrics = getattr(machine, "machine_metrics", None)
    if metrics is None:
        return "-"
    parts = []
    if machine.gpu_count:
        parts.append(f"{metrics.free_gpu_count}/{machine.gpu_count} GPU free")
    if metrics.worker_count or metrics.container_count:
        workers = "worker" if metrics.worker_count == 1 else "workers"
        containers = "container" if metrics.container_count == 1 else "containers"
        parts.append(f"{metrics.worker_count} {workers} · {metrics.container_count} {containers}")
    return "\n".join(parts) or "-"


def machine_status(status: str) -> str:
    styles = {
        "available": "green",
        "ready": "green",
        "pending": "yellow",
        "registered": "yellow",
        "disabled": "red",
        "failed": "red",
        "preflight_failed": "red",
        "disconnected": "yellow",
    }
    normalized = (status or "").lower()
    style = styles.get(normalized)
    if not status or not style:
        return status or "-"
    return f"[{style}]{status}[/{style}]"


def machine_last_keepalive(value: str) -> str:
    last_seen = machine_datetime(value)
    return terminal.humanize_date(last_seen) if last_seen else "Never"


def machine_datetime(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc)
    except ValueError:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
