from datetime import datetime, timezone
from typing import Dict, Optional, Sequence

from rich.table import Column, Table, box

from .. import terminal
from ..clients.gateway import Machine


def gpu_availability_table(gpus: Dict[str, bool]) -> Table:
    table = Table(
        Column("GPU Type"),
        Column("Available", justify="center"),
        box=box.SIMPLE,
    )
    for gpu_type, gpu_available in sorted(gpus.items()):
        table.add_row(gpu_type, "✅" if gpu_available else "❌")
    if not gpus:
        table.add_row("-", "-")
    table.add_section()
    table.add_row(f"[bold]{len(gpus)} items")
    return table


def machine_table(machines: Sequence[Machine]) -> Table:
    table = Table(
        Column("ID", no_wrap=True),
        Column("Pool"),
        Column("State"),
        Column("Capacity", no_wrap=True),
        Column("Load", no_wrap=True),
        Column("Last seen"),
        Column("Agent"),
        box=box.SIMPLE,
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
    table.add_row(f"[bold]{len(machines)} items")
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
