from datetime import datetime, timezone
from typing import Sequence

from rich.table import Column, Table, box

from ..clients.gateway import Machine


def machine_table(machines: Sequence[Machine]) -> Table:
    table = Table(
        Column("Pool"),
        Column("Machine"),
        Column("Status"),
        Column("CPU", justify="right"),
        Column("Memory", justify="right"),
        Column("GPU"),
        Column("Last Seen"),
        box=box.SIMPLE,
    )
    for machine in machines:
        table.add_row(
            machine.pool_name or "-",
            machine.id,
            machine.status or "-",
            machine_cpu(machine),
            machine_memory(machine),
            machine_gpu(machine),
            machine_last_seen(machine.last_keepalive),
        )

    table.add_section()
    table.add_row(f"[bold]{len(machines)} items")
    return table


def machine_cpu(machine: Machine) -> str:
    if machine.cpu <= 0:
        return "-"
    cores = machine.cpu / 1000
    if cores.is_integer():
        return str(int(cores))
    return f"{cores:.2f}"


def machine_memory(machine: Machine) -> str:
    if machine.memory <= 0:
        return "-"
    return format_memory(machine.memory)


def format_cpu(millicores: int) -> str:
    cores = millicores / 1000
    if cores.is_integer():
        return f"{int(cores)}CPU"
    return f"{cores:.2f}CPU"


def format_memory(memory_mb: int) -> str:
    if memory_mb >= 1024:
        return f"{memory_mb / 1024:.1f}GiB"
    return f"{memory_mb}MiB"


def machine_gpu(machine: Machine) -> str:
    if not machine.gpu:
        return "-"
    if machine.gpu_count == 0:
        return machine.gpu
    return f"{machine.gpu} x {machine.gpu_count}"


def machine_last_seen(value: str) -> str:
    if not value:
        return "Never"
    try:
        last_seen = datetime.fromtimestamp(int(value), tz=timezone.utc)
    except ValueError:
        last_seen = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return format_age(last_seen)


def format_age(value: datetime) -> str:
    diff = datetime.now(timezone.utc) - value
    if diff.days > 0:
        return f"{diff.days}d ago"
    seconds = max(0, int(diff.total_seconds()))
    if seconds >= 3600:
        return f"{seconds // 3600}h ago"
    if seconds >= 60:
        return f"{seconds // 60}m ago"
    return "just now"
