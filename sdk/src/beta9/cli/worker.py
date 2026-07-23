from typing import List

import click
from betterproto import Casing
from rich.table import Column, Table, box

from .. import terminal
from ..channel import ServiceClient
from ..cli import extraclick
from ..clients.gateway import ListWorkersRequest, ListWorkersResponse
from ..clients.types import Worker
from .extraclick import ClickCommonGroup, ClickManagementGroup
from .machine_format import format_memory_pair
from .worker_management import apply_worker_action, worker_ids_from_args


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@click.group(
    name="worker",
    help="Inspect and maintain workers visible to the current profile.",
    cls=ClickManagementGroup,
)
def management():
    pass


def _worker_state(worker: Worker) -> str:
    states = []
    if worker.cordon_requested:
        states.append("[red]cordoned[/red]")
    if worker.rollout_generation:
        phase = "draining" if worker.active_containers else "upgrading"
        states.append(f"[yellow]{phase}[/yellow]")
    if states:
        return " · ".join(states)
    return {
        "available": "[green]available[/green]",
        "pending": "[yellow]pending[/yellow]",
        "disabled": "[red]disabled[/red]",
    }.get(worker.status, worker.status or "-")


def _worker_matches_state(worker: Worker, state: str) -> bool:
    if state == "cordoned":
        return worker.cordon_requested
    if state == "draining":
        return bool(worker.rollout_generation and worker.active_containers)
    if state == "upgrading":
        return bool(worker.rollout_generation and not worker.active_containers)
    return worker.status == state


def _worker_cpu(worker: Worker) -> str:
    return f"{worker.free_cpu / 1000:g}/{worker.total_cpu / 1000:g}"


def _worker_memory(worker: Worker) -> str:
    return format_memory_pair(worker.free_memory, worker.total_memory)


def _worker_gpu(worker: Worker) -> str:
    if not worker.gpu and not worker.total_gpu_count:
        return "-"
    return f"{worker.free_gpu_count}/{worker.total_gpu_count} {worker.gpu or 'GPU'}"


def _worker_version(worker: Worker) -> str:
    current = worker.build_version or "-"
    target = worker.rollout_build_version
    if target and target != current:
        version = f"[yellow]{current} → {target}[/yellow]"
    else:
        version = target or current
    if worker.worker_image_override:
        version += " [magenta](pinned)[/magenta]"
    return version


@management.command(
    name="list",
    help="List workers with maintenance and rollout state.",
    epilog="""
    Examples:

      # Show workers in a pool
      {cli_name} worker list --pool my-gpu-pool

      # Watch rollout details as JSON
      watch -n 2 '{cli_name} worker list --state upgrading --format json'
    """,
)
@click.option(
    "--format",
    type=click.Choice(("table", "json")),
    default="table",
    show_default=True,
    help="Output format.",
)
@click.option("--pool", "pool_name", help="Only show workers in this pool.")
@click.option("--machine", "machine_id", help="Only show workers on this machine.")
@click.option(
    "--state",
    "--status",
    "state",
    type=click.Choice(("available", "pending", "disabled", "cordoned", "draining", "upgrading")),
    help="Only show workers in this operational state.",
)
@extraclick.pass_service_client
def list_workers(
    service: ServiceClient,
    format: str,
    pool_name: str,
    machine_id: str,
    state: str,
):
    res: ListWorkersResponse = service.gateway.list_workers(ListWorkersRequest())
    if not res.ok:
        terminal.error(res.err_msg)

    workers: List[Worker] = list(res.workers)
    if pool_name:
        workers = [worker for worker in workers if worker.pool_name == pool_name]
    if machine_id:
        workers = [worker for worker in workers if worker.machine_id == machine_id]
    if state:
        workers = [worker for worker in workers if _worker_matches_state(worker, state)]

    if format == "json":
        terminal.print_json(
            [worker.to_dict(casing=Casing.SNAKE, include_default_values=True) for worker in workers]  # type: ignore
        )
        return

    table = Table(
        Column("ID", no_wrap=True),
        Column("Pool", no_wrap=True),
        Column("State", no_wrap=True),
        Column("Machine", no_wrap=True),
        Column("CPU", justify="right", no_wrap=True),
        Column("Memory", justify="right", no_wrap=True),
        Column("GPU", no_wrap=True),
        Column("Containers", justify="right"),
        Column("Version", no_wrap=True),
        box=box.SIMPLE,
    )
    for worker in workers:
        table.add_row(
            worker.id,
            worker.pool_name or "-",
            _worker_state(worker),
            worker.machine_id or "-",
            _worker_cpu(worker),
            _worker_memory(worker),
            _worker_gpu(worker),
            str(len(worker.active_containers)),
            _worker_version(worker),
        )
    table.add_section()
    count, suffix = terminal.pluralize(workers, "s")
    table.add_row(f"[bold]{count} worker{suffix}")
    terminal.print(table)


@management.command(
    name="cordon",
    help="Stop scheduling new work while existing containers keep running.",
    epilog="""
    Examples:

      {cli_name} worker cordon 675a65c3
      {cli_name} worker list --format json | jq -r '.[].id' | {cli_name} worker cordon -
    """,
)
@click.argument("worker_ids", nargs=-1, required=True)
@extraclick.pass_service_client
def cordon_worker(service: ServiceClient, worker_ids: List[str]):
    apply_worker_action(service, worker_ids_from_args(worker_ids), "cordon")


@management.command(
    name="uncordon",
    help="Return explicitly cordoned workers to scheduling after maintenance.",
    epilog="""
    Examples:

      {cli_name} worker uncordon 675a65c3
      {cli_name} worker list --state cordoned --format json | jq -r '.[].id' | {cli_name} worker uncordon -
    """,
)
@click.argument("worker_ids", nargs=-1, required=True)
@extraclick.pass_service_client
def uncordon_worker(service: ServiceClient, worker_ids: List[str]):
    apply_worker_action(service, worker_ids_from_args(worker_ids), "uncordon")


@management.command(
    name="drain",
    help="Cordon workers and stop their active containers for maintenance.",
    epilog="""
    Drained workers remain cordoned. Run `worker uncordon` after maintenance.

    Examples:

      {cli_name} worker drain 675a65c3
      {cli_name} worker drain 675a65c3 9c1b7bae
      {cli_name} worker list --pool my-pool --format json | jq -r '.[].id' | {cli_name} worker drain -
    """,
)
@click.argument("worker_ids", nargs=-1, required=True)
@extraclick.pass_service_client
def drain_worker(service: ServiceClient, worker_ids: List[str]):
    apply_worker_action(service, worker_ids_from_args(worker_ids), "drain")
