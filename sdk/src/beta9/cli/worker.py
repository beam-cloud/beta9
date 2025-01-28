from typing import List

import click
from betterproto import Casing
from rich.table import Column, Table, box

from .. import terminal
from ..channel import ServiceClient
from ..cli import extraclick
from ..clients.gateway import (
    CordonWorkerRequest,
    DrainWorkerRequest,
    ListWorkersRequest,
    ListWorkersResponse,
    UncordonWorkerRequest,
)
from .extraclick import ClickCommonGroup, ClickManagementGroup


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@click.group(
    name="worker",
    help="Manage workers.",
    cls=ClickManagementGroup,
)
def management():
    pass


@management.command(
    name="list",
    help="List all workers.",
    epilog="""
    Examples:

      # List workers and output in JSON format
      {cli_name} worker list --format json
      \b
    """,
)
@click.option(
    "--format",
    type=click.Choice(("table", "json")),
    default="table",
    show_default=True,
    help="Change the format of the output.",
)
@extraclick.pass_service_client
def list_workers(
    service: ServiceClient,
    format: str,
):
    res: ListWorkersResponse
    res = service.gateway.list_workers(ListWorkersRequest())

    if not res.ok:
        terminal.error(res.err_msg)

    if format == "json":
        workers = [d.to_dict(casing=Casing.SNAKE) for d in res.workers]  # type:ignore
        terminal.print_json(workers)
        return

    table = Table(
        Column("ID"),
        Column("Pool"),
        Column("Status"),
        Column("Machine ID"),
        Column("Priority"),
        Column("CPU"),
        Column("Memory"),
        Column("GPUs"),
        Column("CPU Available"),
        Column("Memory Available"),
        Column("GPUs Available"),
        Column("Containers"),
        Column("Version"),
        box=box.SIMPLE,
    )

    for worker in res.workers:
        table.add_row(
            worker.id,
            worker.pool_name,
            {
                "available": f"[green]{worker.status}[/green]",
                "pending": f"[yellow]{worker.status}[/yellow]",
                "disabled": f"[red]{worker.status}[/red]",
            }.get(worker.status, worker.status),
            worker.machine_id if worker.machine_id else "-",
            str(worker.priority),
            f"{worker.total_cpu:,}m" if worker.total_cpu > 0 else "-",
            terminal.humanize_memory(worker.total_memory * 1024 * 1024),
            str(worker.total_gpu_count),
            f"{worker.free_cpu:,}m",
            terminal.humanize_memory(worker.free_memory * 1024 * 1024),
            str(worker.free_gpu_count),
            str(len(worker.active_containers)),
            worker.build_version,
        )

    table.add_section()
    table.add_row(f"[bold]{len(res.workers)} items")

    terminal.print(table)


@management.command(
    name="cordon",
    help="""
    Cordon a worker.

    When a worker is cordoned, it will not accept new container requests. However,
    it will continue to run existing containers until they are finished. This is
    useful when you want to gracefully remove a worker from the pool.
    """,
    epilog="""
    Examples:

      # Cordon a worker
      {cli_name} worker cordon 675a65c3

      # Cordon multiple workers from stdin (useful for piping)
      {cli_name} worker list --format=json | jq -r '.[].id' | {cli_name} worker cordon -
      \b
    """,
)
@click.argument(
    "worker_ids",
    nargs=-1,
    required=True,
)
@extraclick.pass_service_client
def cordon_worker(service: ServiceClient, worker_ids: List[str]):
    if worker_ids and worker_ids[0] == "-":
        worker_ids = click.get_text_stream("stdin").read().strip().split()

    if not worker_ids:
        return terminal.error("Must provide at least one worker ID.")

    for worker_id in worker_ids:
        res = service.gateway.cordon_worker(CordonWorkerRequest(worker_id=worker_id))
        if not res.ok:
            text = res.err_msg.capitalize()
            terminal.warn(text)
        else:
            terminal.success(f"Worker {worker_id} has been cordoned.")


@management.command(
    name="uncordon",
    help="Uncordon a worker.",
    epilog="""
      Examples:

        # Uncordon a worker
        {cli_name} worker uncordon 675a65c3

        # Uncordon multiple workers
        {cli_name} worker uncordon 675a65c3 9c1b7bae 4c89436cs

        # Uncordon workers from stdin (useful for piping)
        {cli_name} worker list --format=json | jq -r '.[] | select(.status=="disabled") | .id' | {cli_name} worker uncordon -
        \b
    """,
)
@click.argument(
    "worker_ids",
    nargs=-1,
    required=True,
)
@extraclick.pass_service_client
def uncordon_worker(service: ServiceClient, worker_ids: List[str]):
    if worker_ids and worker_ids[0] == "-":
        worker_ids = click.get_text_stream("stdin").read().strip().split()

    if not worker_ids:
        return terminal.error("Must provide at least one worker ID.")

    for worker_id in worker_ids:
        res = service.gateway.uncordon_worker(UncordonWorkerRequest(worker_id=worker_id))
        if not res.ok:
            text = res.err_msg.capitalize()
            terminal.warn(text)
        else:
            terminal.success(f"Worker {worker_id} has been uncordon.")


@management.command(
    name="drain",
    help="""
    Drain a worker.

    When a worker is drained, all running containers on it will be stopped. The
    worker will continue to run until it reaches its idle timeout, but only if
    the worker was cordoned before being drained.
    """,
    epilog="""
      Examples:

        # Drain a worker
        {cli_name} worker drain 675a65c3

        # Drain multiple workers
        {cli_name} worker drain 675a65c3 9c1b7bae 4c89436cs

        # Drain workers from stdin (useful for piping)
        {cli_name} worker list --format=json | jq -r '.[].id' | {cli_name} worker drain -
        \b
    """,
)
@click.argument(
    "worker_ids",
    nargs=-1,
    required=True,
)
@extraclick.pass_service_client
def drain_worker(
    service: ServiceClient,
    worker_ids: List[str],
):
    if worker_ids and worker_ids[0] == "-":
        worker_ids = click.get_text_stream("stdin").read().strip().split()

    if not worker_ids:
        return terminal.error("Must provide at least one worker ID.")

    for worker_id in worker_ids:
        res = service.gateway.drain_worker(DrainWorkerRequest(worker_id=worker_id))
        if not res.ok:
            text = res.err_msg.capitalize()
            terminal.warn(text)
        else:
            terminal.success(f"Worker {worker_id} has been drained.")
