import click
from betterproto import Casing
from grpclib.client import Channel
from rich.table import Column, Table, box

from beam import aio, terminal
from beam.clients.gateway import GatewayServiceStub, ListTasksResponse, StopTaskResponse
from beam.config import with_runner_context


@click.group(
    name="tasks",
    help="List and stop tasks",
)
def cli():
    pass


@cli.command(
    name="list",
    help="List all tasks",
)
@click.option(
    "--format",
    type=click.Choice(("table", "json")),
    default="table",
    show_default=True,
    help="Change the format of the output.",
)
@with_runner_context
def list_tasks(format: str, channel: Channel):
    service = GatewayServiceStub(channel)
    response: ListTasksResponse = aio.run_sync(service.list_tasks())

    if format == "json":
        tasks = [task.to_dict(casing=Casing.SNAKE) for task in response.tasks]
        terminal.print_json(tasks)
        return

    table = Table(
        Column("Task ID"),
        Column("Status"),
        Column("Started At"),
        Column("Ended At"),
        Column("Container ID"),
        Column("Stub"),
        Column("Workspace"),
        box=box.SIMPLE,
    )

    tasks = sorted(response.tasks, key=lambda t: t.created_at, reverse=True)
    for task in tasks:
        table.add_row(
            task.id,
            (
                f"[bold green]{task.status}"
                if task.status.lower() == "complete"
                else f"[bold yellow]{task.status}"
            ),
            terminal.humanize_date(task.started_at),
            terminal.humanize_date(task.ended_at),
            task.container_id,
            str(task.stub_name),
            str(task.workspace_name),
        )

    table.add_section()
    table.add_row(f"[bold]Total: {response.total}")
    terminal.print(table)


@cli.command(
    name="stop",
    help="Stop a task",
)
@click.option(
    "--task-id",
    help="The task to stop.",
)
@with_runner_context
def stop_task(task_id: str, channel: Channel):
    service = GatewayServiceStub(channel)
    response: StopTaskResponse = aio.run_sync(service.stop_task(task_id=task_id))

    if response.ok:
        terminal.detail(f"Stopped task {task_id}", dim=False)
    else:
        terminal.error(f"Failed to stop task {task_id}")
