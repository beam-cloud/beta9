import click
from betterproto import Casing
from rich.table import Column, Table, box

from beam import aio, terminal
from beam.cli.contexts import get_gateway_service
from beam.clients.gateway import GatewayServiceStub, ListTasksResponse, StopTaskResponse


@click.group(
    name="tasks",
    help="List and stop tasks",
)
@click.pass_context
def cli(ctx: click.Context):
    ctx.obj = ctx.with_resource(get_gateway_service())


@cli.command(
    name="list",
    help="List all tasks",
)
@click.option(
    "--limit",
    type=click.IntRange(1, 1000),
    default=100,
    help="The number of tasks to fetch.",
)
@click.option(
    "--format",
    type=click.Choice(("table", "json")),
    default="table",
    show_default=True,
    help="Change the format of the output.",
)
@click.pass_obj
def list_tasks(service: GatewayServiceStub, limit: int, format: str):
    response: ListTasksResponse = aio.run_sync(service.list_tasks(limit=limit))

    if not response.ok:
        terminal.error(response.err_msg)

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

    for task in response.tasks:
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
@click.pass_obj
def stop_task(service: GatewayServiceStub, task_id: str):
    response: StopTaskResponse = aio.run_sync(service.stop_task(task_id=task_id))

    if response.ok:
        terminal.detail(f"Stopped task {task_id}", dim=False)
    else:
        terminal.error(f"{response.err_msg}\nFailed to stop task {task_id}")
