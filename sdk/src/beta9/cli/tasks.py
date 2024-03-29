from typing import Dict, List

import click
from betterproto import Casing
from rich.table import Column, Table, box

from .. import aio, terminal
from ..cli.contexts import get_gateway_service
from ..cli.formatters import EpilogFormatter
from ..clients.gateway import (
    GatewayServiceStub,
    ListTasksResponse,
    StopTaskResponse,
    StringList,
)


@click.group(
    name="tasks",
    help="List and stop tasks",
)
@click.pass_context
def cli(ctx: click.Context):
    ctx.obj = ctx.with_resource(get_gateway_service())


def parse_filter_values(
    ctx: click.Context,
    param: click.Option,
    values: List[str],
) -> Dict[str, StringList]:
    filters: Dict[str, StringList] = {}

    for value in values:
        key, value = value.split("=")
        value_list = value.split(",") if "," in value else [value]

        if key == "status":
            value_list = [v.upper() for v in value_list]

        if not key or not value:
            raise click.BadParameter("Filter must be in the format key=value")

        filters[key] = StringList(values=value_list)

    return filters


@cli.command(
    name="list",
    help="List all tasks",
    cls=EpilogFormatter,
    epilog="""
    # List the first 10 tasks
    beta9 tasks list --limit 10

    # List tasks with status 'running' or 'pending' and stub-id 'function/test:handler'
    beta9 tasks list --filter status=running,pending --filter stub-id=function/test:handler

    # List tasks and output in JSON format
    beta9 tasks list --format json
    """,
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
@click.option(
    "--filter",
    multiple=True,
    callback=parse_filter_values,
    help="Filters tasks. Add this option for each field you want to filter on.",
)
@click.pass_obj
def list_tasks(service: GatewayServiceStub, limit: int, format: str, filter: Dict[str, StringList]):
    response: ListTasksResponse = aio.run_sync(service.list_tasks(filters=filter, limit=limit))

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
        Column("Stub Name"),
        Column("Workspace Name"),
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
            task.stub_name,
            task.workspace_name,
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
