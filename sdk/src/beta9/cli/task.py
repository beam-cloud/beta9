from typing import Dict, List

import click
from betterproto import Casing
from rich.table import Column, Table, box

from .. import aio, terminal
from ..clients.gateway import (
    GatewayServiceStub,
    ListTasksRequest,
    ListTasksResponse,
    StopTaskRequest,
    StopTaskResponse,
    StringList,
)
from .contexts import ServiceClient
from .extraclick import ClickManagementGroup


@click.group(
    name="task",
    help="Manage tasks.",
    cls=ClickManagementGroup,
)
@click.pass_context
def management(ctx: click.Context):
    ctx.obj = ctx.with_resource(ServiceClient())


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


@management.command(
    name="list",
    help="List all tasks.",
    epilog="""
    # List the first 10 tasks
    beta9 task list --limit 10

    # List tasks with status 'running' or 'pending' and stub-id 'function/test:handler'
    beta9 task list --filter status=running,pending --filter stub-id=function/test:handler

    # List tasks and output in JSON format
    beta9 task list --format json
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
def list_tasks(service: ServiceClient, limit: int, format: str, filter: Dict[str, StringList]):
    res: ListTasksResponse
    res = aio.run_sync(service.gateway.list_tasks(ListTasksRequest(filters=filter, limit=limit)))

    if not res.ok:
        terminal.error(res.err_msg)

    if format == "json":
        tasks = [task.to_dict(casing=Casing.SNAKE) for task in res.tasks]
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

    for task in res.tasks:
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
    table.add_row(f"[bold]Total: {res.total}")
    terminal.print(table)


@management.command(
    name="stop",
    help="Stop a task.",
)
@click.option(
    "--task-id",
    help="The task to stop.",
)
@click.pass_obj
def stop_task(service: GatewayServiceStub, task_id: str):
    res: StopTaskResponse = aio.run_sync(service.stop_task(StopTaskRequest(task_id=task_id)))

    if res.ok:
        terminal.detail(f"Stopped task {task_id}", dim=False)
    else:
        terminal.error(f"{res.err_msg}\nFailed to stop task {task_id}")
