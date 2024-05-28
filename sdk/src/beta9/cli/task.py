from typing import Dict

import click
from betterproto import Casing
from rich.table import Column, Table, box

from .. import terminal
from ..channel import ServiceClient
from ..clients.gateway import (
    ListTasksRequest,
    ListTasksResponse,
    StopTasksRequest,
    StopTasksResponse,
    StringList,
)
from . import extraclick
from .extraclick import ClickManagementGroup


@click.group(
    name="task",
    help="Manage tasks.",
    cls=ClickManagementGroup,
)
def management():
    pass


@management.command(
    name="list",
    help="List all tasks.",
    epilog="""
    Examples:

      # List the first 10 tasks
      {cli_name} task list --limit 10

      # List tasks with status 'running' or 'pending' and stub-id 'function/test:handler'
      {cli_name} task list --filter status=running,pending --filter stub-id=function/test:handler

      # List tasks and output in JSON format
      {cli_name} task list --format json
      \b
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
    callback=extraclick.filter_values_callback,
    help="Filters tasks. Add this option for each field you want to filter on.",
)
@extraclick.pass_service_client
def list_tasks(service: ServiceClient, limit: int, format: str, filter: Dict[str, StringList]):
    res: ListTasksResponse
    res = service.gateway.list_tasks(ListTasksRequest(filters=filter, limit=limit))

    if not res.ok:
        terminal.error(res.err_msg)

    if format == "json":
        tasks = [task.to_dict(casing=Casing.SNAKE) for task in res.tasks]  # type:ignore
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
    table.add_row(f"[bold]{res.total} items")
    terminal.print(table)


@management.command(
    name="stop",
    help="Stop a task.",
)
@click.argument(
    "task_id",
    required=True,
)
@extraclick.pass_service_client
def stop_task(service: ServiceClient, task_id: str):
    res: StopTasksResponse
    res = service.gateway.stop_tasks(StopTasksRequest(task_ids=[task_id]))

    if res.ok:
        terminal.success(f"Stopped task {task_id}.")
    else:
        terminal.error(f"{res.err_msg}\nFailed to stop task {task_id}.")
