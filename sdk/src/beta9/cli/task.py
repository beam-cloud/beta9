from typing import Dict, List

import click
from betterproto import Casing
from rich.table import Column, Table, box

from .. import terminal
from ..channel import ServiceClient
from ..clients.gateway import (
    ListTasksRequest,
    ListTasksResponse,
    StopTasksRequest,
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

      # List tasks with two different statuses and a specific stub name
      {cli_name} task list --filter status=running,pending --filter stub-name=function/test:handler

      # List tasks that belong to two different stubs
      {cli_name} task list --filter stub-id=function/test:handler1,endpoint/test:handler2

      # List tasks that are on a specific container
      {cli_name} task list --filter container-id=endpoint-05cc6c0d-fef2-491c-b9b7-313cc21c3496-3cef29e8

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
        Column("Created At"),
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
            terminal.humanize_date(task.created_at),
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
    epilog="""
    Examples:

      # Stop a task
      {cli_name} task stop 05cc6c0d-fef2-491c-b9b7-313cc21c3496

      # Stop multiple tasks
      {cli_name} task stop 05cc6c0d-fef2-491c-b9b7-313cc21c3496 22dee81b-eeab-4c0a-b28a-b81b159358f9

      # Stop multiple tasks from stdin
      {cli_name} task list --filter status=running --format=json | jq -r '.[] | .id' | {cli_name} task stop -
      \b
    """,
)
@click.argument(
    "task_ids",
    nargs=-1,
    required=True,
)
@extraclick.pass_service_client
def stop_task(service: ServiceClient, task_ids: List[str]):
    if task_ids and task_ids[0] == "-":
        task_ids = click.get_text_stream("stdin").read().strip().split()

    if not task_ids:
        return terminal.error("Must provide at least one task ID.")

    for task_id in task_ids:
        res = service.gateway.stop_tasks(StopTasksRequest(task_ids=[task_id]))
        if not res.ok:
            terminal.warn(str(res.err_msg).capitalize())
        else:
            terminal.success(f"Task {task_id} stopped.")
