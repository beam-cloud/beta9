import click
from betterproto import Casing
from rich.table import Column, Table, box

from .. import aio, terminal
from ..channel import ServiceClient
from ..cli import extraclick
from ..clients.gateway import ListMachinesRequest, ListMachinesResponse
from .extraclick import ClickCommonGroup, ClickManagementGroup


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@click.group(
    name="machine",
    help="Manage remote machines.",
    cls=ClickManagementGroup,
)
def management():
    pass


@management.command(
    name="list",
    help="List all external machines.",
    epilog="""
    Examples:

      # List the first 10 machines
      {cli_name} machine list --limit 10

      # List machines and output in JSON format
      {cli_name} machine list --format json
      \b
    """,
)
@click.option(
    "--limit",
    type=click.IntRange(1, 100),
    default=20,
    help="The number of machines to fetch.",
)
@click.option(
    "--format",
    type=click.Choice(("table", "json")),
    default="table",
    show_default=True,
    help="Change the format of the output.",
)
@extraclick.pass_service_client
def list_machines(
    service: ServiceClient,
    limit: int,
    format: str,
):
    res: ListMachinesResponse
    res = aio.run_sync(
        service.gateway.list_machines(ListMachinesRequest(pool_name="default", limit=limit))
    )

    if not res.ok:
        terminal.error(res.err_msg)

    if format == "json":
        machines = [d.to_dict(casing=Casing.SNAKE) for d in res.machines]  # type:ignore
        terminal.print_json(machines)
        return

    table = Table(
        Column("Name"),
        Column("GPU"),
        box=box.SIMPLE,
    )

    for machine in res.machines:
        table.add_row(
            machine.name,
            machine.gpu,
        )

    table.add_section()
    table.add_row(f"[bold]{len(res.machines)} items")
    terminal.print(table)
