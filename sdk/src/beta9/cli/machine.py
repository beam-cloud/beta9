import click
from betterproto import Casing
from rich.table import Column, Table, box

from .. import aio, terminal
from ..channel import ServiceClient
from ..cli import extraclick
from ..clients.gateway import (
    CreateMachineRequest,
    CreateMachineResponse,
    ListMachinesRequest,
    ListMachinesResponse,
)
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
@click.option(
    "--pool",
    "-p",
    help="The pool to filter.",
    required=False,
)
@extraclick.pass_service_client
def list_machines(
    service: ServiceClient,
    limit: int,
    format: str,
    pool: str,
):
    res: ListMachinesResponse
    res = aio.run_sync(
        service.gateway.list_machines(ListMachinesRequest(pool_name=pool, limit=limit))
    )

    if not res.ok:
        terminal.error(res.err_msg)

    if format == "json":
        machines = [d.to_dict(casing=Casing.SNAKE) for d in res.machines]  # type:ignore
        terminal.print_json(machines)
        return

    table = Table(
        Column("ID"),
        Column("CPU"),
        Column("Memory"),
        Column("GPU"),
        Column("Status"),
        Column("Pool"),
        box=box.SIMPLE,
    )

    for machine in res.machines:
        table.add_row(
            machine.id,
            str(machine.cpu),
            str(machine.memory),
            machine.gpu,
            machine.status,
            machine.pool_name,
        )

    table.add_section()
    table.add_row(f"[bold]{len(res.machines)} items")
    terminal.print(table)


@management.command(
    name="create",
    help="Create a new machine.",
    epilog="""
      Examples:

        {cli_name} machine create --pool ec2-t4
        \b
    """,
)
@click.option(
    "--pool",
    "-p",
    help="The pool to select for the machine.",
    required=True,
)
@extraclick.pass_service_client
def create_machine(service: ServiceClient, pool: str):
    res: CreateMachineResponse
    res = aio.run_sync(service.gateway.create_machine(CreateMachineRequest(pool_name=pool)))
    if res.ok:
        terminal.header(
            f"Created machine with ID: '{res.machine.id}'. Use the following command to setup the node:"
        )
        terminal.detail(
            f"""sudo curl -L -o agent https://release.beam.cloud/agent && \\
sudo chmod +x agent && \\
sudo ./agent --token "{res.machine.registration_token}" --machine-id "{res.machine.id}" \\
--tailscale-url "{res.machine.tailscale_url}" \\
--tailscale-auth "{res.machine.tailscale_auth}" \\
--pool-name "{res.machine.pool_name}" \\
--provider-name "{res.machine.provider_name}" """
        )

    else:
        terminal.error(f"Error: {res.err_msg}")
