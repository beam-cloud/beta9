from datetime import datetime, timezone

import click
from betterproto import Casing
from rich.table import Column, Table, box

from .. import terminal
from ..channel import ServiceClient
from ..cli import extraclick
from ..clients.gateway import (
    CreateMachineRequest,
    CreateMachineResponse,
    DeleteMachineRequest,
    DeleteMachineResponse,
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
    res = service.gateway.list_machines(ListMachinesRequest(pool_name=pool, limit=limit))

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
        Column("Created"),
        Column("Last Keepalive"),
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
            terminal.humanize_date(datetime.fromtimestamp(int(machine.created), tz=timezone.utc)),
            terminal.humanize_date(
                datetime.fromtimestamp(int(machine.last_keepalive), tz=timezone.utc)
            )
            if machine.last_keepalive != ""
            else "Never",
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
    res = service.gateway.create_machine(CreateMachineRequest(pool_name=pool))
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


@management.command(
    name="delete",
    help="Delete a machine.",
    epilog="""
      Examples:
 
        {cli_name} machine delete my-machine-id --pool ec2-t4
        \b
    """,
)
@click.argument(
    "machine_id",
    nargs=1,
    required=True,
)
@click.option(
    "--pool",
    "-p",
    help="The pool to select for the machine.",
    required=True,
)
@extraclick.pass_service_client
def delete_machine(service: ServiceClient, machine_id: str, pool: str):
    res: DeleteMachineResponse
    res = service.gateway.delete_machine(
        DeleteMachineRequest(machine_id=machine_id, pool_name=pool)
    )
    if res.ok:
        terminal.success(f"Deleted machine '{machine_id}' from pool '{pool}'")
    else:
        terminal.error(f"Error: {res.err_msg}")
