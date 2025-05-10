from datetime import datetime, timezone

import click
from betterproto import Casing
from rich.table import Column, Table, box

from .. import terminal
from ..channel import ServiceClient
from ..cli import extraclick
from ..clients.gateway import (
    CordonWorkerRequest,
    CreateMachineRequest,
    CreateMachineResponse,
    DeleteMachineRequest,
    DeleteMachineResponse,
    DrainWorkerRequest,
    ListMachinesRequest,
    ListMachinesResponse,
    ListWorkersRequest,
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

    res.gpus = {gpu: res.gpus[gpu] for gpu in sorted(res.gpus)}

    if format == "json":
        machines = [d.to_dict(casing=Casing.SNAKE) for d in res.machines]  # type:ignore
        terminal.print_json({"machines": machines, "gpus": res.gpus})
        return

    # Display GPU types available
    table = Table(
        Column("GPU Type"),
        Column("Available", justify="center"),
        box=box.SIMPLE,
    )
    for gpu_type, gpu_avail in res.gpus.items():
        table.add_row(gpu_type, "✅" if gpu_avail else "❌")
    if not res.gpus:
        table.add_row(*("-" * len(res.gpus)))
    table.add_section()
    table.add_row(f"[bold]{len(res.gpus)} items")
    terminal.print(table)

    # Display external provider machines connected to cluster
    if res.machines:
        table = Table(
            Column("ID"),
            Column("CPU"),
            Column("Memory"),
            Column("GPU"),
            Column("Status"),
            Column("Pool"),
            Column("Created"),
            Column("Last Keepalive"),
            Column("Agent Version"),
            Column("Free GPU Count"),
            box=box.SIMPLE,
        )

        for machine in res.machines:
            table.add_row(
                machine.id,
                f"{machine.cpu:,}m" if machine.cpu > 0 else "-",
                terminal.humanize_memory(machine.memory * 1024 * 1024)
                if machine.memory > 0
                else "-",
                machine.gpu,
                machine.status,
                machine.pool_name,
                terminal.humanize_date(
                    datetime.fromtimestamp(int(machine.created), tz=timezone.utc)
                ),
                terminal.humanize_date(
                    datetime.fromtimestamp(int(machine.last_keepalive), tz=timezone.utc)
                )
                if machine.last_keepalive != ""
                else "Never",
                f"v{machine.agent_version}" if machine.agent_version else "-",
                str(machine.machine_metrics.free_gpu_count),
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
    if not res.ok:
        return terminal.error(f"Error: {res.err_msg}")

    terminal.header(
        f"Created machine with ID: '{res.machine.id}'. Use the following command to setup the node:"
    )

    cmd_args = [
        f'--token "{res.machine.registration_token}"',
        f'--machine-id "{res.machine.id}"',
        f'--tailscale-url "{res.machine.tailscale_url}"',
        f'--tailscale-auth "{res.machine.tailscale_auth}"',
        f'--pool-name "{res.machine.pool_name}"',
        f'--provider-name "{res.machine.provider_name}"',
    ]

    agent_url = "https://release.beam.cloud/agent/agent"
    if res.agent_upstream_url:
        cmd_args.append(f'--flux-upstream "{res.agent_upstream_url}"')

    if res.agent_upstream_branch:
        cmd_args.append(f'--flux-branch "{res.agent_upstream_branch}"')
        if res.agent_upstream_branch != "main":
            agent_url += f"-{res.agent_upstream_branch}"

    cmd_args_formatted = " \\\n\t  ".join(cmd_args)
    text = f"""# -- Agent setup
    sudo curl -L -o agent {agent_url} &&
    sudo chmod +x agent &&
    sudo ./agent {cmd_args_formatted}
    """

    if res.machine.user_data:
        text = f"""# -- User data\n{res.machine.user_data}\n{text}"""

    terminal.detail(text, crop=False, overflow="ignore")


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


@management.command(
    name="drain",
    help="""
    Drain all workers on a specific machine.

    This command will find all workers running on the specified machine ID and drain them.
    When a worker is drained, all running containers on it will be stopped. The worker will 
    continue until its idle timeout if was cordoned before being drained. 
    """,
    epilog="""
      Examples:

        # Drain all workers on a specific machine
        {cli_name} machine drain 0d123123
    """,
)
@click.argument(
    "machine_id",
    nargs=1,
    required=True,
)
@extraclick.pass_service_client
def drain_machine(
    service: ServiceClient,
    machine_id: str,
):
    res = service.gateway.list_workers(ListWorkersRequest())
    if not res.ok:
        return terminal.error(f"Failed to list workers: {res.err_msg}")

    matching_workers = [w.id for w in res.workers if w.machine_id == machine_id]

    if not matching_workers:
        return terminal.error(f"No workers found for machine ID: {machine_id}")

    terminal.detail(f"Found {len(matching_workers)} workers on machine {machine_id}")

    for worker_id in matching_workers:
        res = service.gateway.drain_worker(DrainWorkerRequest(worker_id=worker_id))
        if not res.ok:
            text = res.err_msg.capitalize()
            terminal.warn(text)
        else:
            terminal.success(f"Worker {worker_id} has been drained.")


@management.command(
    name="cordon",
    help="""
    Cordon all workers on a specific machine.

    This command will find all workers running on the specified machine ID and cordon them.
    When workers are cordoned, they will not accept new container requests but will
    continue running existing containers. This is useful when you want to gracefully
    remove workers from a machine.
    """,
    epilog="""
      Examples:

        # Cordon all workers on a specific machine
        {cli_name} machine cordon 0d123123
    """,
)
@click.argument(
    "machine_id",
    nargs=1,
    required=True,
)
@extraclick.pass_service_client
def cordon_machine(
    service: ServiceClient,
    machine_id: str,
):
    res = service.gateway.list_workers(ListWorkersRequest())
    if not res.ok:
        return terminal.error(f"Failed to list workers: {res.err_msg}")

    matching_workers = [w.id for w in res.workers if w.machine_id == machine_id]

    if not matching_workers:
        return terminal.error(f"No workers found for machine ID: {machine_id}")

    terminal.detail(f"Found {len(matching_workers)} workers on machine {machine_id}")

    for worker_id in matching_workers:
        res = service.gateway.cordon_worker(CordonWorkerRequest(worker_id=worker_id))
        if not res.ok:
            text = res.err_msg.capitalize()
            terminal.warn(text)
        else:
            terminal.success(f"Worker {worker_id} has been cordoned.")
