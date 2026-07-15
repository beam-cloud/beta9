from typing import Any, Dict, List, Sequence

import click
from betterproto import Casing

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
    Machine,
)
from .extraclick import ClickCommonGroup, ClickManagementGroup
from .machine_format import gpu_availability_table, machine_table
from .worker_management import apply_worker_action, worker_ids_for_machine


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@click.group(
    name="machine",
    help="Manage machines visible to the current profile.",
    cls=ClickManagementGroup,
)
def management():
    pass


def _machine_list_renderables(gpus: Dict[str, bool], machines: Sequence[Machine]) -> List[Any]:
    return [gpu_availability_table(gpus), machine_table(machines)]


@management.command(
    name="list",
    help="List control-plane or private/BYOC machines for the current profile.",
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
        res.gpus = {gpu: res.gpus[gpu] for gpu in sorted(res.gpus)}
        terminal.print_json({"machines": machines, "gpus": res.gpus})
        return

    for renderable in _machine_list_renderables(res.gpus, res.machines):
        terminal.print(renderable)


@management.command(
    name="create",
    help="Create a machine in a cluster-managed pool.",
    epilog="""
      For private hardware, use `pool join`. For managed BYOC capacity, use
      `pool scale`.

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

    if res.install_command:
        terminal.header(
            f"Created machine with ID: '{res.machine.id}'. Run the following command on the node:"
        )
        terminal.detail(res.install_command, crop=False, overflow="ignore")
        terminal.detail(
            "Leave --worker-image unchanged to follow cluster rollouts, or edit its tag to pin this machine to a test build."
        )
        return

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

    if res.agent_upstream_token:
        cmd_args.append(f'--flux-github-token "{res.agent_upstream_token}"')

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

        # Private/BYOC machines are resolved in the current workspace
        {cli_name} machine delete my-machine-id

        # Control-plane machines require their pool
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
    help="Pool name. Required for control-plane machines; inferred for private/BYOC machines.",
)
@extraclick.pass_service_client
def delete_machine(service: ServiceClient, machine_id: str, pool: str):
    res: DeleteMachineResponse
    res = service.gateway.delete_machine(
        DeleteMachineRequest(machine_id=machine_id, pool_name=pool)
    )
    if res.ok:
        location = f" from pool '{pool}'" if pool else ""
        terminal.success(f"Deleted machine '{machine_id}'{location}")
    else:
        terminal.error(f"Error: {res.err_msg}")


@management.command(
    name="drain",
    help="Cordon every worker on a machine and stop its active containers.",
    epilog="""
      Drained workers remain cordoned. Run `machine uncordon` after maintenance.

      Example:

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
    apply_worker_action(service, worker_ids_for_machine(service, machine_id), "drain")


@management.command(
    name="cordon",
    help="Stop new scheduling on every worker on a machine.",
    epilog="""
      Example:

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
    apply_worker_action(service, worker_ids_for_machine(service, machine_id), "cordon")


@management.command(
    name="uncordon",
    help="Return every explicitly cordoned worker on a machine to scheduling.",
    epilog="""
      Example:

        {cli_name} machine uncordon 0d123123
    """,
)
@click.argument("machine_id", nargs=1, required=True)
@extraclick.pass_service_client
def uncordon_machine(service: ServiceClient, machine_id: str):
    apply_worker_action(service, worker_ids_for_machine(service, machine_id), "uncordon")
