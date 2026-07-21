from typing import Any, Dict, List, Optional, Sequence

import click
from betterproto import Casing

from .. import terminal
from ..abstractions.base.capacity import (
    DEFAULT_ONDEMAND_TTL,
    INDEFINITE_TTL,
    cli_name,
    credit_error_hint,
    credits_url,
    fetch_offers,
    no_offers_hint,
    offer_options,
    pool_config_for_offer,
    select_ttl,
    ttl_label,
    valid_ttl,
    wait_for_pool_ready,
)
from ..abstractions.pod import Pod
from ..channel import ServiceClient
from ..cli import extraclick
from ..clients.gateway import (
    CreateMachineRequest,
    CreateMachineResponse,
    DeleteMachineRequest,
    DeleteMachineResponse,
    DeletePoolRequest,
    LaunchPoolCapacityRequest,
    LaunchPoolCapacityResponse,
    ListContainersRequest,
    ListMachinesRequest,
    ListMachinesResponse,
    ListPrivatePoolsRequest,
    PoolOffer,
    PrivatePool,
)
from .extraclick import ClickCommonGroup, ClickManagementGroup
from .machine_format import gpu_inventory_table, machine_table
from .worker_management import apply_worker_action, worker_ids_for_machine


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@click.group(
    name="machine",
    help="Browse GPU inventory, reserve and release on-demand hardware, and operate machines.",
    cls=ClickManagementGroup,
)
def management():
    pass


def _cheapest_offers_by_gpu(offers: Sequence[PoolOffer]) -> Dict[str, PoolOffer]:
    cheapest: Dict[str, PoolOffer] = {}
    for offer in offers:
        if not offer.gpu:
            continue
        current = cheapest.get(offer.gpu)
        if current is None or offer.hourly_cost_micros < current.hourly_cost_micros:
            cheapest[offer.gpu] = offer
    return cheapest


def _section_header(text: str) -> str:
    return f"[bold {terminal.BRAND_COLOR}]=> {text}[/bold {terminal.BRAND_COLOR}]"


def _machine_list_renderables(
    res: ListMachinesResponse,
    offers: Optional[Sequence[PoolOffer]],
) -> List[Any]:
    renderables: List[Any] = [
        _section_header("GPU inventory"),
        gpu_inventory_table(
            res.gpus,
            res.supported_gpus,
            _cheapest_offers_by_gpu(offers or []),
            offers_available=offers is not None,
        ),
        "",
        _section_header("Your machines"),
    ]
    if res.machines:
        renderables.append(machine_table(res.machines))
    else:
        renderables.append(
            f"[dim]  none yet — try '{cli_name()} machine reserve --gpu <type>'[/dim]"
        )
    return renderables


def _fetch_offers_quiet(service: ServiceClient) -> List[PoolOffer]:
    """Fetch offers without any decorative output, for JSON consumers."""
    from ..clients.gateway import ListPoolOffersRequest, PoolConfig

    res = service.gateway.list_pool_offers(ListPoolOffersRequest(pool=PoolConfig()))
    return list(res.offers) if res.ok else []


def _inventory_json(
    res: ListMachinesResponse, offers: Optional[Sequence[PoolOffer]]
) -> List[Dict[str, Any]]:
    cheapest = _cheapest_offers_by_gpu(offers or [])
    inventory = []
    for gpu_type in sorted(set(res.gpus) | set(res.supported_gpus) | set(cheapest)):
        if res.gpus.get(gpu_type):
            serverless = "ready"
        elif res.supported_gpus.get(gpu_type):
            serverless = "available"
        else:
            serverless = "none"
        offer = cheapest.get(gpu_type)
        inventory.append(
            {
                "gpu": gpu_type,
                "serverless": serverless,
                "on_demand": offer.to_dict(casing=Casing.SNAKE) if offer else None,  # type: ignore
            }
        )
    return inventory


@management.command(
    name="list",
    help="Show GPU inventory (serverless + on-demand pricing) and your machines.",
    epilog="""
    Examples:

      # List the first 10 machines
      {cli_name} machine list --limit 10

      # Skip fetching on-demand offer pricing (faster)
      {cli_name} machine list --no-offers

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
@click.option(
    "--offers/--no-offers",
    "show_offers",
    default=True,
    show_default=True,
    help="Include on-demand offer pricing in the inventory.",
)
@extraclick.pass_service_client
def list_machines(
    service: ServiceClient,
    limit: int,
    format: str,
    pool: str,
    show_offers: bool,
):
    res: ListMachinesResponse
    res = service.gateway.list_machines(ListMachinesRequest(pool_name=pool, limit=limit))

    if not res.ok:
        terminal.error(res.err_msg)

    if format == "json":
        offers = _fetch_offers_quiet(service) if show_offers else []
        machines = [d.to_dict(casing=Casing.SNAKE) for d in res.machines]  # type:ignore
        terminal.print_json(
            {
                "inventory": _inventory_json(res, offers),
                "machines": machines,
            }
        )
        return

    offers: Optional[List[PoolOffer]] = []
    if show_offers:
        offers = fetch_offers(service.gateway, [], limit=0)

    for renderable in _machine_list_renderables(res, offers):
        terminal.print(renderable)

    if res.machines:
        terminal.detail(
            f"  hint: reserve on-demand hardware with '{cli_name()} machine reserve --gpu <type>'"
        )


@management.command(
    name="ssh",
    help="Open a shell container on a reserved machine.",
    epilog="""
    Examples:

      {cli_name} machine ssh 9a8b7c6d
      \b
    """,
)
@click.argument("machine_id", required=True)
@extraclick.pass_service_client
def ssh_machine(service: ServiceClient, machine_id: str):
    machines_res = service.gateway.list_machines(ListMachinesRequest(limit=100))
    if not machines_res.ok:
        return terminal.error(machines_res.err_msg)

    machine = next((m for m in machines_res.machines if m.id == machine_id), None)
    if machine is None:
        return terminal.error(f"Machine '{machine_id}' was not found.")
    if machine.status.lower() not in ("available", "ready"):
        return terminal.error(f"Machine '{machine_id}' is {machine.status or 'not ready'}.")

    containers_res = service.gateway.list_containers(ListContainersRequest())
    if not containers_res.ok:
        return terminal.error(containers_res.error_msg)
    containers = [
        c
        for c in containers_res.containers
        if c.machine_id == machine_id and c.status.upper() == "RUNNING"
    ]

    if containers:
        options = [
            terminal.SelectOption(
                label=c.container_id,
                value=c.container_id,
                description=f"existing {c.stub_id or 'container'}",
            )
            for c in containers
        ]
        options.append(
            terminal.SelectOption(
                label="New shell container",
                value="",
                description="uses the machine's currently free hardware",
            )
        )
        container_id = terminal.select("Where do you want to connect?", options)
        if container_id:
            return Pod().shell(container_id=container_id)

    free_gpu_count = max(0, int(machine.machine_metrics.free_gpu_count))
    pod = Pod(
        pool=machine.pool_name,
        gpu=machine.gpu if free_gpu_count > 0 else "",
        gpu_count=free_gpu_count,
    )
    return pod.shell(machine_id=machine_id)


@management.command(
    name="reserve",
    help="Reserve on-demand hardware. Interactive in a terminal; scriptable "
    "(picks the cheapest offer) when piped or given --yes.",
    epilog="""
    Examples:

      # Pick from the cheapest available A6000 offers
      {cli_name} machine reserve --gpu A6000

      # Script/agent friendly: cheapest A6000 for 2h, no prompts
      {cli_name} machine reserve --gpu A6000 --ttl 2h --yes

      # Reserve two nodes until released, in a named pool
      {cli_name} machine reserve --gpu H100 --nodes 2 --ttl indefinite --name training
      \b
    """,
)
@click.option("--gpu", "gpu", multiple=True, help="GPU type to reserve (repeatable).")
@click.option("--nodes", type=click.IntRange(1), default=1, show_default=True)
@click.option(
    "--ttl",
    default=None,
    help="Reservation duration, e.g. 1h, 6h, 2d, or 'indefinite' for manual spindown. "
    "Prompted when omitted.",
)
@click.option("--name", default="", help="Pool name (defaults to ondemand-<gpu>).")
@click.option("--max-spend", type=float, default=0.0, help="Max spend in USD (auto if omitted).")
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    help="Skip prompts: pick the cheapest offer and reserve immediately.",
)
@extraclick.pass_service_client
def reserve_machine(
    service: ServiceClient,
    gpu: Sequence[str],
    nodes: int,
    ttl: Optional[str],
    name: str,
    max_spend: float,
    yes: bool,
):
    if ttl is not None and ttl.strip().lower() in ("indefinite", "none", "manual"):
        ttl = INDEFINITE_TTL
    if ttl is not None and not valid_ttl(ttl):
        return terminal.error(
            f"Invalid --ttl '{ttl}'.",
            hint="use a number with a unit (45m, 6h, 2d) or 'indefinite'",
        )

    offers = fetch_offers(service.gateway, list(gpu), limit=15)
    if offers is None:
        return
    if not offers:
        gpu_label = ", ".join(gpu) if gpu else "your request"
        return terminal.error(
            f"No on-demand offers currently available for {gpu_label}.",
            hint=no_offers_hint(),
        )

    # Scripts and agents get deterministic behavior: cheapest offer, no prompts.
    interactive = terminal.is_interactive() and not yes
    if interactive:
        offer = terminal.select("Which hardware do you want to reserve?", offer_options(offers))
    else:
        offer = offers[0]

    hourly = offer.hourly_cost_micros / 1_000_000
    if ttl is None:
        ttl = select_ttl(hourly, nodes) if interactive else DEFAULT_ONDEMAND_TTL

    pool = pool_config_for_offer(offer, name=name, nodes=nodes, ttl=ttl, max_spend=max_spend)
    hardware = f"{offer.gpu_count or 1}x {offer.gpu}" if offer.gpu else offer.instance_type
    if nodes > 1:
        hardware = f"{nodes} nodes of {hardware}"
    summary = (
        f"Reserve {hardware} for ~${hourly * nodes:.2f}/hr? "
        f"{ttl_label(ttl).capitalize()}, max spend ${pool.max_spend:.2f}."
    )
    if interactive:
        if not terminal.confirm(summary, default=True):
            return
    else:
        terminal.detail(summary.replace("Reserve", "Reserving", 1).replace("?", ".", 1))

    res: LaunchPoolCapacityResponse
    res = service.gateway.launch_pool_capacity(LaunchPoolCapacityRequest(pool=pool, nodes=nodes))
    if not res.ok:
        hint = None
        if "credit" in (res.error_code or "") or credit_error_hint(res.err_msg):
            parts = []
            if res.required_cents:
                parts.append(
                    f"requires ${res.required_cents / 100:.2f} in credit, "
                    f"you have ${res.available_cents / 100:.2f}"
                )
            if url := credits_url():
                parts.append(f"purchase credits at {url}")
            hint = " — ".join(parts) or None
        return terminal.error(f"Failed to reserve capacity: {res.err_msg}", hint=hint)

    if not wait_for_pool_ready(service.gateway, pool.name):
        return

    terminal.done(f"Reserved '{pool.name}'")
    terminal.detail(f'  run on it:    @function(gpu="{offer.gpu}", pool="{pool.name}")')
    terminal.detail(f"  check status: {cli_name()} machine list")
    terminal.detail(f"  release:      {cli_name()} machine release")


def _release_candidates(pools: Sequence[PrivatePool]) -> List[PrivatePool]:
    return [
        pool
        for pool in pools
        if pool.reservations or pool.machine_count > 0 or pool.reserved_nodes > 0
    ]


def _release_options(pools: Sequence[PrivatePool]) -> List[terminal.SelectOption]:
    rows = []
    for pool in pools:
        gpu = "cpu"
        if pool.config and pool.config.gpu:
            gpu = pool.config.gpu[0]
        count = max(pool.machine_count, pool.reserved_nodes, len(pool.reservations))
        machines = f"{count} machine{'s' if count != 1 else ''}"
        rows.append((pool.name, gpu, machines, _release_expiry(pool)))

    widths = [max(len(row[i]) for row in rows) for i in range(3)] if rows else [0, 0, 0]
    return [
        terminal.SelectOption(
            label=f"{name:<{widths[0]}}  {gpu:<{widths[1]}}  {machines:<{widths[2]}}",
            value=name,
            description=expiry,
        )
        for name, gpu, machines, expiry in rows
    ]


def _release_expiry(pool: PrivatePool) -> str:
    from datetime import datetime, timezone

    expires_at = getattr(pool, "expires_at", None)
    # betterproto renders unset timestamps as the epoch, not None.
    if not expires_at or expires_at.year <= 1970:
        return "manual release"

    remaining = expires_at - datetime.now(timezone.utc)
    if remaining.total_seconds() <= 0:
        return "expiring now"
    return f"expires in {terminal.humanize_duration(remaining)}"


def _release_pool(service: ServiceClient, pool_name: str, yes: bool):
    if not yes and not terminal.confirm(
        f"Release '{pool_name}'? Its machines shut down and billing stops.", default=True
    ):
        return

    res = service.gateway.delete_pool(DeletePoolRequest(name=pool_name))
    if not res.ok:
        return terminal.error(f"Failed to release '{pool_name}': {res.err_msg}")
    terminal.done(f"Released '{pool_name}'")


@management.command(
    name="release",
    help="Release reserved on-demand hardware and stop billing.",
    epilog="""
    Examples:

      # Pick what to release interactively
      {cli_name} machine release

      # Release everything reserved under a pool
      {cli_name} machine release --pool ondemand-a6000

      # Release a single machine by ID
      {cli_name} machine release 9a8b7c6d
      \b
    """,
)
@click.argument("machine_id", required=False)
@click.option("--pool", "-p", help="Release all reserved machines in this pool.")
@click.option("--yes", "-y", is_flag=True, help="Skip the confirmation prompt.")
@extraclick.pass_service_client
def release_machine(service: ServiceClient, machine_id: Optional[str], pool: str, yes: bool):
    if machine_id:
        res: DeleteMachineResponse
        res = service.gateway.delete_machine(
            DeleteMachineRequest(machine_id=machine_id, pool_name=pool or "")
        )
        if not res.ok:
            return terminal.error(f"Failed to release machine '{machine_id}': {res.err_msg}")
        return terminal.done(f"Released machine '{machine_id}'")

    if pool:
        return _release_pool(service, pool, yes)

    pools_res = service.gateway.list_private_pools(ListPrivatePoolsRequest())
    if not pools_res.ok:
        return terminal.error(pools_res.err_msg)

    candidates = _release_candidates(pools_res.pools)
    if not candidates:
        return terminal.detail("Nothing is reserved right now.")

    if len(candidates) == 1:
        return _release_pool(service, candidates[0].name, yes)

    chosen = terminal.select("What do you want to release?", _release_options(candidates))
    _release_pool(service, chosen, yes)


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
    help="Delete a machine record. For reserved on-demand hardware, prefer 'machine release'.",
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
