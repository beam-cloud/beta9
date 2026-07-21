"""
Interactive capacity flow.

When stub creation reports that no serverless pool supports the requested GPU
(a guaranteed scheduling blackhole), this module lets the user launch
on-demand hardware from the compute marketplace and routes the workload onto
it — or fails fast with an actionable error in headless environments.
"""

import time
from typing import TYPE_CHECKING, List, Optional

from ... import terminal
from ...clients.gateway import (
    GetOrCreateStubRequest,
    GetOrCreateStubResponse,
    ListPoolOffersRequest,
    ListPrivatePoolsRequest,
    PoolConfig,
    PoolOffer,
)

if TYPE_CHECKING:
    from .runner import RunnerAbstraction

CAPACITY_STATUS_AVAILABLE = "available"
CAPACITY_STATUS_LOW = "low"
CAPACITY_STATUS_NONE = "none"

DEFAULT_ONDEMAND_TTL = "1h"
DEFAULT_ONDEMAND_NODES = 1
# "Indefinite" reservations: the control plane requires a TTL and spend cap
# on every reservation, so manual-spindown mode reserves a 30-day window the
# user releases early with 'beta9 machine release' (or extends with
# 'beta9 pool extend').
INDEFINITE_TTL = "720h"
MAX_OFFER_CHOICES = 10
PROVISION_TIMEOUT_S = 15 * 60
PROVISION_POLL_INTERVAL_S = 5


def requested_gpus(stub_request: GetOrCreateStubRequest) -> List[str]:
    return [g for g in (stub_request.gpu or "").split(",") if g and g != "NO_GPU"]


def cli_name() -> str:
    """The installed CLI executable name ("beta9" or "beam"), for command hints."""
    from ...config import get_settings

    return (get_settings().name or "beta9").lower()


def no_capacity_hint(gpus: List[str]) -> str:
    gpu = gpus[0] if gpus else "<gpu>"
    return f"run '{cli_name()} machine reserve --gpu {gpu}' to get hardware, or pick a different GPU"


def no_offers_hint() -> str:
    cli = cli_name()
    return (
        f"browse available GPUs with '{cli} machine list', "
        f"or attach your own machine with '{cli} pool join'"
    )


def credits_url() -> Optional[str]:
    """
    The dashboard credits page, derived from the SDK's API host so the link
    follows the environment (app.beam.cloud -> platform.beam.cloud,
    app.stage.beam.cloud -> platform.stage.beam.cloud). Self-hosted installs
    have no credits page, so hosts without the app. prefix return None.
    """
    from ...config import get_settings

    host = (get_settings().api_host or "").split(":")[0]
    if host.startswith("app."):
        return f"https://platform.{host[len('app.'):]}/settings/credits"
    return None


def credit_error_hint(message: str) -> Optional[str]:
    """A purchase link for credit-related failures; None for everything else."""
    url = credits_url()
    if url and "credit" in (message or "").lower():
        return f"purchase credits at {url}"
    return None


def handle_capacity_verdict(
    runner: "RunnerAbstraction",
    stub_request: GetOrCreateStubRequest,
    stub_response: GetOrCreateStubResponse,
    stub_type: str,
) -> Optional[GetOrCreateStubResponse]:
    """
    Reacts to the capacity verdict on a stub creation response. Returns the
    response to continue with, or None when the workload should not proceed.
    """
    if not stub_response.ok:
        return stub_response

    # An explicit pool config is the user's placement choice; don't second-guess it.
    if runner.pool_config is not None:
        return stub_response

    if stub_response.matched_private_pool:
        return _attach_to_private_pool(runner, stub_request, stub_response)

    if stub_response.capacity_status != CAPACITY_STATUS_NONE:
        return stub_response

    gpus = requested_gpus(stub_request)
    gpu_label = ", ".join(gpus) if gpus else "the requested GPU"

    if stub_type.endswith("/deployment"):
        return _handle_deployment_capacity(runner, stub_request, stub_response, gpus, gpu_label)

    if terminal.is_interactive() and not getattr(runner, "headless", False):
        return _run_interactive_capacity_flow(
            runner,
            stub_request,
            gpus,
            gpu_label,
            wait_for_ready=stub_type != "pod/run",
        )

    terminal.error(
        f"No compute capacity supports {gpu_label}, so this workload can never be scheduled.",
        exit=False,
        hint=no_capacity_hint(gpus),
    )
    return None


def _handle_deployment_capacity(
    runner: "RunnerAbstraction",
    stub_request: GetOrCreateStubRequest,
    stub_response: GetOrCreateStubResponse,
    gpus: List[str],
    gpu_label: str,
) -> Optional[GetOrCreateStubResponse]:
    """
    Deployments are long-lived and capacity is dynamic. Interactively the
    best path is to reserve on-demand hardware and pin the deployment to
    that pool; alternatively the user can deploy anyway (requests fail fast
    with a clear reason until capacity is attached, then start succeeding
    with no redeploy). Headless deploys warn and proceed — CI never blocks.
    """
    terminal.warn(f"No compute capacity currently supports {gpu_label}.")

    if not terminal.is_interactive() or getattr(runner, "headless", False):
        terminal.detail(f"  hint: {no_capacity_hint(gpus)}")
        return stub_response

    try:
        choice = terminal.select(
            "How do you want to proceed?",
            [
                terminal.SelectOption(
                    label="Reserve on-demand hardware",
                    value="reserve",
                    description="pins this deployment to the reserved machine",
                ),
                terminal.SelectOption(
                    label="Deploy anyway",
                    value="deploy",
                    description="requests fail until capacity is attached",
                ),
                terminal.SelectOption(label="Cancel", value="cancel"),
            ],
        )
    except KeyboardInterrupt:
        choice = "cancel"

    if choice == "reserve":
        return _run_interactive_capacity_flow(runner, stub_request, gpus, gpu_label, announce=False)
    if choice == "cancel":
        # Cancelling is not a failure.
        terminal.detail("Deployment cancelled.")
        raise SystemExit(0)
    return stub_response


def _attach_to_private_pool(
    runner: "RunnerAbstraction",
    stub_request: GetOrCreateStubRequest,
    stub_response: GetOrCreateStubResponse,
) -> GetOrCreateStubResponse:
    pool_name = stub_response.matched_private_pool
    pool = PoolConfig(name=pool_name, selector=pool_name)

    terminal.print(
        f"[bold {terminal.BRAND_COLOR}]=>[/bold {terminal.BRAND_COLOR}] "
        f"No serverless capacity for this GPU — using your pool [bold]{pool_name}[/bold]"
    )

    stub_request.pool = pool
    response = runner.gateway_stub.get_or_create_stub(stub_request)
    if response.ok:
        runner.pool_config = pool
    return response


def _run_interactive_capacity_flow(
    runner: "RunnerAbstraction",
    stub_request: GetOrCreateStubRequest,
    gpus: List[str],
    gpu_label: str,
    announce: bool = True,
    wait_for_ready: bool = True,
) -> Optional[GetOrCreateStubResponse]:
    if announce:
        terminal.warn(f"No serverless capacity for {gpu_label}.")

    offers = fetch_offers(runner.gateway_stub, gpus)
    if offers is None:
        return None
    if not offers:
        terminal.error(
            f"No on-demand offers currently available for {gpu_label}.",
            exit=False,
            hint=no_offers_hint(),
        )
        return None

    try:
        offer = terminal.select(
            "Launch on-demand hardware to run this workload?",
            offer_options(offers),
        )
        if offer is None:
            return None

        hourly = offer.hourly_cost_micros / 1_000_000
        ttl = select_ttl(hourly)
        if not terminal.confirm(
            f"Launch {offer.gpu_count or 1}x {offer.gpu} for ~${hourly:.2f}/hr ({ttl_label(ttl)})?",
            default=True,
        ):
            return None
    except KeyboardInterrupt:
        terminal.detail("Cancelled.")
        return None

    pool = pool_config_for_offer(offer, ttl=ttl)
    stub_request.pool = pool

    response = runner.gateway_stub.get_or_create_stub(stub_request)
    if not response.ok:
        return response
    runner.pool_config = pool

    if wait_for_ready and not wait_for_pool_ready(runner.gateway_stub, pool.name):
        return None
    return response


def fetch_offers(
    gateway_stub, gpus: List[str], limit: int = MAX_OFFER_CHOICES
) -> Optional[List[PoolOffer]]:
    """Fetch on-demand offers sorted by price; None indicates a fetch error."""
    with terminal.progress("Finding available hardware..."):
        res = gateway_stub.list_pool_offers(ListPoolOffersRequest(pool=PoolConfig(gpu=gpus)))
    if not res.ok:
        terminal.error(f"Failed to list hardware offers: {res.err_msg}", exit=False)
        return None

    offers = list(res.offers)
    offers.sort(key=lambda o: o.hourly_cost_micros)
    if limit > 0:
        offers = offers[:limit]
    return offers


def _offer_columns(offer: PoolOffer) -> tuple:
    hourly = offer.hourly_cost_micros / 1_000_000
    region = offer.region_display_name or offer.region or "any region"
    gpu_count = offer.gpu_count or 1
    hardware = (
        f"{gpu_count}x {offer.gpu}"
        if offer.gpu
        else (offer.display_name or offer.instance_type or "CPU node")
    )
    details = []
    if offer.cpu_millicores:
        details.append(f"{offer.cpu_millicores // 1000} vCPU")
    if offer.memory_mb:
        details.append(f"{offer.memory_mb // 1024}GB RAM")
    if offer.reliability:
        details.append(f"{offer.reliability * 100:.0f}% reliability")
    return hardware, region, f"${hourly:.2f}/hr", ", ".join(details)


def offer_options(offers: List[PoolOffer]) -> List[terminal.SelectOption]:
    """
    Build select options with the hardware/region/price columns padded to
    equal widths across all offers, so the picker reads like a table.
    """
    rows = [_offer_columns(o) for o in offers]
    widths = [max(len(row[i]) for row in rows) for i in range(3)] if rows else [0, 0, 0]
    return [
        terminal.SelectOption(
            label=f"{hardware:<{widths[0]}}  {region:<{widths[1]}}  {price:>{widths[2]}}",
            value=offer,
            description=details,
        )
        for offer, (hardware, region, price, details) in zip(offers, rows)
    ]


def pool_config_for_offer(
    offer: PoolOffer,
    name: str = "",
    nodes: int = DEFAULT_ONDEMAND_NODES,
    ttl: str = DEFAULT_ONDEMAND_TTL,
    max_spend: float = 0.0,
) -> PoolConfig:
    hourly = offer.hourly_cost_micros / 1_000_000
    if max_spend <= 0:
        # Cover the TTL with headroom so billing reconciliation never kills
        # the node mid-session; the TTL is what actually bounds the spend.
        # Long/indefinite reservations get a slimmer buffer so the cap stays
        # a sane guardrail rather than a blank check.
        hours = max(1.0, ttl_hours(ttl))
        buffer = 2.0 if hours <= 24 else 1.25
        max_spend = max(1.0, round(hourly * nodes * hours * buffer, 2))
    pool_name = _sanitize_pool_name(name or f"ondemand-{offer.gpu or 'cpu'}")
    return PoolConfig(
        name=pool_name,
        selector=pool_name,
        gpu=[offer.gpu] if offer.gpu else [],
        nodes=nodes,
        ttl=ttl,
        max_spend=max_spend,
        providers=[offer.provider] if offer.provider else [],
        offer_id=offer.id,
    )


def ttl_hours(ttl: str) -> float:
    value = (ttl or "").strip().lower()
    try:
        if value.endswith("h"):
            return float(value[:-1])
        if value.endswith("m"):
            return float(value[:-1]) / 60
        if value.endswith("d"):
            return float(value[:-1]) * 24
        return float(value)
    except ValueError:
        return 1.0


def ttl_label(ttl: str) -> str:
    if ttl == INDEFINITE_TTL:
        return "runs until you release it (30d cap)"
    return f"expires in {ttl}"


def valid_ttl(ttl: str) -> bool:
    value = (ttl or "").strip().lower()
    if len(value) < 2 or value[-1] not in "mhd":
        return False
    try:
        return float(value[:-1]) > 0
    except ValueError:
        return False


TTL_CHOICES = ["1h", "2h", "4h", "8h", "24h"]


def select_ttl(hourly: float, nodes: int = 1, default: str = DEFAULT_ONDEMAND_TTL) -> str:
    """
    Interactive duration picker with the projected cost per choice. Falls
    back to the default duration when the terminal isn't interactive.
    """
    if not terminal.is_interactive():
        return default

    options = [
        terminal.SelectOption(
            label="until I stop it",
            value=INDEFINITE_TTL,
            description=f"~${hourly * nodes:.2f}/hr until '{cli_name()} machine release' (30d cap)",
        )
    ]
    for ttl in TTL_CHOICES:
        cost = hourly * nodes * ttl_hours(ttl)
        options.append(terminal.SelectOption(label=f"{ttl:<12}  ~${cost:.2f}", value=ttl))
    options.append(terminal.SelectOption(label="other", value="", description="custom duration"))

    choice = terminal.select("How long do you need it?", options)
    if choice:
        return choice

    while True:
        raw = str(terminal.prompt(text="Duration (e.g. 45m, 6h, 2d)", default=default))
        if valid_ttl(raw):
            return raw
        terminal.warn("Use a number with a unit: 45m, 6h, or 2d.")


def _sanitize_pool_name(value: str) -> str:
    out = "".join(c if (c.isalnum() or c in "-._") else "-" for c in value.lower())
    return out.strip("-._") or "ondemand"


def wait_for_pool_ready(
    gateway_stub,
    pool_name: str,
    timeout_s: int = PROVISION_TIMEOUT_S,
) -> bool:
    """
    Block with live progress until the pool has at least one ready machine.
    Milestones (reservation → booting → ready) print with elapsed times as
    they are observed.
    """
    start = time.monotonic()
    milestones_seen = set()

    def milestone(name: str, label: str):
        if name in milestones_seen:
            return
        milestones_seen.add(name)
        terminal.print(
            f"[bold green]✓[/bold green] {label} [dim]({time.monotonic() - start:.0f}s)[/dim]"
        )

    try:
        with terminal.progress("Provisioning node...") as status:
            while time.monotonic() - start < timeout_s:
                pool = _find_pool(gateway_stub, pool_name)
                if pool is not None:
                    if pool.reservations or pool.reserved_nodes > 0:
                        milestone("reserved", "Node reserved")
                        status.update("Waiting for instance to boot...")
                    if pool.machine_count > 0:
                        milestone("joined", "Machine online, agent joined")
                        status.update("Waiting for machine to become ready...")
                    if pool.ready_machine_count > 0:
                        milestone("ready", "Machine ready")
                        return True
                time.sleep(PROVISION_POLL_INTERVAL_S)
    except KeyboardInterrupt:
        terminal.warn(f"Interrupted; pool '{pool_name}' keeps provisioning in the background.")
        terminal.detail(f"  hint: check progress with '{cli_name()} pool machines {pool_name}'")
        return False

    terminal.error(
        f"Timed out waiting for pool '{pool_name}' to become ready; it may still be provisioning.",
        exit=False,
        hint=f"check progress with '{cli_name()} pool machines {pool_name}' and re-run once a machine is ready",
    )
    return False


def _find_pool(gateway_stub, pool_name: str):
    res = gateway_stub.list_private_pools(ListPrivatePoolsRequest())
    if not res.ok:
        return None
    for pool in res.pools:
        if pool.name == pool_name or pool.selector == pool_name:
            return pool
    return None
