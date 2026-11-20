"""
Interactive capacity flow.

When stub creation reports that no serverless pool supports the requested GPU
(a guaranteed scheduling blackhole), this module lets the user launch
on-demand hardware from a cloud provider offer and routes the workload onto
it — or fails fast with an actionable error in headless environments.

A call can also find, once it is made, that there is no machine for it now.
What it does then is at the end of this module: it keeps waiting, gives up
after as long as its caller allows, or has its caller reserve a machine.
"""

import math
import os
import time
from typing import TYPE_CHECKING, Callable, List, Optional

from ... import terminal
from ...channel import rpc_timeout
from ...clients.gateway import (
    GetOrCreateStubRequest,
    GetOrCreateStubResponse,
    ListPoolOffersRequest,
    ListPrivatePoolsRequest,
    PoolConfig,
    PoolOffer,
)
from ...exceptions import NoCapacityError
from ...pending import PROVISIONING, WAITING_FOR_CAPACITY, Wait, clock
from ...terminal import cli_name

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


def no_capacity_hint(gpus: List[str]) -> str:
    gpu = gpus[0] if gpus else "<gpu>"
    return (
        f"run '{cli_name()} machine reserve --gpu {gpu}' to get hardware, or pick a different GPU"
    )


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
        return f"https://platform.{host[len('app.') :]}/settings/credits"
    return None


def credit_error_hint(message: str) -> Optional[str]:
    """A purchase link for credit-related failures that do not already name one."""
    url = credits_url()
    text = (message or "").lower()
    if url and "credit" in text and "http" not in text:
        return f"purchase credits at {url}"
    return None


def launch_failure_hint(
    err_msg: str,
    error_code: str = "",
    required_cents: int = 0,
    available_cents: int = 0,
) -> Optional[str]:
    """Actionable hint for a failed capacity launch; currently credit-focused."""
    if "credit" not in (error_code or "") and not credit_error_hint(err_msg):
        return None

    parts = []
    if required_cents:
        parts.append(
            f"requires ${required_cents / 100:.2f} in credit, you have ${available_cents / 100:.2f}"
        )
    if url := credits_url():
        parts.append(f"purchase credits at {url}")
    return " — ".join(parts) or None


def offer_hardware_label(offer: PoolOffer, nodes: int = 1) -> str:
    """Human label for an offer's hardware, e.g. '2 nodes of 4x A6000'."""
    gpu_count = offer.gpu_count or 1
    hardware = (
        f"{gpu_count}x {offer.gpu}"
        if offer.gpu
        else (offer.display_name or offer.instance_type or "CPU node")
    )
    if nodes > 1:
        hardware = f"{nodes} nodes of {hardware}"
    return hardware


def no_offers_error(gpu_label: str, pricing: str = "on_demand") -> None:
    """Report that nothing is offered for a GPU; pricing is what was asked for (on_demand, spot, any)."""
    kind = {"spot": "spot ", "any": ""}.get(pricing, "on-demand ")
    terminal.error(
        f"No {kind}offers currently available for {gpu_label}.",
        exit=False,
        hint=no_offers_hint(),
    )


def _runner_interactive(runner: "RunnerAbstraction") -> bool:
    return terminal.is_interactive() and not getattr(runner, "headless", False)


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

    if _runner_interactive(runner):
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

    if not _runner_interactive(runner):
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
        no_offers_error(gpu_label)
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
            f"Launch {offer_hardware_label(offer)} for ~${hourly:.2f}/hr ({ttl_label(ttl)})?",
            default=True,
        ):
            terminal.detail("Cancelled.")
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


# How the machines of an offer are sold, as PoolConfig.pricing names it. On
# demand is the default and is sent as nothing.
PRICING_ON_DEMAND = ""
PRICING_SPOT = "spot"
PRICING_ANY = "any"


def fetch_offers(
    gateway_stub,
    gpus: List[str],
    limit: int = MAX_OFFER_CHOICES,
    pricing: str = PRICING_ON_DEMAND,
) -> Optional[List[PoolOffer]]:
    """
    Fetch offers sorted by price; None indicates a fetch error. Offers are
    on-demand unless pricing asks for spot machines, or for both ("any").
    """
    with terminal.progress("Finding available hardware..."):
        res = gateway_stub.list_pool_offers(
            ListPoolOffersRequest(pool=PoolConfig(gpu=gpus, pricing=pricing))
        )
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
    hardware = offer_hardware_label(offer)
    details = []
    if offer.spot:
        details.append("spot, can be taken back")
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
        # A machine that is sold both ways is two offers with one ID.
        pricing=PRICING_SPOT if offer.spot else PRICING_ON_DEMAND,
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


# A call that waits for a machine
#
# Most calls that wait get a machine: one starts, and the call says so while
# it does (see pending.py). The rest wait for capacity that is not there, for
# as long as the scheduler keeps trying, which is many minutes. A caller who
# would rather not wait that long can say so, and one at a terminal is asked.

# How long a call goes without capacity before its caller is asked what to
# do about it, or, where nobody can be asked, told what can be done.
ASK_AFTER_S = 60.0

_UNITS = {"s": 1, "m": 60, "h": 3600}


def wait_limit_name() -> str:
    """The environment variable that limits how long a call waits for a machine."""
    from ...config import get_settings

    return f"{get_settings().name.upper()}_CAPACITY_WAIT"


def wait_limit() -> float:
    """
    The longest a call may go without a machine, in seconds, as the
    environment sets it: a number of seconds, or a number with a unit (90s,
    5m, 1h). Nothing, or 0, is no limit.
    """
    name = wait_limit_name()
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return 0.0
    number, unit = (raw[:-1], _UNITS[raw[-1]]) if raw[-1] in _UNITS else (raw, 1)
    try:
        seconds = float(number) * unit
    except ValueError:
        seconds = math.nan
    if not math.isfinite(seconds) or seconds < 0:
        terminal.error(
            f"{name}={raw} is not a length of time.",
            hint="Use a number of seconds, or a number with a unit: 90, 90s, 5m.",
            code="INVALID_CONFIG",
        )
    return seconds


def runner_gpus(runner: "RunnerAbstraction") -> List[str]:
    """The GPU types a workload asks for, in its order of preference."""
    gpu = getattr(runner, "gpu", "") or ""
    values = gpu if isinstance(gpu, (list, tuple)) else str(gpu).split(",")
    names = [str(getattr(value, "value", value)).strip() for value in values]
    return [name for name in names if name and name != "NO_GPU"]


def capacity_label(gpus: List[str]) -> str:
    """The capacity that was asked for: "A10G capacity", "A10G or T4 capacity", "capacity"."""
    if not gpus:
        return "capacity"
    if len(gpus) == 1:
        return f"{gpus[0]} capacity"
    return f"{', '.join(gpus[:-1])} or {gpus[-1]} capacity"


def _took(seconds: float) -> str:
    if seconds < 120:
        return f"{int(seconds)} seconds"
    return f"{round(seconds / 60)} minutes"


def _reserve_command(gateway_stub, gpus: List[str]) -> str:
    """The command that reserves a machine with the first of these GPUs, and what it costs if that can be found out."""
    command = f"`{cli_name()} machine reserve --gpu {gpus[0]}`"
    try:
        with rpc_timeout(5):
            res = gateway_stub.list_pool_offers(
                ListPoolOffersRequest(pool=PoolConfig(gpu=gpus[:1]))
            )
        cheapest = min((o.hourly_cost_micros for o in res.offers if res.ok), default=0)
    except Exception:
        # What a machine costs is a help, not something to fail for.
        cheapest = 0
    if cheapest > 0:
        command += f" (from ${cheapest / 1_000_000:.2f}/hr)"
    return command


def _what_else(runner: "RunnerAbstraction", wait: Wait, gpus: List[str], price: bool = True) -> str:
    """What a caller can do other than wait. With price, what a machine costs is looked up."""
    cli = cli_name()
    if wait.reason == "no_spare_capacity":
        return (
            "The Spot tier runs on capacity the other tiers leave spare. "
            'cpu_tier="shared" or "performance" does not wait for it.'
        )
    if wait.reason == "machine_busy" or not gpus:
        return ""
    reserve = (
        _reserve_command(runner.gateway_stub, gpus)
        if price
        else f"`{cli} machine reserve --gpu {gpus[0]}`"
    )
    if len(gpus) > 1:
        more = "Add GPU types to the list"
    else:
        other = "A10G" if gpus[0] == "T4" else "T4"
        more = f'List more than one GPU type (gpu=["{gpus[0]}", "{other}"])'
    return f"{more} so that whichever has capacity is used, or reserve a machine: {reserve}."


def gave_up(runner: "RunnerAbstraction", wait: Wait, price: bool = True) -> NoCapacityError:
    """A call has gone without a machine for as long as it is allowed to: why, and what else there is."""
    gpus = runner_gpus(runner)
    took = _took(wait.seconds())
    if wait.phase == WAITING_FOR_CAPACITY and wait.reason == "no_spare_capacity":
        what = f"The Spot tier had no spare capacity for this call in {took}."
    elif wait.phase == WAITING_FOR_CAPACITY and wait.reason == "machine_busy":
        what = f"The machine this call is pinned to had no room for it in {took}."
    elif wait.phase == WAITING_FOR_CAPACITY:
        what = f"No {capacity_label(gpus)} became available in {took}."
    elif wait.phase == PROVISIONING:
        what = f"A machine for this call was still starting after {took}."
    else:
        what = f"This call was not placed on a machine in {took}."

    hint = (
        _what_else(runner, wait, gpus, price),
        f"`{cli_name()} machine list` shows what can be reserved."
        if wait.reason not in ("no_spare_capacity", "machine_busy")
        else "",
        f"{wait_limit_name()} limits the wait to {_took(wait.limit)}.",
    )
    return NoCapacityError(
        f"{what} The call was cancelled.", hint=" ".join(part for part in hint if part)
    )


def still_waiting(runner: "RunnerAbstraction", wait: Wait) -> str:
    """
    One line for a call that has gone a while without capacity and keeps
    waiting, where nobody can be asked: how long it will wait, and what can
    be done instead.
    """
    gpus = runner_gpus(runner)
    waited = clock(wait.without_capacity())
    if wait.reason == "no_spare_capacity":
        what = f"Still no spare capacity for the Spot tier after {waited}."
    elif wait.reason == "machine_busy":
        what = f"The machine this is pinned to has been full for {waited}."
    else:
        what = f"Still no {capacity_label(gpus)} after {waited}."

    if wait.limit > 0:
        keeps = f"The call waits until {clock(wait.limit)} ({wait_limit_name()})."
    else:
        keeps = f"The call keeps waiting; {wait_limit_name()}=<seconds> gives up sooner."
    return "\n".join(
        part for part in (f"{what} {keeps}", _what_else(runner, wait, gpus, False)) if part
    )


WAIT, RESERVE, CANCEL = "wait", "reserve", "cancel"


def can_reserve(runner: "RunnerAbstraction", wait: Wait) -> bool:
    """
    Whether reserving a machine would get this call one: it asked for a GPU,
    left it to the platform where to run, and there is no capacity for it.
    """
    return (
        wait.reason == "no_capacity"
        and bool(runner_gpus(runner))
        and getattr(runner, "pool_config", None) is None
        and getattr(runner, "stub_request", None) is not None
    )


def ask_while_waiting(runner: "RunnerAbstraction", wait: Wait) -> str:
    """Ask a caller at a terminal what to do about a call there is no capacity for: WAIT, RESERVE or CANCEL."""
    gpus = runner_gpus(runner)
    waited = clock(wait.without_capacity())
    if wait.reason == "no_spare_capacity":
        terminal.warn(f"The Spot tier has had no spare capacity for {waited}.")
    elif wait.reason == "machine_busy":
        terminal.warn(f"The machine this is pinned to has been full for {waited}.")
    else:
        terminal.warn(
            f"No {capacity_label(gpus)} has been available for {waited}. "
            "This is a platform limit, not your code."
        )
    if advice := _what_else(runner, wait, gpus) if not can_reserve(runner, wait) else "":
        terminal.detail(f"  {advice}")

    options = [
        terminal.SelectOption(
            label="Keep waiting",
            value=WAIT,
            description="runs as soon as there is capacity",
        )
    ]
    if can_reserve(runner, wait):
        options.append(
            terminal.SelectOption(
                label="Reserve a machine",
                value=RESERVE,
                description="on demand, billed by the hour; runs this call on it",
            )
        )
    options.append(
        terminal.SelectOption(label="Cancel", value=CANCEL, description="the function does not run")
    )
    try:
        return terminal.select("How do you want to proceed?", options)
    except KeyboardInterrupt:
        return CANCEL


def reserve_while_waiting(
    runner: "RunnerAbstraction", stop_call: Callable[[], None]
) -> Optional[bool]:
    """
    Have the caller reserve a machine for a call that waits, and point the
    workload at it. stop_call gives up the call that waits; it is called
    once a machine is reserved, so that the call cannot run twice.

    True when the machine is ready and the call can be made again, None when
    nothing was reserved and the call still waits, False when a machine was
    reserved and is not ready (which has been reported).
    """
    gpus = runner_gpus(runner)
    response = _run_interactive_capacity_flow(
        runner,
        runner.stub_request,
        gpus,
        ", ".join(gpus),
        announce=False,
        wait_for_ready=False,
    )
    if response is None:
        return None
    if not response.ok:
        # No machine was reserved, and why is reported. The call that waits
        # is still the way to run.
        runner._apply_stub_response(response)
        runner.stub_request.pool = None
        return None

    stop_call()
    if not runner._apply_stub_response(response):
        return False
    return wait_for_pool_ready(runner.gateway_stub, runner.pool_config.name)
