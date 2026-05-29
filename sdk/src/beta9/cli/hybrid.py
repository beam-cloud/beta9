import click
from betterproto import Casing
from rich.table import Column, Table, box

from .. import terminal
from ..channel import ServiceClient
from ..cli import extraclick
from ..clients.gateway import (
    AttachHybridPoolRequest,
    DeleteHybridPoolRequest,
    ExtendHybridPoolRequest,
    HybridPoolConfig,
    ListHybridOffersRequest,
    ListHybridPoolsRequest,
    ReserveHybridPoolRequest,
)
from .extraclick import ClickCommonGroup, ClickManagementGroup


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@click.group(
    name="hybrid",
    help="Manage reserved GPU pools.",
    cls=ClickManagementGroup,
)
def management():
    pass


def _pool_config(
    name: str = "",
    gpu: tuple[str, ...] = (),
    gpus: int = 0,
    ttl: str = "",
    max_spend: float = 0,
    provider: tuple[str, ...] = (),
    region: tuple[str, ...] = (),
    min_reliability: float = 0,
) -> HybridPoolConfig:
    return HybridPoolConfig(
        name=name or "",
        gpu=list(gpu),
        gpus=gpus or 0,
        ttl=ttl or "",
        max_spend=max_spend or 0,
        providers=list(provider),
        regions=list(region),
        min_reliability=min_reliability or 0,
        reservation_required=bool(gpus or ttl or max_spend or provider or region or min_reliability),
        selector=name or "",
    )


@management.command(name="offers", help="List compatible reserved GPU offers.")
@click.option("--gpu", "gpu", multiple=True, required=True, help="GPU type to search for.")
@click.option("--gpus", type=click.IntRange(1), default=1, show_default=True)
@click.option("--ttl", default="1h", show_default=True)
@click.option("--max-spend", type=float, default=0)
@click.option("--provider", multiple=True, help="Restrict to a vendor such as vast or shadeform.")
@click.option("--region", multiple=True, help="Restrict to a region.")
@click.option("--min-reliability", type=click.FloatRange(0, 1), default=0)
@click.option("--format", type=click.Choice(("table", "json")), default="table", show_default=True)
@extraclick.pass_service_client
def offers(
    service: ServiceClient,
    gpu: tuple[str, ...],
    gpus: int,
    ttl: str,
    max_spend: float,
    provider: tuple[str, ...],
    region: tuple[str, ...],
    min_reliability: float,
    format: str,
):
    res = service.gateway.list_hybrid_offers(
        ListHybridOffersRequest(
            pool=_pool_config(
                gpu=gpu,
                gpus=gpus,
                ttl=ttl,
                max_spend=max_spend,
                provider=provider,
                region=region,
                min_reliability=min_reliability,
            )
        )
    )
    if not res.ok:
        return terminal.error(res.err_msg)
    if format == "json":
        terminal.print_json({"offers": [o.to_dict(casing=Casing.SNAKE) for o in res.offers]})
        return

    table = Table(
        Column("Provider"),
        Column("GPU"),
        Column("Count", justify="right"),
        Column("Instance"),
        Column("Region"),
        Column("$/hr", justify="right"),
        Column("Available", justify="right"),
        box=box.SIMPLE,
    )
    for offer in res.offers:
        table.add_row(
            offer.provider,
            offer.gpu,
            str(offer.gpu_count),
            offer.instance_type,
            offer.region or "-",
            f"{offer.hourly_cost_micros / 1_000_000:.4f}",
            str(offer.available),
        )
    terminal.print(table)


@management.command(name="reserve", help="Reserve a named GPU pool.")
@click.argument("name")
@click.option("--gpu", "gpu", multiple=True, required=True)
@click.option("--gpus", type=click.IntRange(1), required=True)
@click.option("--ttl", required=True)
@click.option("--max-spend", type=float, required=True)
@click.option("--provider", multiple=True)
@click.option("--region", multiple=True)
@click.option("--min-reliability", type=click.FloatRange(0, 1), default=0)
@extraclick.pass_service_client
def reserve(
    service: ServiceClient,
    name: str,
    gpu: tuple[str, ...],
    gpus: int,
    ttl: str,
    max_spend: float,
    provider: tuple[str, ...],
    region: tuple[str, ...],
    min_reliability: float,
):
    res = service.gateway.reserve_hybrid_pool(
        ReserveHybridPoolRequest(
            pool=_pool_config(
                name=name,
                gpu=gpu,
                gpus=gpus,
                ttl=ttl,
                max_spend=max_spend,
                provider=provider,
                region=region,
                min_reliability=min_reliability,
            )
        )
    )
    if not res.ok:
        return terminal.error(res.err_msg)
    terminal.success(f"Reserved hybrid pool '{res.pool.name}'")


@management.command(name="list", help="List reserved GPU pools.")
@click.option("--limit", type=click.IntRange(1, 100), default=20, show_default=True)
@click.option("--format", type=click.Choice(("table", "json")), default="table", show_default=True)
@extraclick.pass_service_client
def list_pools(service: ServiceClient, limit: int, format: str):
    res = service.gateway.list_hybrid_pools(ListHybridPoolsRequest(limit=limit))
    if not res.ok:
        return terminal.error(res.err_msg)
    if format == "json":
        terminal.print_json({"pools": [p.to_dict(casing=Casing.SNAKE) for p in res.pools]})
        return

    table = Table(
        Column("Name"),
        Column("Status"),
        Column("GPUs", justify="right"),
        Column("Spend", justify="right"),
        Column("Source"),
        box=box.SIMPLE,
    )
    for pool in res.pools:
        table.add_row(
            pool.name,
            pool.status,
            str(pool.reserved_gpus),
            f"${pool.committed_spend_micros / 1_000_000:.2f}",
            pool.source,
        )
    terminal.print(table)


@management.command(name="delete", help="Delete a reserved GPU pool.")
@click.argument("name")
@extraclick.pass_service_client
def delete(service: ServiceClient, name: str):
    res = service.gateway.delete_hybrid_pool(DeleteHybridPoolRequest(name=name))
    if not res.ok:
        return terminal.error(res.err_msg)
    terminal.success(f"Deleted hybrid pool '{name}'")


@management.command(name="extend", help="Extend a reserved GPU pool.")
@click.argument("name")
@click.option("--ttl", default="")
@click.option("--max-spend", type=float, default=0)
@extraclick.pass_service_client
def extend(service: ServiceClient, name: str, ttl: str, max_spend: float):
    res = service.gateway.extend_hybrid_pool(
        ExtendHybridPoolRequest(name=name, ttl=ttl, max_spend=max_spend)
    )
    if not res.ok:
        return terminal.error(res.err_msg)
    terminal.success(f"Extended hybrid pool '{name}'")


@management.command(name="attach", help="Show the command for attaching manual compute.")
@click.argument("name")
@extraclick.pass_service_client
def attach(service: ServiceClient, name: str):
    res = service.gateway.attach_hybrid_pool(AttachHybridPoolRequest(name=name))
    if not res.ok:
        return terminal.error(res.err_msg)
    terminal.detail(res.command, crop=False, overflow="ignore")

