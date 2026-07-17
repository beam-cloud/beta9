from datetime import datetime, timedelta, timezone
from io import StringIO

from rich.console import Console

from beta9.cli.machine import (
    _cheapest_offers_by_gpu,
    _machine_list_renderables,
    _release_candidates,
    _release_expiry,
    _release_options,
)
from beta9.cli.machine_format import gpu_inventory_table
from beta9.clients.gateway import (
    ListMachinesResponse,
    Machine,
    MachineMetrics,
    PoolConfig,
    PoolOffer,
    PrivatePool,
    ProviderInstance,
)


def _render(renderables) -> str:
    output = StringIO()
    console = Console(file=output, force_terminal=False, width=160)
    for renderable in renderables:
        console.print(renderable)
    return output.getvalue()


def test_machine_list_shows_inventory_and_machines():
    res = ListMachinesResponse(
        ok=True,
        gpus={"A10G": True, "A6000": False},
        supported_gpus={"A10G": True, "T4": True, "A6000": False},
        machines=[
            Machine(
                id="machine-one",
                cpu=4000,
                memory=8192,
                gpu="A10G",
                gpu_count=1,
                status="available",
                pool_name="gpu-pool",
                machine_metrics=MachineMetrics(free_gpu_count=1),
            )
        ],
    )
    offers = [
        PoolOffer(id="o1", provider="vast", gpu="A6000", hourly_cost_micros=450_000),
        PoolOffer(id="o2", provider="shadeform", gpu="A6000", hourly_cost_micros=620_000),
    ]

    text = _render(_machine_list_renderables(res, offers))

    assert "GPU inventory" in text
    assert "A10G" in text
    assert "ready" in text
    # T4 pool exists but has no live workers: shown as available, not hidden.
    assert "T4" in text
    assert "available" in text
    # A6000 has no serverless capacity but has an on-demand offer with the
    # cheapest price shown.
    assert "$0.45/hr" in text
    assert "Your machines" in text
    assert "machine-one" in text
    assert "gpu-pool" in text


def test_gpu_inventory_collapses_empty_gpu_types():
    table = gpu_inventory_table(
        live_gpus={"A10G": False, "H100": False, "T4": True},
        supported_gpus={"A10G": False, "H100": False, "T4": True},
        cheapest_offers={},
    )

    text = _render([table])
    assert "T4" in text
    assert "A10G" not in text
    assert "H100" not in text
    # Hidden GPU types are summarized in the caption, not table rows (long
    # row text would stretch the GPU column).
    assert table.caption.strip() == "2 more GPU types · no capacity"


def test_release_candidates_skips_empty_pools():
    pools = [
        PrivatePool(name="reserved", reservations=[ProviderInstance(id="r1")]),
        PrivatePool(name="joined", machine_count=1),
        PrivatePool(name="pending", reserved_nodes=2),
        PrivatePool(name="empty"),
    ]

    names = [p.name for p in _release_candidates(pools)]
    assert names == ["reserved", "joined", "pending"]


def test_release_options_show_gpu_machines_and_expiry():
    pools = [
        PrivatePool(
            name="ondemand-a6000",
            config=PoolConfig(gpu=["A6000"]),
            machine_count=1,
            expires_at=datetime.now(timezone.utc) + timedelta(hours=3),
        ),
        PrivatePool(name="cpu-pool", machine_count=2),
    ]

    options = _release_options(pools)
    assert options[0].value == "ondemand-a6000"
    assert "A6000" in options[0].label
    assert "1 machine" in options[0].label
    assert "expires in" in options[0].description
    assert "2 machines" in options[1].label


def test_release_expiry_handles_missing_expiration():
    assert _release_expiry(PrivatePool(name="p")) == "manual release"


def test_inventory_json_shape():
    from beta9.cli.machine import _inventory_json

    res = ListMachinesResponse(
        ok=True,
        gpus={"T4": True},
        supported_gpus={"T4": True, "A10G": True, "H100": False},
    )
    offers = [PoolOffer(id="o1", provider="vast", gpu="A6000", hourly_cost_micros=550_000)]

    inventory = _inventory_json(res, offers)
    by_gpu = {entry["gpu"]: entry for entry in inventory}

    assert by_gpu["T4"]["serverless"] == "ready"
    assert by_gpu["A10G"]["serverless"] == "available"
    assert by_gpu["H100"]["serverless"] == "none"
    assert by_gpu["A6000"]["serverless"] == "none"
    assert by_gpu["A6000"]["on_demand"]["id"] == "o1"
    assert by_gpu["T4"]["on_demand"] is None


def test_format_memory_pair_uses_single_unit():
    from beta9.cli.machine_format import format_memory_pair

    assert format_memory_pair(35533, 65536) == "34.7/64.0GiB"
    assert format_memory_pair(0, 81920) == "0.0/80.0GiB"
    assert format_memory_pair(256, 512) == "256/512MiB"


def test_cheapest_offers_by_gpu_picks_lowest_price():
    offers = [
        PoolOffer(id="o1", provider="vast", gpu="A6000", hourly_cost_micros=620_000),
        PoolOffer(id="o2", provider="hetzner", gpu="A6000", hourly_cost_micros=450_000),
        PoolOffer(id="o3", provider="vast", gpu="", hourly_cost_micros=100_000),  # CPU offer
    ]

    cheapest = _cheapest_offers_by_gpu(offers)
    assert set(cheapest) == {"A6000"}
    assert cheapest["A6000"].id == "o2"
