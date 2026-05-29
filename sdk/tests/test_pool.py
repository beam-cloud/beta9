import pytest

from beta9 import Pool
from beta9.abstractions.function import Function
from beta9.cli.pool import _append_join_args, _join_transport
from beta9.config import ConfigContext


def test_reserved_pool_serialization_and_gpu_inheritance():
    fn = Function(
        pool=Pool(
            name="training-h100",
            gpu=["H100", "H200"],
            gpus=10,
            ttl="6h",
            max_spend=80,
            providers=["vast"],
            regions=["us-east"],
            min_reliability=0.9,
        ),
        gpu_count=1,
    )

    assert fn.gpu == ["H100", "H200"]
    assert fn.pool_config.name == "training-h100"
    assert fn.pool_config.gpu == ["H100", "H200"]
    assert fn.pool_config.gpus == 10
    assert fn.pool_config.ttl == "6h"
    assert fn.pool_config.max_spend == 80
    assert fn.pool_config.providers == ["vast"]
    assert fn.pool_config.regions == ["us-east"]
    assert fn.pool_config.min_reliability == 0.9
    assert fn.pool_config.reservation_required is True


def test_pool_string_routes_to_manual_pool():
    fn = Function(pool="manual-training")

    assert fn.pool_config.name == "manual-training"
    assert fn.pool_config.selector == "manual-training"
    assert fn.pool_config.reservation_required is False


def test_pool_requires_budget_and_ttl_for_reservation():
    with pytest.raises(ValueError):
        Function(pool=Pool(gpu="H100", gpus=10, max_spend=80))

    with pytest.raises(ValueError):
        Function(pool=Pool(gpu="H100", gpus=10, ttl="6h"))


def test_function_gpu_must_be_compatible_with_pool_gpu():
    with pytest.raises(ValueError):
        Function(gpu="L4", pool=Pool(gpu="H100", gpus=1, ttl="1h", max_spend=10))


def test_pool_join_transport_auto_uses_tsnet_for_local_gateway():
    service = type("Service", (), {"_config": ConfigContext(gateway_host="localhost")})()

    assert _join_transport(service, "auto") == "tsnet_restricted"
    assert _join_transport(service, "tsnet-restricted") == "tsnet-restricted"


def test_pool_join_appends_agent_flags():
    command = _append_join_args(
        "curl -fsSL http://localhost/install/hybrid-worker | bash -s -- --dev",
        listen="0.0.0.0:0",
        advertise_host="host.docker.internal",
        agent_bin="/tmp/beam agent",
    )

    assert "--listen 0.0.0.0:0" in command
    assert "--advertise-host host.docker.internal" in command
    assert "--agent-bin '/tmp/beam agent'" in command
