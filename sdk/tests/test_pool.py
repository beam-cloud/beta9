import pytest

from beta9 import Pool
from beta9.abstractions.function import Function
from beta9.cli.pool import (
    _agent_join_interrupted,
    _append_join_args,
    _get_pool_renderable,
    _join_transport,
    management,
)
from beta9.clients.gateway import (
    ListPoolsResponse,
    ListPrivatePoolsResponse,
    PoolConfig,
    PrivatePool,
)
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


def test_pool_string_routes_to_manual_pool():
    fn = Function(pool="manual-training")

    assert fn.pool_config.name == "manual-training"
    assert fn.pool_config.selector == "manual-training"


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
        "curl -fsSL http://localhost/install/agent | bash -s -- --dev",
        agent_bin="/tmp/beam agent",
        executor="worker-container",
        worker_image="registry.localhost:5000/beta9-worker:latest",
        max_cpu="8",
        max_memory="32Gi",
        max_gpus=2,
        gpu_ids="0,1",
        network_slots=64,
        container_start_concurrency=12,
        background=False,
        service_manager="systemd",
        service_name="beam-private",
        state_dir="/var/lib/beam/private",
    )

    assert "--foreground" in command
    assert "--service-manager systemd" in command
    assert "--service-name beam-private" in command
    assert "--state-dir /var/lib/beam/private" in command
    assert "--agent-bin '/tmp/beam agent'" in command
    assert "--executor worker-container" in command
    assert "--worker-image registry.localhost:5000/beta9-worker:latest" in command
    assert "--max-cpu 8" in command
    assert "--max-memory 32Gi" in command
    assert "--max-gpus 2" in command
    assert "--gpu-ids 0,1" in command
    assert "--network-slots 64" in command
    assert "--container-start-concurrency 12" in command


def test_pool_join_appends_agent_flags_to_branched_command():
    command = _append_join_args(
        "if [ \"$(uname -s)\" = \"Darwin\" ] || [ \"$(id -u)\" -eq 0 ]; then curl -fsSL 'https://app.stage.beam.cloud/install/agent' | sh -s -- --gateway 'https://app.stage.beam.cloud' --join-token 'token'; else curl -fsSL 'https://app.stage.beam.cloud/install/agent' | sudo sh -s -- --gateway 'https://app.stage.beam.cloud' --join-token 'token'; fi",
        max_cpu="4",
        max_memory="8Gi",
        network_slots=16,
    )

    assert "fi --max-cpu" not in command
    assert command.count("--max-cpu 4") == 2
    assert command.count("--max-memory 8Gi") == 2
    assert command.count("--network-slots 16") == 2


def test_pool_list_ignores_denied_control_plane_scope_for_default_view():
    class Gateway:
        def list_private_pools(self, _req):
            return ListPrivatePoolsResponse(
                ok=True,
                pools=[
                    PrivatePool(
                        name="private-dev",
                        config=PoolConfig(name="private-dev", fallback="internal"),
                        status="active",
                    )
                ],
            )

        def list_pools(self, _req):
            return ListPoolsResponse(ok=False, err_msg="This action is not permitted")

    service = type("Service", (), {"gateway": Gateway()})()

    renderable = _get_pool_renderable(service, 20, "json", {}, "all")

    assert "private-dev" in renderable.plain
    assert "[red]" not in renderable.plain


def test_pool_launch_command_is_not_exposed():
    assert "launch" not in management.commands
    assert "join" in management.commands
    assert "create" in management.commands


def test_pool_join_treats_sigint_exit_as_clean_stop():
    assert _agent_join_interrupted(130)
    assert _agent_join_interrupted(-2)
    assert not _agent_join_interrupted(1)
