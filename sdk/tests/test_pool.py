import pytest

from beta9 import Pool
from beta9.abstractions.function import Function
from beta9.abstractions.sandbox import Sandbox
from beta9.cli.pool import (
    _agent_join_interrupted,
    _append_join_args,
    _get_pool_renderable,
    _pool_capacity_summary,
    _pool_config,
    _private_pool_compute,
    _join_transport,
    management,
)
from beta9.clients.gateway import (
    ListPoolsResponse,
    ListPrivatePoolsResponse,
    PoolConfig,
    ProviderInstance,
    PrivatePool,
)
from beta9.config import ConfigContext


def test_reserved_pool_serialization_and_gpu_inheritance():
    fn = Function(
        pool=Pool(
            name="training-h100",
            gpu=["H100", "H200"],
            nodes=10,
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
    assert fn.pool_config.nodes == 10
    assert fn.pool_config.ttl == "6h"
    assert fn.pool_config.max_spend == 80
    assert fn.pool_config.providers == ["vast"]
    assert fn.pool_config.regions == ["us-east"]
    assert fn.pool_config.min_reliability == 0.9


def test_pool_string_routes_to_manual_pool():
    fn = Function(pool="manual-training")

    assert fn.pool_config.name == "manual-training"
    assert fn.pool_config.selector == "manual-training"


def test_function_routes_multi_gpu_to_private_pool():
    fn = Function(gpu="H100", gpu_count=8, pool="gpu-pool")

    assert fn.gpu == "H100"
    assert fn.gpu_count == 8
    assert fn.pool_config.name == "gpu-pool"


def test_sandbox_pool_string_routes_to_manual_pool():
    sandbox = Sandbox(pool="sandbox-cpu")

    assert sandbox.pool_config.name == "sandbox-cpu"
    assert sandbox.pool_config.selector == "sandbox-cpu"


def test_sandbox_pool_config_serializes_managed_capacity_request():
    sandbox = Sandbox(
        pool=Pool(
            name="sandbox-managed",
            nodes=1,
            ttl="1h",
            max_spend=1,
            providers=["hetzner"],
            regions=["ash"],
        )
    )

    assert sandbox.pool_config.name == "sandbox-managed"
    assert sandbox.pool_config.selector == "sandbox-managed"
    assert sandbox.pool_config.nodes == 1
    assert sandbox.pool_config.ttl == "1h"
    assert sandbox.pool_config.max_spend == 1
    assert sandbox.pool_config.providers == ["hetzner"]
    assert sandbox.pool_config.regions == ["ash"]


def test_pool_requires_budget_and_ttl_for_reservation():
    with pytest.raises(ValueError):
        Function(pool=Pool(gpu="H100", nodes=10, max_spend=80))

    with pytest.raises(ValueError):
        Function(pool=Pool(gpu="H100", nodes=10, ttl="6h"))


def test_function_gpu_must_be_compatible_with_pool_gpu():
    with pytest.raises(ValueError):
        Function(gpu="L4", pool=Pool(gpu="H100", nodes=1, ttl="1h", max_spend=10))


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


def test_pool_managed_capacity_commands_are_exposed():
    assert "scale" in management.commands
    assert "offers" in management.commands
    assert "join" in management.commands
    assert "create" in management.commands


def test_pool_create_config_accepts_gpu_type():
    pool = _pool_config("gpu-pool", gpu=("H100",))

    assert pool.name == "gpu-pool"
    assert pool.gpu == ["H100"]
    assert pool.mode == "private"


def test_pool_capacity_summary_counts_reservations():
    pool = PrivatePool(
        machine_count=2,
        ready_machine_count=1,
        reservations=[ProviderInstance(node_count=2)],
    )

    assert _pool_capacity_summary(pool) == "1/2 ready, 2 reserved"


def test_private_pool_compute_summarizes_nodes_with_gpu_attributes():
    pool = PrivatePool(
        config=PoolConfig(gpu=["H100"], nodes=2),
        reserved_nodes=2,
    )

    assert _private_pool_compute(pool) == "2 nodes (H100)"


def test_private_pool_compute_summarizes_cpu_nodes():
    pool = PrivatePool(config=PoolConfig(nodes=3), reserved_nodes=3)

    assert _private_pool_compute(pool) == "3 nodes (CPU)"


def test_pool_join_treats_sigint_exit_as_clean_stop():
    assert _agent_join_interrupted(130)
    assert _agent_join_interrupted(-2)
    assert not _agent_join_interrupted(1)
