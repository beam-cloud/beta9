import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import grpc
import pytest

from beta9.abstractions.endpoint import Endpoint
from beta9.abstractions.function import Function
from beta9.abstractions.pod import Pod
from beta9.abstractions.sandbox import Sandbox
from beta9.abstractions.service import Service
from beta9.abstractions.taskqueue import TaskQueue
from beta9.abstractions.vm import VM


@pytest.mark.parametrize("cls", [Sandbox, Function, Endpoint, TaskQueue, Pod, Service])
def test_tier_is_exposed_by_cpu_workload_constructors(cls):
    workload = cls(cpu_tier="shared")
    assert workload.cpu_tier == "shared"
    with pytest.raises(ValueError, match="cpu_tier"):
        cls(cpu_tier="sharedd")


@pytest.mark.parametrize(
    "kwargs, default_tier, memory",
    [
        ({}, "performance", 1024),
        ({}, "", 128),
        ({"cpu_tier": "spot"}, "", 1024),
        ({"memory": 128, "cpu_tier": "shared"}, "", 128),
        # A pool never sets the tier, and one that sells none takes no default.
        ({"pool": "shared-pool"}, "performance", 1024),
        ({"pool": "shared-pool"}, "", 128),
        ({"pool": "private-pool"}, "performance", 128),
    ],
)
def test_vm_sandbox_omitted_memory_follows_the_cpu_tier(kwargs, default_tier, memory):
    sandbox = Sandbox(use_vm=True, **kwargs)
    sandbox.gateway_stub = MagicMock()
    sandbox.gateway_stub.get_cpu_tier_catalog.return_value.catalog = json.dumps(
        {"default_cpu_tier": default_tier, "pools": {"shared-pool": {"cpu_class": "shared"}}}
    )
    sandbox.prepare_runtime = MagicMock(return_value=False)
    sandbox.create()
    assert sandbox.memory == memory


def test_vm_sandbox_on_a_pool_does_not_need_the_tier_catalog():
    sandbox = Sandbox(use_vm=True, pool="private-pool")
    sandbox.gateway_stub = MagicMock()
    sandbox.gateway_stub.get_cpu_tier_catalog.side_effect = grpc.RpcError()
    sandbox.prepare_runtime = MagicMock(return_value=False)
    sandbox.create()
    assert sandbox.memory == 128
    # Without a pool the account default decides, so the catalog is required.
    unpooled = Sandbox(use_vm=True)
    unpooled.gateway_stub = sandbox.gateway_stub
    unpooled.prepare_runtime = MagicMock(return_value=False)
    with pytest.raises(grpc.RpcError):
        unpooled.create()


def test_vm_tier_creation_and_stopped_update_use_normal_api():
    service = SimpleNamespace(
        channel=SimpleNamespace(config=SimpleNamespace(token="test")), http=MagicMock()
    )
    vm = VM("dev", cpu_tier="spot", _service=service)
    assert vm._spec["cpu_tier"] == "spot"
    vm._set({"id": "vm-id", "name": "dev", "spec": {}})
    vm._api = MagicMock(
        return_value={"id": "vm-id", "name": "dev", "spec": {"cpu_tier": "performance"}}
    )
    vm.update(cpu_tier="performance")
    assert vm._api.call_args.kwargs["json"] == {"cpu_tier": "performance"}
