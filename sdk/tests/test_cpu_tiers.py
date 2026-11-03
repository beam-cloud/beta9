from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from beta9.abstractions.sandbox import Sandbox
from beta9.abstractions.function import Function
from beta9.abstractions.endpoint import Endpoint
from beta9.abstractions.taskqueue import TaskQueue
from beta9.abstractions.pod import Pod
from beta9.abstractions.service import Service
from beta9.abstractions.vm import VM


@pytest.mark.parametrize('cls', [Sandbox, Function, Endpoint, TaskQueue, Pod, Service])
def test_tier_is_exposed_by_cpu_workload_constructors(cls):
    workload = cls(cpu_tier='shared')
    assert workload.cpu_tier == 'shared'


def test_vm_sandbox_omitted_memory_uses_the_authenticated_default():
    sandbox = Sandbox(use_vm=True)
    sandbox.cpu_tiers = MagicMock(return_value={'default_cpu_tier': 'shared'})
    sandbox._resolve_native_vm_defaults()
    assert sandbox.memory == 1024
    explicit = Sandbox(use_vm=True, memory=128, cpu_tier='shared')
    explicit._resolve_native_vm_defaults()
    assert explicit.memory == 128
    legacy = Sandbox(use_vm=True)
    legacy.cpu_tiers = MagicMock(return_value={'default_cpu_tier': ''})
    legacy._resolve_native_vm_defaults()
    assert legacy.memory == 128


def test_vm_tier_creation_and_stopped_update_use_normal_api():
    service = SimpleNamespace(channel=SimpleNamespace(config=SimpleNamespace(token='test')), http=MagicMock())
    vm = VM('dev', cpu_tier='spot', _service=service)
    assert vm._spec['cpu_tier'] == 'spot'
    vm._set({'id': 'vm-id', 'name': 'dev', 'spec': {}})
    vm._api = MagicMock(return_value={'id': 'vm-id', 'name': 'dev', 'spec': {'cpu_tier': 'performance'}})
    vm.update(cpu_tier='performance')
    assert vm._api.call_args.kwargs['json'] == {'cpu_tier': 'performance'}
