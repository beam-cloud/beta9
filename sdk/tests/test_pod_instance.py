from types import SimpleNamespace

from beta9.abstractions.pod import PodInstance
from beta9.type import TaskStatus


class FakeGateway:
    def __init__(self, statuses: list[str]):
        self.statuses = statuses
        self.requests = []

    def list_tasks(self, request):
        self.requests.append(request)
        status = self.statuses.pop(0)
        return SimpleNamespace(
            ok=True,
            err_msg="",
            tasks=[SimpleNamespace(status=status)],
        )


def pod_instance(gateway: FakeGateway) -> PodInstance:
    instance = object.__new__(PodInstance)
    instance.task_id = "task-123"
    instance.gateway_stub = gateway
    return instance


def test_pod_instance_status_uses_its_task_id():
    gateway = FakeGateway(["RUNNING"])
    instance = pod_instance(gateway)

    assert instance.status() is TaskStatus.Running
    assert gateway.requests[0].filters["id"].values == ["task-123"]


def test_pod_instance_wait_returns_terminal_status():
    gateway = FakeGateway(["RUNNING", "COMPLETE"])
    instance = pod_instance(gateway)

    assert instance.wait(timeout=1, poll_interval=0.001) is TaskStatus.Complete
    assert len(gateway.requests) == 2
