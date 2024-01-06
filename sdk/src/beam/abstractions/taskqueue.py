import os
from typing import Any, Callable

from beam.abstractions.image import Image
from beam.abstractions.runner import RunnerAbstraction
from beam.clients.taskqueue import TaskQueuePutResponse, TaskQueueServiceStub

TASKQUEUE_STUB_TYPE = "TASK_QUEUE"
TASKQUEUE_STUB_PREFIX = "taskqueue"


class TaskQueue(RunnerAbstraction):
    def __init__(
        self,
        image: Image,
        cpu: int = 100,
        memory: int = 128,
        gpu="",
        concurrency: int = 1,
        max_containers: int = 1,
    ) -> None:
        super().__init__(image=image, cpu=cpu, memory=memory, gpu=gpu)

        self.concurrency = concurrency
        self.max_containers = max_containers

        self.taskqueue_stub: TaskQueueServiceStub = TaskQueueServiceStub(self.channel)

    def __call__(self, func):
        return _CallableWrapper(func, self)


class _CallableWrapper:
    def __init__(self, func: Callable, parent: TaskQueue):
        self.func: Callable = func
        self.parent: TaskQueue = parent

    def __call__(self, *args, **kwargs) -> Any:
        container_id = os.getenv("CONTAINER_ID")
        if container_id is not None:
            return self.local(*args, **kwargs)

        if not self.parent.prepare_runtime(
            func=self.func,
            stub_type=TASKQUEUE_STUB_TYPE,
            stub_name=f"{TASKQUEUE_STUB_PREFIX}/{self.parent.handler}",
        ):
            return

    def local(self, *args, **kwargs) -> Any:
        return self.func(*args, **kwargs)

    def put(self, payload: Any):
        if not self.parent.prepare_runtime(
            func=self.func,
            stub_type=TASKQUEUE_STUB_TYPE,
            stub_name=f"{TASKQUEUE_STUB_PREFIX}/{self.parent.handler}",
        ):
            return

        r: TaskQueuePutResponse = self.parent.run_sync(
            self.parent.taskqueue_stub.task_queue_put(stub_id=self.parent.stub_id, payload=payload)
        )
        print(r)
