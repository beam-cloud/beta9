import inspect
import os
from typing import Any, Callable

from beam.abstractions.base import BaseAbstraction
from beam.abstractions.image import Image, ImageBuildResult
from beam.clients.gateway import GatewayServiceStub, GetOrCreateStubResponse
from beam.clients.taskqueue import TaskQueuePopResponse, TaskQueuePutResponse, TaskQueueServiceStub
from beam.sync import FileSyncer

TASKQUEUE_STUB_TYPE = "TASK_QUEUE"
TASKQUEUE_STUB_PREFIX = "taskqueue"


class TaskQueue(BaseAbstraction):
    def __init__(
        self,
        image: Image,
        cpu: int = 100,
        memory: int = 128,
        gpu="",
        concurrency: int = 1,
        max_containers: int = 1,
    ) -> None:
        super().__init__()

        if image is None:
            image = Image()

        self.image: Image = image
        self.image_available: bool = False
        self.files_synced: bool = False
        self.stub_created: bool = False
        self.runtime_ready: bool = False
        self.object_id: str = ""
        self.image_id: str = ""
        self.stub_id: str = ""
        self.handler: str = ""
        self.cpu = cpu
        self.memory = memory
        self.gpu = gpu
        self.concurrency = concurrency
        self.max_containers = max_containers

        self.gateway_stub: GatewayServiceStub = GatewayServiceStub(self.channel)
        self.taskqueue_stub: TaskQueueServiceStub = TaskQueueServiceStub(self.channel)
        self.syncer: FileSyncer = FileSyncer(self.gateway_stub)

    def __call__(self, func):
        return _CallableWrapper(func, self)


class _CallableWrapper:
    def __init__(self, func: Callable, parent: TaskQueue):
        self.func: Callable = func
        self.parent: TaskQueue = parent

    def _prepare_runtime(self) -> bool:
        module = inspect.getmodule(self.func)  # Determine module / function name
        if module:
            module_file = os.path.basename(module.__file__)
            module_name = os.path.splitext(module_file)[0]
        else:
            module_name = "__main__"

        function_name = self.func.__name__
        self.parent.handler = f"{module_name}:{function_name}"

        if not self.parent.image_available:
            image_build_result: ImageBuildResult = self.parent.image.build()

            if image_build_result and image_build_result.success:
                self.parent.image_available = True
                self.parent.image_id = image_build_result.image_id
            else:
                return False

        if not self.parent.files_synced:
            sync_result = self.parent.syncer.sync()

            if sync_result.success:
                self.parent.files_synced = True
                self.parent.object_id = sync_result.object_id
            else:
                return False

        if not self.parent.stub_created:
            stub_response: GetOrCreateStubResponse = self.parent.run_sync(
                self.parent.gateway_stub.get_or_create_stub(
                    object_id=self.parent.object_id,
                    image_id=self.parent.image_id,
                    stub_type=TASKQUEUE_STUB_TYPE,
                    name=f"{TASKQUEUE_STUB_PREFIX}/{self.parent.handler}",
                    python_version=self.parent.image.python_version,
                    cpu=self.parent.cpu,
                    memory=self.parent.memory,
                    gpu=self.parent.gpu,
                )
            )

            if stub_response.ok:
                self.parent.stub_created = True
                self.parent.stub_id = stub_response.stub_id
            else:
                return False

        self.parent.runtime_ready = True
        return True

    def __call__(self, *args, **kwargs) -> Any:
        container_id = os.getenv("CONTAINER_ID")
        if container_id is not None:
            return self.local(*args, **kwargs)

        if not self.parent.runtime_ready and not self._prepare_runtime():
            return

    def local(self, *args, **kwargs) -> Any:
        return self.func(*args, **kwargs)

    def put(self, payload: Any):
        if not self.parent.runtime_ready and not self._prepare_runtime():
            return

        r: TaskQueuePutResponse = self.parent.run_sync(self.parent.taskqueue_stub.task_queue_put())
        print(r)

    def pop(self):
        if not self.parent.runtime_ready and not self._prepare_runtime():
            return

        r: TaskQueuePopResponse = self.parent.run_sync(self.parent.taskqueue_stub.task_queue_pop())
        print(r)
