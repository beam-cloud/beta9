import inspect
import os
from typing import Callable, List, Optional

from beam.abstractions.base import BaseAbstraction
from beam.abstractions.image import Image, ImageBuildResult
from beam.abstractions.volume import Volume
from beam.clients.gateway import GatewayServiceStub, GetOrCreateStubResponse
from beam.sync import FileSyncer

FUNCTION_STUB_TYPE = "function"
FUNCTION_STUB_PREFIX = "function"

TASKQUEUE_STUB_TYPE = "taskqueue"
TASKQUEUE_STUB_PREFIX = "taskqueue"


class RunnerAbstraction(BaseAbstraction):
    def __init__(
        self,
        image: Image,
        cpu: int = 100,
        memory: int = 128,
        gpu="",
        concurrency: int = 1,
        max_containers: int = 1,
        keep_warm_seconds: float = 10.0,
        max_pending_tasks: int = 100,
        retries: int = 3,
        timeout: int = 3600,
        volumes: Optional[List[Volume]] = None,
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
        self.volumes = volumes or []

        self.concurrency = concurrency
        self.keep_warm_seconds = keep_warm_seconds
        self.max_pending_tasks = max_pending_tasks
        self.max_containers = max_containers
        self.retries = retries
        self.timeout = timeout

        self.gateway_stub: GatewayServiceStub = GatewayServiceStub(self.channel)
        self.syncer: FileSyncer = FileSyncer(self.gateway_stub)

    def _load_handler(self, func: Callable) -> None:
        if self.handler:
            return

        module = inspect.getmodule(func)  # Determine module / function name
        if module:
            module_file = os.path.basename(module.__file__)
            module_name = os.path.splitext(module_file)[0]
        else:
            module_name = "__main__"

        function_name = func.__name__
        self.handler = f"{module_name}:{function_name}"

    def prepare_runtime(self, *, func: Callable, stub_type: str) -> bool:
        self._load_handler(func)

        stub_name = ""
        if stub_type == FUNCTION_STUB_TYPE:
            stub_name = f"{FUNCTION_STUB_PREFIX}/{self.handler}"
        elif stub_type == TASKQUEUE_STUB_TYPE:
            stub_name = f"{TASKQUEUE_STUB_PREFIX}/{self.handler}"

        if self.runtime_ready:
            return True

        if not self.image_available:
            image_build_result: ImageBuildResult = self.image.build()

            if image_build_result and image_build_result.success:
                self.image_available = True
                self.image_id = image_build_result.image_id
            else:
                return False

        if not self.files_synced:
            sync_result = self.syncer.sync()

            if sync_result.success:
                self.files_synced = True
                self.object_id = sync_result.object_id
            else:
                return False

        for v in self.volumes:
            if not v.ready and not v.get_or_create():
                return False

        if not self.stub_created:
            stub_response: GetOrCreateStubResponse = self.run_sync(
                self.gateway_stub.get_or_create_stub(
                    object_id=self.object_id,
                    image_id=self.image_id,
                    stub_type=stub_type,
                    name=stub_name,
                    python_version=self.image.python_version,
                    cpu=self.cpu,
                    memory=self.memory,
                    gpu=self.gpu,
                    handler=self.handler,
                    retries=self.retries,
                    timeout=self.timeout,
                    keep_warm_seconds=self.keep_warm_seconds,
                    concurrency=self.concurrency,
                    max_containers=self.max_containers,
                    max_pending_tasks=self.max_pending_tasks,
                    volumes=[v.export() for v in self.volumes],
                )
            )

            if stub_response.ok:
                self.stub_created = True
                self.stub_id = stub_response.stub_id
            else:
                return False

        self.runtime_ready = True
        return True
