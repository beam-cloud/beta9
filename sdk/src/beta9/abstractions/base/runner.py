import inspect
import os
from typing import Callable, List, Optional, Union

from ...abstractions.base import BaseAbstraction
from ...abstractions.image import Image, ImageBuildResult
from ...abstractions.volume import Volume
from ...clients.gateway import GatewayServiceStub, GetOrCreateStubResponse
from ...sync import FileSyncer

CONTAINER_STUB_TYPE = "container"
FUNCTION_STUB_TYPE = "function"
TASKQUEUE_STUB_TYPE = "taskqueue"
WEBSERVER_STUB_TYPE = "endpoint"
TASKQUEUE_DEPLOYMENT_STUB_TYPE = "taskqueue/deployment"
ENDPOINT_DEPLOYMENT_STUB_TYPE = "endpoint/deployment"
FUNCTION_DEPLOYMENT_STUB_TYPE = "function/deployment"


class RunnerAbstraction(BaseAbstraction):
    def __init__(
        self,
        cpu: Union[int, float, str] = 1.0,
        memory: int = 128,
        gpu: str = "",
        image: Image = Image(),
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

    def _parse_cpu_to_millicores(self, cpu: Union[float, str]) -> int:
        """
        Parse the cpu argument to an integer value in millicores.

        Args:
        cpu (Union[int, float, str]): The CPU requirement specified as a float (cores) or string (millicores).

        Returns:
        int: The CPU requirement in millicores.

        Raises:
        ValueError: If the input is invalid or out of the specified range.
        """
        min_cores = 0.1
        max_cores = 64.0

        if isinstance(cpu, float) or isinstance(cpu, int):
            if min_cores <= cpu <= max_cores:
                return int(cpu * 1000)  # convert cores to millicores
            else:
                raise ValueError("CPU value out of range. Must be between 0.1 and 64 cores.")

        elif isinstance(cpu, str):
            if cpu.endswith("m") and cpu[:-1].isdigit():
                millicores = int(cpu[:-1])
                if min_cores * 1000 <= millicores <= max_cores * 1000:
                    return millicores
                else:
                    raise ValueError("CPU value out of range. Must be between 100m and 64000m.")
            else:
                raise ValueError(
                    "Invalid CPU string format. Must be a digit followed by 'm' (e.g., '1000m')."
                )

        else:
            raise TypeError("CPU must be a float or a string.")

    def _load_handler(self, func: Callable) -> None:
        if self.handler or func is None:
            return

        module = inspect.getmodule(func)  # Determine module / function name
        if module:
            module_file = os.path.basename(module.__file__)
            module_name = os.path.splitext(module_file)[0]
        else:
            module_name = "__main__"

        function_name = func.__name__
        self.handler = f"{module_name}:{function_name}"

    def prepare_runtime(
        self,
        *,
        func: Optional[Callable] = None,
        stub_type: str,
        force_create_stub: bool = False,
        name: Optional[str] = None,
    ) -> bool:
        if func is not None:
            self._load_handler(func)

        stub_name = f"{stub_type}/{self.handler}" if self.handler else stub_type

        if name:
            stub_name = f"{stub_name}/{name}"

        if self.runtime_ready:
            return True

        self.cpu = self._parse_cpu_to_millicores(self.cpu)

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
                    force_create=force_create_stub,
                )
            )

            if stub_response.ok:
                self.stub_created = True
                self.stub_id = stub_response.stub_id
            else:
                return False

        self.runtime_ready = True
        return True
