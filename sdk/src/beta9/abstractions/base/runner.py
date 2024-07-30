import inspect
import os
import time
from queue import Empty, Queue
from typing import Callable, List, Optional, Union

from watchdog.observers import Observer

from ... import terminal
from ...abstractions.base import BaseAbstraction
from ...abstractions.image import Image, ImageBuildResult
from ...abstractions.volume import Volume
from ...clients.gateway import Autoscaler as AutoscalerProto
from ...clients.gateway import (
    GatewayServiceStub,
    GetOrCreateStubRequest,
    GetOrCreateStubResponse,
    ReplaceObjectContentOperation,
    ReplaceObjectContentRequest,
    ReplaceObjectContentResponse,
    SecretVar,
)
from ...config import ConfigContext, SDKSettings, get_config_context, get_settings
from ...env import called_on_import
from ...sync import FileSyncer, SyncEventHandler
from ...type import _AUTOSCALER_TYPES, Autoscaler, GpuType, GpuTypeAlias, QueueDepthAutoscaler

CONTAINER_STUB_TYPE = "container"
FUNCTION_STUB_TYPE = "function"
TASKQUEUE_STUB_TYPE = "taskqueue"
ENDPOINT_STUB_TYPE = "endpoint"
TASKQUEUE_DEPLOYMENT_STUB_TYPE = "taskqueue/deployment"
ENDPOINT_DEPLOYMENT_STUB_TYPE = "endpoint/deployment"
FUNCTION_DEPLOYMENT_STUB_TYPE = "function/deployment"
TASKQUEUE_SERVE_STUB_TYPE = "taskqueue/serve"
ENDPOINT_SERVE_STUB_TYPE = "endpoint/serve"
FUNCTION_SERVE_STUB_TYPE = "function/serve"


class RunnerAbstraction(BaseAbstraction):
    def __init__(
        self,
        cpu: Union[int, float, str] = 1.0,
        memory: Union[int, str] = 128,
        gpu: GpuTypeAlias = GpuType.NoGPU,
        image: Image = Image(),
        workers: int = 1,
        keep_warm_seconds: float = 10.0,
        max_pending_tasks: int = 100,
        retries: int = 3,
        timeout: int = 3600,
        volumes: Optional[List[Volume]] = None,
        secrets: Optional[List[str]] = None,
        on_start: Optional[Callable] = None,
        callback_url: Optional[str] = None,
        authorized: bool = True,
        name: Optional[str] = None,
        autoscaler: Autoscaler = QueueDepthAutoscaler(),
    ) -> None:
        super().__init__()

        if image is None:
            image = Image()

        self.name = name
        self.authorized = authorized
        self.image: Image = image
        self.image_available: bool = False
        self.files_synced: bool = False
        self.stub_created: bool = False
        self.runtime_ready: bool = False
        self.object_id: str = ""
        self.image_id: str = ""
        self.stub_id: str = ""
        self.handler: str = ""
        self.on_start: str = ""
        self.callback_url = callback_url or ""
        self.cpu = cpu
        self.memory = self._parse_memory(memory) if isinstance(memory, str) else memory
        self.gpu = gpu
        self.volumes = volumes or []
        self.secrets = [SecretVar(name=s) for s in (secrets or [])]
        self.workers = workers
        self.keep_warm_seconds = keep_warm_seconds
        self.max_pending_tasks = max_pending_tasks
        self.retries = retries
        self.timeout = timeout
        self.autoscaler = autoscaler

        if on_start is not None:
            self._map_callable_to_attr(attr="on_start", func=on_start)

        self._gateway_stub: Optional[GatewayServiceStub] = None
        self.syncer: FileSyncer = FileSyncer(self.gateway_stub)
        self.settings: SDKSettings = get_settings()
        self.config_context: ConfigContext = get_config_context()

    def print_invocation_snippet(self, invocation_url: str) -> None:
        """Print curl request to call deployed container URL"""

        terminal.header("Invocation details")
        commands = [
            f"curl -X POST '{invocation_url}' \\",
            "-H 'Connection: keep-alive' \\",
            "-H 'Content-Type: application/json' \\",
            *(
                [f"-H 'Authorization: Bearer {self.config_context.token}' \\"]
                if self.authorized
                else []
            ),
            "-d '{}'",
        ]
        terminal.print("\n".join(commands), crop=False, overflow="ignore")

    def _parse_memory(self, memory_str: str) -> int:
        """Parse memory str (with units) to megabytes."""

        if memory_str.lower().endswith("mi"):
            return int(memory_str[:-2])
        elif memory_str.lower().endswith("gb"):
            return int(memory_str[:-2]) * 1000
        elif memory_str.lower().endswith("gi"):
            return int(memory_str[:-2]) * 1024
        else:
            raise ValueError("Unsupported memory format")

    @property
    def gateway_stub(self) -> GatewayServiceStub:
        if not self._gateway_stub:
            self._gateway_stub = GatewayServiceStub(self.channel)
        return self._gateway_stub

    @gateway_stub.setter
    def gateway_stub(self, value) -> None:
        self._gateway_stub = value

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

    def _map_callable_to_attr(self, *, attr: str, func: Callable):
        """
        Determine the module and function name of a callable function, and cache on the class.
        This is used for passing callables to stub config.
        """
        if getattr(self, attr):
            return

        module = inspect.getmodule(func)  # Determine module / function name
        if module:
            module_file = os.path.relpath(module.__file__, start=os.getcwd()).replace("/", ".")
            module_name = os.path.splitext(module_file)[0]
        else:
            module_name = "__main__"

        function_name = func.__name__
        setattr(self, attr, f"{module_name}:{function_name}")

    def _sync_content(
        self,
        *,
        dir: str,
        object_id: str,
        file_update_queue: Queue,
        on_event: Optional[Callable] = None,
    ) -> None:
        try:
            operation, path, new_path = file_update_queue.get_nowait()

            if on_event:
                on_event(operation, path, new_path)

            req = ReplaceObjectContentRequest(
                object_id=object_id,
                path=os.path.relpath(path, start=dir),
                is_dir=os.path.isdir(path),
                op=operation,
            )

            if operation == ReplaceObjectContentOperation.WRITE:
                if not req.is_dir:
                    with open(path, "rb") as f:
                        req.data = f.read()

            elif operation == ReplaceObjectContentOperation.DELETE:
                pass

            elif operation == ReplaceObjectContentOperation.MOVED:
                req.new_path = os.path.relpath(new_path, start=dir)

            res = self.gateway_stub.replace_object_content(req)
            if not res.ok:
                terminal.warn("Failed to sync content")

            file_update_queue.task_done()
        except Empty:
            time.sleep(0.1)
        except Exception as e:
            terminal.warn(str(e))

    def sync_dir_to_workspace(
        self, *, dir: str, object_id: str, on_event: Optional[Callable] = None
    ) -> ReplaceObjectContentResponse:
        file_update_queue = Queue()
        event_handler = SyncEventHandler(file_update_queue)

        observer = Observer()
        observer.schedule(event_handler, dir, recursive=True)
        observer.start()

        terminal.header(f"Watching '{dir}' for changes...")
        while True:
            self._sync_content(
                dir=dir, object_id=object_id, file_update_queue=file_update_queue, on_event=on_event
            )

    def prepare_runtime(
        self,
        *,
        func: Optional[Callable] = None,
        stub_type: str,
        force_create_stub: bool = False,
    ) -> bool:
        if called_on_import():
            return False

        if func is not None:
            self._map_callable_to_attr(attr="handler", func=func)

        stub_name = f"{stub_type}/{self.handler}" if self.handler else stub_type

        if self.runtime_ready:
            return True

        self.cpu = self._parse_cpu_to_millicores(self.cpu)

        if not self.image_available:
            image_build_result: ImageBuildResult = self.image.build()

            if image_build_result and image_build_result.success:
                self.image_available = True
                self.image_id = image_build_result.image_id
            else:
                terminal.error("Image build failed", exit=False)
                return False

        if not self.files_synced:
            sync_result = self.syncer.sync()

            if sync_result.success:
                self.files_synced = True
                self.object_id = sync_result.object_id
            else:
                terminal.error("File sync failed", exit=False)
                return False

        for v in self.volumes:
            if not v.ready and not v.get_or_create():
                terminal.error(f"Volume is not ready: {v.name}", exit=False)
                return False

        try:
            self.gpu = GpuType(self.gpu).value
        except ValueError:
            terminal.error(f"Invalid GPU type: {self.gpu}", exit=False)
            return False

        autoscaler_type = _AUTOSCALER_TYPES.get(type(self.autoscaler), "")
        if not autoscaler_type:
            terminal.error(
                f"Invalid Autoscaler class: {type(self.autoscaler).__name__}",
                exit=False,
            )
            return False

        if not self.stub_created:
            stub_response: GetOrCreateStubResponse = self.gateway_stub.get_or_create_stub(
                GetOrCreateStubRequest(
                    object_id=self.object_id,
                    image_id=self.image_id,
                    stub_type=stub_type,
                    name=stub_name,
                    python_version=self.image.python_version,
                    cpu=self.cpu,
                    memory=self.memory,
                    gpu=self.gpu,
                    handler=self.handler,
                    on_start=self.on_start,
                    callback_url=self.callback_url,
                    retries=self.retries,
                    timeout=self.timeout,
                    keep_warm_seconds=self.keep_warm_seconds,
                    workers=self.workers,
                    max_pending_tasks=self.max_pending_tasks,
                    volumes=[v.export() for v in self.volumes],
                    secrets=self.secrets,
                    force_create=force_create_stub,
                    authorized=self.authorized,
                    autoscaler=AutoscalerProto(
                        type=autoscaler_type,
                        max_containers=self.autoscaler.max_containers,
                        tasks_per_container=self.autoscaler.tasks_per_container,
                    ),
                )
            )

            if stub_response.ok:
                self.stub_created = True
                self.stub_id = stub_response.stub_id
            else:
                if err := stub_response.err_msg:
                    terminal.error(err, exit=False)
                else:
                    terminal.error("Failed to get or create stub", exit=False)
                return False

        self.runtime_ready = True
        return True
