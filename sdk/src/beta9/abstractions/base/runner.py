import inspect
import json
import os
import threading
from typing import Callable, Dict, List, Optional, Union

import cloudpickle

from ... import terminal
from ...abstractions.base import BaseAbstraction
from ...abstractions.image import Image, ImageBuildResult
from ...abstractions.volume import Volume
from ...clients.gateway import Autoscaler as AutoscalerProto
from ...clients.gateway import (
    GatewayServiceStub,
    GetOrCreateStubRequest,
    GetOrCreateStubResponse,
    GetUrlRequest,
    GetUrlResponse,
    SecretVar,
)
from ...clients.gateway import (
    Schema as SchemaProto,
)
from ...clients.gateway import (
    SchemaField as SchemaFieldProto,
)
from ...clients.gateway import TaskPolicy as TaskPolicyProto
from ...clients.shell import ShellServiceStub
from ...clients.types import PricingPolicy as PricingPolicyProto
from ...config import ConfigContext, SDKSettings, get_config_context, get_settings
from ...env import called_on_import, is_notebook_env
from ...schema import Schema
from ...sync import FileSyncer
from ...type import (
    _AUTOSCALER_TYPES,
    Autoscaler,
    GpuType,
    GpuTypeAlias,
    PricingPolicy,
    QueueDepthAutoscaler,
    TaskPolicy,
)
from ...utils import TempFile

CONTAINER_STUB_TYPE = "container"
FUNCTION_STUB_TYPE = "function"
TASKQUEUE_STUB_TYPE = "taskqueue"
ENDPOINT_STUB_TYPE = "endpoint"
ASGI_STUB_TYPE = "asgi"
SCHEDULE_STUB_TYPE = "schedule"
BOT_STUB_TYPE = "bot"
SHELL_STUB_TYPE = "shell"

TASKQUEUE_DEPLOYMENT_STUB_TYPE = "taskqueue/deployment"
ENDPOINT_DEPLOYMENT_STUB_TYPE = "endpoint/deployment"
ASGI_DEPLOYMENT_STUB_TYPE = "asgi/deployment"
FUNCTION_DEPLOYMENT_STUB_TYPE = "function/deployment"
SCHEDULE_DEPLOYMENT_STUB_TYPE = "schedule/deployment"
BOT_DEPLOYMENT_STUB_TYPE = "bot/deployment"

TASKQUEUE_SERVE_STUB_TYPE = "taskqueue/serve"
ENDPOINT_SERVE_STUB_TYPE = "endpoint/serve"
ASGI_SERVE_STUB_TYPE = "asgi/serve"
FUNCTION_SERVE_STUB_TYPE = "function/serve"
BOT_SERVE_STUB_TYPE = "bot/serve"

POD_DEPLOYMENT_STUB_TYPE = "pod/deployment"
POD_RUN_STUB_TYPE = "pod/run"
SANDBOX_STUB_TYPE = "sandbox"

_stub_creation_lock = threading.Lock()
_stub_created_for_workspace = False


def _is_stub_created_for_workspace() -> bool:
    global _stub_created_for_workspace
    _stub_created_for_workspace = False
    return _stub_created_for_workspace


def _set_stub_created_for_workspace(value: bool) -> None:
    global _stub_created_for_workspace
    _stub_created_for_workspace = value


class RunnerAbstraction(BaseAbstraction):
    def __init__(
        self,
        app: str = "",
        cpu: Union[int, float, str] = 1.0,
        memory: Union[int, str] = 128,
        gpu: Union[GpuTypeAlias, List[GpuTypeAlias]] = GpuType.NoGPU,
        gpu_count: int = 0,
        image: Image = Image(),
        workers: int = 1,
        concurrent_requests: int = 1,
        keep_warm_seconds: float = 10.0,
        max_pending_tasks: int = 100,
        retries: int = 3,
        timeout: int = 3600,
        volumes: Optional[List[Volume]] = None,
        secrets: Optional[List[str]] = None,
        env: Optional[Dict[str, str]] = {},
        on_start: Optional[Callable] = None,
        on_deploy: Optional["AbstractCallableWrapper"] = None,
        callback_url: Optional[str] = None,
        authorized: bool = True,
        name: Optional[str] = None,
        autoscaler: Autoscaler = QueueDepthAutoscaler(),
        task_policy: TaskPolicy = TaskPolicy(),
        checkpoint_enabled: bool = False,
        entrypoint: Optional[List[str]] = None,
        ports: Optional[List[int]] = [],
        pricing: Optional[PricingPolicy] = None,
        inputs: Optional[Schema] = None,
        outputs: Optional[Schema] = None,
    ) -> None:
        super().__init__()

        if image is None:
            image = Image()

        formatted_env = []
        if env:
            formatted_env = [f"{k}={v}" for k, v in env.items()]

        self.name = name
        self.app = app
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
        self.on_deploy: "AbstractCallableWrapper" = self.parse_on_deploy(on_deploy)
        self.callback_url = callback_url or ""
        self.cpu = self.parse_cpu(cpu)
        self.memory = self.parse_memory(memory)
        self.gpu = gpu
        self.gpu_count = gpu_count
        self.volumes = volumes or []
        self.secrets = [SecretVar(name=s) for s in (secrets or [])]
        self.env: List[str] = formatted_env
        self.workers = workers
        self.concurrent_requests = concurrent_requests
        self.keep_warm_seconds = keep_warm_seconds
        self.max_pending_tasks = max_pending_tasks
        self.autoscaler = autoscaler
        self.task_policy = TaskPolicy(
            max_retries=task_policy.max_retries or retries,
            timeout=task_policy.timeout or timeout,
            ttl=task_policy.ttl,
        )
        self.checkpoint_enabled = checkpoint_enabled
        self.extra: dict = {}
        self.entrypoint: Optional[List[str]] = entrypoint

        if (self.gpu != "" or len(self.gpu) > 0) and self.gpu_count == 0:
            self.gpu_count = 1

        if on_start is not None:
            self._map_callable_to_attr(attr="on_start", func=on_start)

        self._gateway_stub: Optional[GatewayServiceStub] = None
        self._shell_stub: Optional[ShellServiceStub] = None
        self.syncer: FileSyncer = FileSyncer(self.gateway_stub)
        self.settings: SDKSettings = get_settings()
        self.config_context: ConfigContext = get_config_context()
        self.tmp_files: List[TempFile] = []
        self.is_websocket: bool = False
        self.ports: List[int] = ports or []
        self.pricing: Optional[PricingPolicy] = pricing
        self.inputs: Optional[Schema] = inputs
        self.outputs: Optional[Schema] = outputs

    def print_invocation_snippet(self, url_type: str = "") -> GetUrlResponse:
        """Print curl request to call deployed container URL"""

        res = self.gateway_stub.get_url(
            GetUrlRequest(
                stub_id=self.stub_id,
                deployment_id=getattr(self, "deployment_id", ""),
                url_type=url_type,
            )
        )
        if not res.ok:
            return terminal.error("Failed to get invocation URL", exit=False)

        if "<PORT>" in res.url:
            terminal.header("Exposed endpoints\n")

            for port in self.ports:
                terminal.print(f"\tPort {port}: {res.url.replace('<PORT>', str(port))}")

            terminal.print("")
            return res

        terminal.header("Invocation details")
        commands = [
            f"curl -X POST '{res.url}' \\",
            "-H 'Connection: keep-alive' \\",
            "-H 'Content-Type: application/json' \\",
            *(
                [f"-H 'Authorization: Bearer {self.config_context.token}' \\"]
                if self.authorized
                else []
            ),
            "-d '{}'",
        ]

        if self.is_websocket:
            res.url = res.url.replace("http://", "ws://").replace("https://", "wss://")
            commands = [
                f"websocat '{res.url}' \\",
                *(
                    [f"-H 'Authorization: Bearer {self.config_context.token}'"]
                    if self.authorized
                    else []
                ),
            ]

        terminal.print("\n".join(commands), crop=False, overflow="ignore")

        return res

    def parse_memory(self, memory_str: str) -> int:
        if not isinstance(memory_str, str):
            return memory_str

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

    @property
    def shell_stub(self) -> ShellServiceStub:
        if not self._shell_stub:
            self._shell_stub = ShellServiceStub(self.channel)
        return self._shell_stub

    @shell_stub.setter
    def shell_stub(self, value) -> None:
        self._shell_stub = value

    def parse_cpu(self, cpu: Union[float, str]) -> int:
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
        For Jupyter notebooks, serialize everything using cloudpickle.
        """
        if getattr(self, attr):
            return

        module = inspect.getmodule(func)

        # We check for the notebook environment before a normal module (.py file) because
        # marimo notebooks are normal python files.
        if is_notebook_env():
            tmp_file = TempFile(name=f"{func.__name__}.pkl", mode="wb")
            try:
                cloudpickle.dump(func, tmp_file)
                tmp_file.flush()
                module_name = os.path.basename(tmp_file.name)
                self.tmp_files.append(tmp_file)

                setattr(self, attr, f"{module_name}:{func.__name__}")
            except Exception as e:
                os.unlink(tmp_file.name)
                raise ValueError(f"Failed to pickle function: {str(e)}")
        elif hasattr(module, "__file__"):
            # Normal module case - use relative path
            module_file = os.path.relpath(module.__file__, start=os.getcwd()).replace("/", ".")
            module_name = os.path.splitext(module_file)[0]
            setattr(self, attr, f"{module_name}:{func.__name__}")
        else:
            module_name = "__main__"
            setattr(self, attr, f"{module_name}:{func.__name__}")

    def _remove_tmp_files(self) -> None:
        for tmp_file in self.tmp_files:
            tmp_file.close()

    def parse_gpu(self, gpu: Union[GpuTypeAlias, List[GpuTypeAlias]]) -> str:
        if not isinstance(gpu, str) and not isinstance(gpu, list):
            raise ValueError("Invalid GPU type")

        if isinstance(gpu, list):
            return ",".join([GpuType(g).value for g in gpu])
        else:
            return GpuType(gpu).value

    def parse_on_deploy(self, func: Callable):
        if func is None:
            return None

        if not callable(func) or not hasattr(func, "parent") or not hasattr(func, "func"):
            raise terminal.error(
                "Build failed: on_deploy must be a callable function with a function decorator"
            )

        return func

    def _schema_to_proto(self, py_schema):
        if py_schema is None:
            return None

        def _field_to_proto(field_dict):
            # Handle nested object fields
            if field_dict["type"] == "Object":
                # Recursively convert nested fields
                nested_fields = field_dict.get("fields", {}).get("fields", {})
                return SchemaFieldProto(
                    type="object",
                    fields=SchemaProto(
                        fields={k: _field_to_proto(v) for k, v in nested_fields.items()}
                    ),
                )
            else:
                return SchemaFieldProto(type=field_dict["type"])

        fields_dict = py_schema.to_dict()["fields"]
        return SchemaProto(fields={k: _field_to_proto(v) for k, v in fields_dict.items()})

    def prepare_runtime(
        self,
        *,
        func: Optional[Callable] = None,
        stub_type: str,
        force_create_stub: bool = False,
        ignore_patterns: Optional[List[str]] = None,
    ) -> bool:
        if called_on_import():
            return False

        if func is not None:
            self._map_callable_to_attr(attr="handler", func=func)

        stub_name = f"{stub_type}/{self.handler}" if self.handler else stub_type

        if self.runtime_ready:
            return True

        if not self.image_available:
            image_build_result: ImageBuildResult = self.image.build()

            if image_build_result and image_build_result.success:
                self.image_available = True
                self.image_id = image_build_result.image_id
                self.image.python_version = image_build_result.python_version
            else:
                terminal.error("Image build failed ❌", exit=False)
                return False

        if not self.files_synced:
            sync_result = self.syncer.sync(ignore_patterns=ignore_patterns)
            self._remove_tmp_files()

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
            self.gpu = self.parse_gpu(self.gpu)
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

        if not self.app:
            self.app = self.name or os.path.basename(os.getcwd())

        inputs = None
        if self.inputs:
            inputs = self._schema_to_proto(self.inputs)

        outputs = None
        if self.outputs:
            outputs = self._schema_to_proto(self.outputs)

        if not self.stub_created:
            stub_request = GetOrCreateStubRequest(
                object_id=self.object_id,
                image_id=self.image_id,
                stub_type=stub_type,
                name=stub_name,
                app_name=self.app,
                python_version=self.image.python_version,
                cpu=self.cpu,
                memory=self.memory,
                gpu=self.gpu,
                gpu_count=self.gpu_count,
                handler=self.handler,
                on_start=self.on_start,
                on_deploy=self.on_deploy.parent.handler if self.on_deploy else "",
                on_deploy_stub_id=self.on_deploy.parent.stub_id if self.on_deploy else "",
                callback_url=self.callback_url,
                keep_warm_seconds=self.keep_warm_seconds,
                workers=self.workers,
                max_pending_tasks=self.max_pending_tasks,
                volumes=[v.export() for v in self.volumes],
                secrets=self.secrets,
                env=self.env,
                force_create=force_create_stub,
                authorized=self.authorized,
                autoscaler=AutoscalerProto(
                    type=autoscaler_type,
                    max_containers=self.autoscaler.max_containers,
                    tasks_per_container=self.autoscaler.tasks_per_container,
                    min_containers=self.autoscaler.min_containers,
                ),
                task_policy=TaskPolicyProto(
                    max_retries=self.task_policy.max_retries,
                    timeout=self.task_policy.timeout,
                    ttl=self.task_policy.ttl,
                ),
                concurrent_requests=self.concurrent_requests,
                checkpoint_enabled=self.checkpoint_enabled,
                extra=json.dumps(self.extra),
                entrypoint=self.entrypoint,
                ports=self.ports,
                pricing=PricingPolicyProto(
                    cost_per_task=self.pricing.cost_per_task,
                    cost_per_task_duration_ms=self.pricing.cost_per_task_duration_ms,
                    cost_model=self.pricing.cost_model,
                    max_in_flight=self.pricing.max_in_flight,
                )
                if self.pricing
                else None,
                inputs=inputs,
                outputs=outputs,
            )
            if _is_stub_created_for_workspace():
                stub_response: GetOrCreateStubResponse = self.gateway_stub.get_or_create_stub(
                    stub_request
                )
            else:
                with _stub_creation_lock:
                    stub_response: GetOrCreateStubResponse = self.gateway_stub.get_or_create_stub(
                        stub_request
                    )
                    _set_stub_created_for_workspace(True)

            if stub_response.ok:
                self.stub_created = True
                self.stub_id = stub_response.stub_id
                if stub_response.warn_msg:
                    terminal.warn(stub_response.warn_msg)
            else:
                if err := stub_response.err_msg:
                    terminal.error(err, exit=False)
                else:
                    terminal.error("Failed to get or create stub", exit=False)
                return False

        self.runtime_ready = True
        return True


class AbstractCallableWrapper:
    def __init__(self, func: Callable, parent: RunnerAbstraction):
        self.func = func
        self.parent = parent

    def __call__(self, *args, **kwargs):
        raise NotImplementedError
