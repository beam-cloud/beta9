import os
import threading
from typing import Any, Callable, List, Optional, Union

from .. import terminal
from ..abstractions.base.runner import (
    ASGI_DEPLOYMENT_STUB_TYPE,
    ASGI_SERVE_STUB_TYPE,
    ASGI_STUB_TYPE,
    ENDPOINT_DEPLOYMENT_STUB_TYPE,
    ENDPOINT_SERVE_STUB_TYPE,
    ENDPOINT_STUB_TYPE,
    RunnerAbstraction,
)
from ..abstractions.image import Image
from ..abstractions.volume import Volume
from ..channel import with_grpc_error_handling
from ..clients.endpoint import (
    EndpointServeKeepAliveRequest,
    EndpointServiceStub,
    StartEndpointServeRequest,
    StartEndpointServeResponse,
    StopEndpointServeRequest,
)
from ..env import is_local
from ..type import Autoscaler, GpuType, GpuTypeAlias, QueueDepthAutoscaler, TaskPolicy
from .mixins import DeployableMixin


class Endpoint(RunnerAbstraction):
    """
    Decorator which allows you to create a web endpoint out of the decorated function.
    Tasks are invoked synchronously as HTTP requests.

    Parameters:
        cpu (Union[int, float, str]):
            The number of CPU cores allocated to the container. Default is 1.0.
        memory (Union[int, str]):
            The amount of memory allocated to the container. It should be specified in
            MiB, or as a string with units (e.g. "1Gi"). Default is 128 MiB.
        gpu (Union[GpuType, str]):
            The type or name of the GPU device to be used for GPU-accelerated tasks. If not
            applicable or no GPU required, leave it empty. Default is [GpuType.NoGPU](#gputype).
        image (Union[Image, dict]):
            The container image used for the task execution. Default is [Image](#image).
        volumes (Optional[List[Volume]]):
            A list of volumes to be mounted to the endpoint. Default is None.
        timeout (Optional[int]):
            The maximum number of seconds a task can run before it times out.
            Default is 3600. Set it to -1 to disable the timeout.
        retries (Optional[int]):
            The maximum number of times a task will be retried if the container crashes. Default is 3.
        workers (Optional[int]):
            The number of processes handling tasks per container.
            Modifying this parameter can improve throughput for certain workloads.
            Workers will share the CPU, Memory, and GPU defined.
            You may need to increase these values to increase concurrency.
            Default is 1.
        keep_warm_seconds (int):
            The duration in seconds to keep the task queue warm even if there are no pending
            tasks. Keeping the queue warm helps to reduce the latency when new tasks arrive.
            Default is 10s.
        max_pending_tasks (int):
            The maximum number of tasks that can be pending in the queue. If the number of
            pending tasks exceeds this value, the task queue will stop accepting new tasks.
            Default is 100.
        secrets (Optional[List[str]):
            A list of secrets that are injected into the container as environment variables. Default is [].
        name (Optional[str]):
            An optional name for this endpoint, used during deployment. If not specified, you must specify the name
            at deploy time with the --name argument
        authorized (Optional[str]):
            If false, allows the endpoint to be invoked without an auth token.
            Default is True.
        autoscaler (Optional[Autoscaler]):
            Configure a deployment autoscaler - if specified, you can use scale your function horizontally using
            various autoscaling strategies (Defaults to QueueDepthAutoscaler())
        callback_url (Optional[str]):
            An optional URL to send a callback to when a task is completed, timed out, or cancelled.
        task_policy (TaskPolicy):
            The task policy for the function. This helps manage the lifecycle of an individual task.
            Setting values here will override timeout and retries.
    Example:
        ```python
        from beta9 import endpoint, Image

        @endpoint(
            cpu=1.0,
            memory=128,
            gpu="T4",
            image=Image(python_packages=["torch"]),
            keep_warm_seconds=1000,
            name="my-app",
        )
        def multiply(**inputs):
            result = inputs["x"] * 2
            return {"result": result}
        ```
    """

    def __init__(
        self,
        cpu: Union[int, float, str] = 1.0,
        memory: Union[int, str] = 128,
        gpu: GpuTypeAlias = GpuType.NoGPU,
        image: Image = Image(),
        timeout: int = 180,
        workers: int = 1,
        keep_warm_seconds: int = 180,
        max_pending_tasks: int = 100,
        on_start: Optional[Callable] = None,
        volumes: Optional[List[Volume]] = None,
        secrets: Optional[List[str]] = None,
        name: Optional[str] = None,
        authorized: bool = True,
        autoscaler: Autoscaler = QueueDepthAutoscaler(),
        callback_url: Optional[str] = None,
        task_policy: TaskPolicy = TaskPolicy(),
    ):
        super().__init__(
            cpu=cpu,
            memory=memory,
            gpu=gpu,
            image=image,
            workers=workers,
            timeout=timeout,
            retries=0,
            keep_warm_seconds=keep_warm_seconds,
            max_pending_tasks=max_pending_tasks,
            on_start=on_start,
            volumes=volumes,
            secrets=secrets,
            name=name,
            authorized=authorized,
            autoscaler=autoscaler,
            callback_url=callback_url,
            task_policy=task_policy,
        )

        self._endpoint_stub: Optional[EndpointServiceStub] = None

    @property
    def endpoint_stub(self) -> EndpointServiceStub:
        if not self._endpoint_stub:
            self._endpoint_stub = EndpointServiceStub(self.channel)
        return self._endpoint_stub

    def __call__(self, func):
        return _CallableWrapper(func, self)


class ASGI(Endpoint):
    def __init__(
        self,
        cpu: Union[int, float, str] = 1.0,
        memory: Union[int, str] = 128,
        gpu: GpuTypeAlias = GpuType.NoGPU,
        image: Image = Image(),
        timeout: int = 180,
        workers: int = 1,
        keep_warm_seconds: int = 180,
        max_pending_tasks: int = 100,
        on_start: Optional[Callable] = None,
        volumes: Optional[List[Volume]] = None,
        secrets: Optional[List[str]] = None,
        name: Optional[str] = None,
        authorized: bool = True,
        autoscaler: Autoscaler = QueueDepthAutoscaler(),
        callback_url: Optional[str] = None,
    ):
        super().__init__(
            cpu,
            memory,
            gpu,
            image,
            timeout,
            workers,
            keep_warm_seconds,
            max_pending_tasks,
            on_start,
            volumes,
            secrets,
            name,
            authorized,
            autoscaler,
            callback_url,
        )

        self.is_asgi = True


class _CallableWrapper(DeployableMixin):
    deployment_stub_type = ENDPOINT_DEPLOYMENT_STUB_TYPE

    base_stub_type = ENDPOINT_STUB_TYPE

    def __init__(self, func: Callable, parent: Union[Endpoint, ASGI]):
        self.func: Callable = func
        self.parent: Union[Endpoint, ASGI] = parent

        if getattr(self.parent, "is_asgi", None):
            self.deployment_stub_type = ASGI_DEPLOYMENT_STUB_TYPE
            self.base_stub_type = ASGI_STUB_TYPE

    def __call__(self, *args, **kwargs) -> Any:
        if not is_local():
            return self.local(*args, **kwargs)

        raise NotImplementedError("Direct calls to Endpoints are not supported.")

    def local(self, *args, **kwargs) -> Any:
        return self.func(*args, **kwargs)

    @with_grpc_error_handling
    def serve(self, timeout: int = 0, url_type: str = ""):
        stub_type = ENDPOINT_SERVE_STUB_TYPE

        if getattr(self.parent, "is_asgi", None):
            stub_type = ASGI_SERVE_STUB_TYPE

        if not self.parent.prepare_runtime(
            func=self.func, stub_type=stub_type, force_create_stub=True
        ):
            return False

        try:
            with terminal.progress("Serving endpoint..."):
                self.parent.print_invocation_snippet(url_type=url_type)

                return self._serve(
                    dir=os.getcwd(), object_id=self.parent.object_id, timeout=timeout
                )

        except KeyboardInterrupt:
            self._handle_serve_interrupt()

    def _handle_serve_interrupt(self) -> None:
        terminal.header("Stopping serve container")
        self.parent.endpoint_stub.stop_endpoint_serve(
            StopEndpointServeRequest(stub_id=self.parent.stub_id)
        )
        terminal.print("Goodbye 👋")
        os._exit(0)  # kills all threads immediately

    def _serve(self, *, dir: str, object_id: str, timeout: int = 0):
        def notify(*_, **__):
            self.parent.endpoint_stub.endpoint_serve_keep_alive(
                EndpointServeKeepAliveRequest(
                    stub_id=self.parent.stub_id,
                    timeout=timeout,
                )
            )

        threading.Thread(
            target=self.parent.sync_dir_to_workspace,
            kwargs={"dir": dir, "object_id": object_id, "on_event": notify},
            daemon=True,
        ).start()

        r: Optional[StartEndpointServeResponse] = None
        for r in self.parent.endpoint_stub.start_endpoint_serve(
            StartEndpointServeRequest(
                stub_id=self.parent.stub_id,
                timeout=timeout,
            )
        ):
            if r.output != "":
                terminal.detail(r.output, end="")

            if r.done or r.exit_code != 0:
                break

        if r is None or not r.done or r.exit_code != 0:
            terminal.error("Serve container failed ❌")

        terminal.warn("Endpoint serve timed out. Container has been stopped.")
