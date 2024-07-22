import os
import threading
from typing import Any, Callable, List, Optional, Union

from .. import terminal
from ..abstractions.base.runner import (
    ENDPOINT_DEPLOYMENT_STUB_TYPE,
    ENDPOINT_SERVE_STUB_TYPE,
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
from ..clients.gateway import DeployStubRequest, DeployStubResponse
from ..env import is_local
from ..type import Autoscaler, GpuType, GpuTypeAlias, QueueDepthAutoscaler


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
        )

        self._endpoint_stub: Optional[EndpointServiceStub] = None

    @property
    def endpoint_stub(self) -> EndpointServiceStub:
        if not self._endpoint_stub:
            self._endpoint_stub = EndpointServiceStub(self.channel)
        return self._endpoint_stub

    def __call__(self, func):
        return _CallableWrapper(func, self)


class _CallableWrapper:
    def __init__(self, func: Callable, parent: Endpoint):
        self.func: Callable = func
        self.parent: Endpoint = parent

    def __call__(self, *args, **kwargs) -> Any:
        if not is_local():
            return self.local(*args, **kwargs)

        raise NotImplementedError("Direct calls to Endpoints are not supported.")

    def local(self, *args, **kwargs) -> Any:
        return self.func(*args, **kwargs)

    def deploy(self, name: str) -> bool:
        name = name or self.parent.name
        if not name or name == "":
            terminal.error(
                "You must specify an app name (either in the decorator or via the --name argument)."
            )

        if not self.parent.prepare_runtime(
            func=self.func, stub_type=ENDPOINT_DEPLOYMENT_STUB_TYPE, force_create_stub=True
        ):
            return False

        terminal.header("Deploying endpoint")
        deploy_response: DeployStubResponse = self.parent.gateway_stub.deploy_stub(
            DeployStubRequest(stub_id=self.parent.stub_id, name=name)
        )

        if deploy_response.ok:
            terminal.header("Deployed 🎉")
            self.parent.print_invocation_snippet(deploy_response.invoke_url)

        return deploy_response.ok

    @with_grpc_error_handling
    def serve(self, timeout: int = 0):
        if not self.parent.prepare_runtime(
            func=self.func, stub_type=ENDPOINT_SERVE_STUB_TYPE, force_create_stub=True
        ):
            return False

        try:
            with terminal.progress("Serving endpoint..."):
                base_url = self.parent.settings.api_host
                if not base_url.startswith(("http://", "https://")):
                    base_url = f"http://{base_url}"

                self.parent.print_invocation_snippet(
                    invocation_url=f"{base_url}/endpoint/id/{self.parent.stub_id}"
                )

                return self._serve(
                    dir=os.getcwd(), object_id=self.parent.object_id, timeout=timeout
                )

        except KeyboardInterrupt:
            self._handle_serve_interrupt()

    def _handle_serve_interrupt(self) -> None:
        response = "y"

        try:
            response = terminal.prompt(
                text="Would you like to stop the container? (y/n)", default="y"
            )
        except KeyboardInterrupt:
            pass

        if response == "y":
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
