import os
from typing import Any, Callable, List, Optional, Union

from .. import terminal
from ..abstractions.base.runner import (
    ENDPOINT_DEPLOYMENT_STUB_TYPE,
    ENDPOINT_SERVE_STUB_TYPE,
    RunnerAbstraction,
)
from ..abstractions.image import Image
from ..abstractions.volume import Volume
from ..clients.endpoint import (
    EndpointServiceStub,
    StartEndpointServeRequest,
    StopEndpointServeRequest,
)
from ..clients.gateway import DeployStubRequest, DeployStubResponse
from ..env import is_local


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
        concurrency (Optional[int]):
            The number of concurrent tasks to handle per container.
            Modifying this parameter can improve throughput for certain workloads.
            Workers will share the CPU, Memory, and GPU defined.
            You may need to increase these values to increase concurrency.
            Default is 1.
        max_containers (int):
            The maximum number of containers that the task queue will autoscale to. If the number of tasks
            in the queue goes over the concurrency value, the task queue will automatically add containers to
            handle the load.
            Default is 1.
        keep_warm_seconds (int):
            The duration in seconds to keep the task queue warm even if there are no pending
            tasks. Keeping the queue warm helps to reduce the latency when new tasks arrive.
            Default is 10s.
        max_pending_tasks (int):
            The maximum number of tasks that can be pending in the queue. If the number of
            pending tasks exceeds this value, the task queue will stop accepting new tasks.
            Default is 100.
    Example:
        ```python
        from beta9 import endpoint, Image

        @endpoint(
            cpu=1.0,
            memory=128,
            gpu="T4",
            image=Image(python_packages=["torch"]),
            keep_warm_seconds=1000,
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
        gpu: str = "",
        image: Image = Image(),
        timeout: int = 180,
        concurrency: int = 1,
        max_containers: int = 1,
        keep_warm_seconds: int = 300,
        max_pending_tasks: int = 100,
        on_start: Optional[Callable] = None,
        volumes: Optional[List[Volume]] = None,
    ):
        super().__init__(
            cpu=cpu,
            memory=memory,
            gpu=gpu,
            image=image,
            concurrency=concurrency,
            max_containers=max_containers,
            timeout=timeout,
            retries=0,
            keep_warm_seconds=keep_warm_seconds,
            max_pending_tasks=max_pending_tasks,
            on_start=on_start,
            volumes=volumes,
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
        if not self.parent.prepare_runtime(
            func=self.func, stub_type=ENDPOINT_DEPLOYMENT_STUB_TYPE, force_create_stub=True
        ):
            return False

        terminal.header("Deploying endpoint")
        deploy_response: DeployStubResponse = self.parent.run_sync(
            self.parent.gateway_stub.deploy_stub(
                DeployStubRequest(stub_id=self.parent.stub_id, name=name)
            )
        )

        if deploy_response.ok:
            base_url = self.parent.settings.api_host
            if not base_url.startswith(("http://", "https://")):
                base_url = f"http://{base_url}"

            terminal.header("Deployed 🎉")
            self.parent.print_invocation_snippet(
                invocation_url=f"{base_url}/endpoint/{name}/v{deploy_response.version}"
            )

        return deploy_response.ok

    def serve(self):
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

                return self.parent.run_sync(
                    self._serve(dir=os.getcwd(), object_id=self.parent.object_id)
                )
        except KeyboardInterrupt:
            response = terminal.prompt(
                text="Would you like to stop the container? (y/n)", default="y"
            )
            if response == "y":
                terminal.header("Stopping serve container")
                self.parent.run_sync(
                    self.parent.endpoint_stub.stop_endpoint_serve(
                        StopEndpointServeRequest(stub_id=self.parent.stub_id)
                    )
                )

        terminal.print("Goodbye 👋")

    async def _serve(self, *, dir: str, object_id: str):
        sync_task = self.parent.loop.create_task(
            self.parent.sync_dir_to_workspace(dir=dir, object_id=object_id)
        )
        try:
            async for r in self.parent.endpoint_stub.start_endpoint_serve(
                StartEndpointServeRequest(
                    stub_id=self.parent.stub_id,
                )
            ):
                if r.output != "":
                    terminal.detail(r.output, end="")

                if r.done or r.exit_code != 0:
                    last_response = r
                    break

            if last_response is None or not last_response.done or last_response.exit_code != 0:
                terminal.error("Serve container failed ☠️")
        finally:
            sync_task.cancel()
