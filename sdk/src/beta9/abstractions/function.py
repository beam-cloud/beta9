import asyncio
import os
from typing import Any, Callable, Iterator, List, Optional, Sequence, Union

import cloudpickle
from beta9 import terminal
from beta9.abstractions.base.runner import (
    FUNCTION_DEPLOYMENT_STUB_TYPE,
    FUNCTION_STUB_TYPE,
    RunnerAbstraction,
)
from beta9.abstractions.image import Image
from beta9.abstractions.volume import Volume
from beta9.clients.function import (
    FunctionInvokeResponse,
    FunctionServiceStub,
)
from beta9.clients.gateway import DeployStubResponse
from beta9.config import GatewayConfig, get_gateway_config
from beta9.sync import FileSyncer


class Function(RunnerAbstraction):
    """
    Decorator which allows you to run the decorated function in a remote container.

    Parameters:
        cpu (Union[int, float, str]):
            The number of CPU cores allocated to the container. Default is 1.0.
        memory (int):
            The amount of memory allocated to the container. It should be specified in
            megabytes (e.g., 128 for 128 megabytes). Default is 128.
        gpu (Union[GpuType, str]):
            The type or name of the GPU device to be used for GPU-accelerated tasks. If not
            applicable or no GPU required, leave it empty. Default is [GpuType.NoGPU](#gputype).
        image (Union[Image, dict]):
            The container image used for the task execution. Default is [Image](#image).
    Example:
        ```python
        from beta9 import function, Image

        @function(cpu=1.0, memory=128, gpu="T4", image=Image(python_packages=["torch"]), keep_warm_seconds=1000)
        def transcribe(filename: str):
            print(filename)
            return "some_result"

        # Call a function in a remote container
        function.remote("some_file.mp4")

        # Map the function over several inputs
        # Each of these inputs will be routed to remote containers
        for result in function.map(["file1.mp4", "file2.mp4"]):
            print(result)


        ```
    """

    def __init__(
        self,
        cpu: Union[int, float, str] = 1.0,
        memory: int = 128,
        gpu: str = "",
        image: Image = Image(),
        volumes: Optional[List[Volume]] = None,
    ) -> None:
        super().__init__(cpu=cpu, memory=memory, gpu=gpu, image=image, volumes=volumes)

        self.function_stub: FunctionServiceStub = FunctionServiceStub(self.channel)
        self.syncer: FileSyncer = FileSyncer(self.gateway_stub)

    def __call__(self, func):
        return _CallableWrapper(func, self)


class _CallableWrapper:
    def __init__(self, func: Callable, parent: Function) -> None:
        self.func: Callable = func
        self.parent: Function = parent

    def __call__(self, *args, **kwargs) -> Any:
        container_id = os.getenv("CONTAINER_ID")
        if container_id:
            return self.local(*args, **kwargs)

        if not self.parent.prepare_runtime(
            func=self.func,
            stub_type=FUNCTION_STUB_TYPE,
        ):
            return

        with terminal.progress("Working..."):
            return self.parent.run_sync(self._call_remote(*args, **kwargs))

    async def _call_remote(self, *args, **kwargs) -> Any:
        args = cloudpickle.dumps(
            {
                "args": args,
                "kwargs": kwargs,
            },
        )

        terminal.header("Running function")
        last_response: Union[None, FunctionInvokeResponse] = None

        async for r in self.parent.function_stub.function_invoke(
            stub_id=self.parent.stub_id,
            args=args,
        ):
            if r.output != "":
                terminal.detail(r.output)

            if r.done or r.exit_code != 0:
                last_response = r
                break

        if not last_response.done or last_response.exit_code != 0:
            terminal.error("Function failed ☠️")
            return None

        terminal.header("Function complete 🎉")
        return cloudpickle.loads(last_response.result)

    def local(self, *args, **kwargs) -> Any:
        return self.func(*args, **kwargs)

    def remote(self, *args, **kwargs) -> Any:
        return self(*args, **kwargs)

    def deploy(self, name: str) -> bool:
        if not self.parent.prepare_runtime(
            func=self.func, stub_type=FUNCTION_DEPLOYMENT_STUB_TYPE, force_create_stub=True
        ):
            return False

        terminal.header("Deploying function")
        deploy_response: DeployStubResponse = self.parent.run_sync(
            self.parent.gateway_stub.deploy_stub(stub_id=self.parent.stub_id, name=name)
        )

        if deploy_response.ok:
            gateway_config: GatewayConfig = get_gateway_config()
            gateway_url = f"{gateway_config.gateway_host}:{gateway_config.gateway_port}"

            terminal.header("Deployed 🎉")
            terminal.detail(
                f"Call your deployment at: {gateway_url}/api/v1/function/{name}/v{deploy_response.version}"
            )

        return deploy_response.ok

    def _gather_and_yield_results(self, inputs: Sequence) -> Iterator[Any]:
        container_count = len(inputs)

        async def _gather_async():
            tasks = [asyncio.create_task(self._call_remote(input)) for input in inputs]
            for task in asyncio.as_completed(tasks):
                yield await task

        async_gen = _gather_async()
        with terminal.progress(f"Running {container_count} containers..."):
            while True:
                try:
                    yield self.parent.loop.run_until_complete(async_gen.__anext__())
                except StopAsyncIteration:
                    break

    def map(self, inputs: Sequence[Any]) -> Iterator[Any]:
        if not self.parent.prepare_runtime(
            func=self.func,
            stub_type=FUNCTION_STUB_TYPE,
        ):
            terminal.error("Function failed to prepare runtime ☠️")

        return self._gather_and_yield_results(inputs)
