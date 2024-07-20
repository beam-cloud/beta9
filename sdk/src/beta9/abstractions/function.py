import concurrent.futures
from typing import Any, Callable, Iterator, List, Optional, Sequence, Union

import cloudpickle

from .. import terminal
from ..abstractions.base.runner import (
    FUNCTION_DEPLOYMENT_STUB_TYPE,
    FUNCTION_STUB_TYPE,
    RunnerAbstraction,
)
from ..abstractions.image import Image
from ..abstractions.volume import Volume
from ..channel import with_grpc_error_handling
from ..clients.function import (
    FunctionInvokeRequest,
    FunctionInvokeResponse,
    FunctionServiceStub,
)
from ..clients.gateway import DeployStubRequest, DeployStubResponse
from ..env import is_local
from ..sync import FileSyncer
from ..type import GpuType, GpuTypeAlias


class Function(RunnerAbstraction):
    """
    Decorator which allows you to run the decorated function in a remote container.

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
        timeout (Optional[int]):
            The maximum number of seconds a task can run before it times out.
            Default is 3600. Set it to -1 to disable the timeout.
        retries (Optional[int]):
            The maximum number of times a task will be retried if the container crashes. Default is 3.
        callback_url (Optional[str]):
            An optional URL to send a callback to when a task is completed, timed out, or cancelled.
        volumes (Optional[List[Volume]]):
            A list of storage volumes to be associated with the function. Default is [].
        secrets (Optional[List[str]):
            A list of secrets that are injected into the container as environment variables. Default is [].
        name (Optional[str]):
            An optional name for this function, used during deployment. If not specified, you must specify the name
            at deploy time with the --name argument
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
        memory: Union[int, str] = 128,
        gpu: GpuTypeAlias = GpuType.NoGPU,
        image: Image = Image(),
        timeout: int = 3600,
        retries: int = 3,
        callback_url: Optional[str] = "",
        volumes: Optional[List[Volume]] = None,
        secrets: Optional[List[str]] = None,
        name: Optional[str] = None,
    ) -> None:
        super().__init__(
            cpu=cpu,
            memory=memory,
            gpu=gpu,
            image=image,
            timeout=timeout,
            retries=retries,
            callback_url=callback_url,
            volumes=volumes,
            secrets=secrets,
            name=name,
        )

        self._function_stub: Optional[FunctionServiceStub] = None
        self.syncer: FileSyncer = FileSyncer(self.gateway_stub)

    def __call__(self, func):
        return _CallableWrapper(func, self)

    @property
    def function_stub(self) -> FunctionServiceStub:
        if not self._function_stub:
            self._function_stub = FunctionServiceStub(self.channel)
        return self._function_stub

    @function_stub.setter
    def function_stub(self, value: FunctionServiceStub) -> None:
        self._function_stub = value


class _CallableWrapper:
    def __init__(self, func: Callable, parent: Function) -> None:
        self.func: Callable = func
        self.parent: Function = parent

    @with_grpc_error_handling
    def __call__(self, *args, **kwargs) -> Any:
        if not is_local():
            return self.local(*args, **kwargs)

        if not self.parent.prepare_runtime(
            func=self.func,
            stub_type=FUNCTION_STUB_TYPE,
        ):
            return

        with terminal.progress("Working..."):
            return self._call_remote(*args, **kwargs)

    @with_grpc_error_handling
    def _call_remote(self, *args, **kwargs) -> Any:
        args = cloudpickle.dumps(
            {
                "args": args,
                "kwargs": kwargs,
            },
        )

        terminal.header(f"Running function: <{self.parent.handler}>")
        last_response: Optional[FunctionInvokeResponse] = None

        for r in self.parent.function_stub.function_invoke(
            FunctionInvokeRequest(
                stub_id=self.parent.stub_id,
                args=args,
            )
        ):
            if r.output != "":
                terminal.detail(r.output, end="")

            if r.done or r.exit_code != 0:
                last_response = r
                break

        if last_response is None or not last_response.done or last_response.exit_code != 0:
            terminal.error(f"Function failed <{last_response.task_id}> ❌", exit=False)
            return

        terminal.header(f"Function complete <{last_response.task_id}>")
        return cloudpickle.loads(last_response.result)

    def local(self, *args, **kwargs) -> Any:
        return self.func(*args, **kwargs)

    def remote(self, *args, **kwargs) -> Any:
        return self(*args, **kwargs)

    def serve(self, **kwargs):
        terminal.error("Serve has not yet been implemented for functions.")

    def deploy(self, name: str) -> bool:
        name = name or self.parent.name
        if not name or name == "":
            terminal.error(
                "You must specify an app name (either in the decorator or via the --name argument)."
            )

        if not self.parent.prepare_runtime(
            func=self.func, stub_type=FUNCTION_DEPLOYMENT_STUB_TYPE, force_create_stub=True
        ):
            return False

        terminal.header("Deploying function")
        deploy_response: DeployStubResponse = self.parent.gateway_stub.deploy_stub(
            DeployStubRequest(stub_id=self.parent.stub_id, name=name)
        )

        if deploy_response.ok:
            terminal.header("Deployed 🎉")
            self.parent.print_invocation_snippet(deploy_response.invoke_url)

        return deploy_response.ok

    def _format_args(self, args):
        if isinstance(args, tuple):
            return list(args)
        elif not isinstance(args, list):
            return [args]
        return args

    def _threaded_map(self, inputs: Sequence) -> Iterator[Any]:
        with terminal.progress(f"Running {len(inputs)} container(s)..."):
            with concurrent.futures.ThreadPoolExecutor(len(inputs)) as pool:
                futures = [
                    pool.submit(self._call_remote, *self._format_args(args)) for args in inputs
                ]
                for future in concurrent.futures.as_completed(futures):
                    yield future.result()

    def map(self, inputs: Sequence[Any]) -> Iterator[Any]:
        if not self.parent.prepare_runtime(
            func=self.func,
            stub_type=FUNCTION_STUB_TYPE,
        ):
            terminal.error("Function failed to prepare runtime ❌")

        return self._threaded_map(inputs)
