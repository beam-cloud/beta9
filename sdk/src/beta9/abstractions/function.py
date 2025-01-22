import concurrent.futures
import inspect
import time
from datetime import datetime, timezone
from typing import Any, Callable, Iterator, List, Optional, Sequence, Union

import cloudpickle

from .. import terminal
from ..abstractions.base.runner import (
    FUNCTION_DEPLOYMENT_STUB_TYPE,
    FUNCTION_STUB_TYPE,
    SCHEDULE_DEPLOYMENT_STUB_TYPE,
    SCHEDULE_STUB_TYPE,
    AbstractCallableWrapper,
    RunnerAbstraction,
)
from ..abstractions.image import Image
from ..abstractions.volume import Volume
from ..channel import with_grpc_error_handling
from ..clients.function import (
    FunctionInvokeRequest,
    FunctionInvokeResponse,
    FunctionScheduleRequest,
    FunctionServiceStub,
)
from ..env import called_on_import, is_local
from ..sync import FileSyncer
from ..type import GpuType, GpuTypeAlias, TaskPolicy
from .mixins import DeployableMixin


class Function(RunnerAbstraction):
    """
    Decorator which allows you to run the decorated function in a remote container.

    Parameters:
        cpu (Union[int, float, str]):
            The number of CPU cores allocated to the container. Default is 1.0.
        memory (Union[int, str]):
            The amount of memory allocated to the container. It should be specified in
            MiB, or as a string with units (e.g. "1Gi"). Default is 128 MiB.
        gpu (Union[GpuTypeAlias, List[GpuTypeAlias]]):
            The type or name of the GPU device to be used for GPU-accelerated tasks. If not
            applicable or no GPU required, leave it empty.
            You can specify multiple GPUs by providing a list of GpuTypeAlias. If you specify several GPUs,
            the scheduler prioritizes their selection based on their order in the list.
        gpu_count (int):
            The number of GPUs allocated to the container. Default is 0. If a GPU is
            specified but this value is set to 0, it will be automatically updated to 1.
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
        task_policy (TaskPolicy):
            The task policy for the function. This helps manage the lifecycle of an individual task.
            Setting values here will override timeout and retries.
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
        gpu: Union[GpuTypeAlias, List[GpuTypeAlias]] = GpuType.NoGPU,
        gpu_count: int = 0,
        image: Image = Image(),
        timeout: int = 3600,
        retries: int = 3,
        callback_url: Optional[str] = None,
        volumes: Optional[List[Volume]] = None,
        secrets: Optional[List[str]] = None,
        name: Optional[str] = None,
        task_policy: TaskPolicy = TaskPolicy(),
        on_deploy: Optional[AbstractCallableWrapper] = None,
    ) -> None:
        super().__init__(
            cpu=cpu,
            memory=memory,
            gpu=gpu,
            gpu_count=gpu_count,
            image=image,
            timeout=timeout,
            retries=retries,
            callback_url=callback_url,
            volumes=volumes,
            secrets=secrets,
            name=name,
            task_policy=task_policy,
            on_deploy=on_deploy,
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


class _CallableWrapper(DeployableMixin):
    base_stub_type = FUNCTION_STUB_TYPE
    deployment_stub_type = FUNCTION_DEPLOYMENT_STUB_TYPE

    def __init__(self, func: Callable, parent: Function) -> None:
        self.func: Callable = func
        self.parent: Function = parent

    @with_grpc_error_handling
    def __call__(self, *args, **kwargs) -> Any:
        if called_on_import():
            return

        if not is_local():
            return self.local(*args, **kwargs)

        if not self.parent.prepare_runtime(
            func=self.func,
            stub_type=self.base_stub_type,
        ):
            return

        try:
            with terminal.progress("Working..."):
                return self._call_remote(*args, **kwargs)
        except KeyboardInterrupt:
            terminal.error("Exiting Shell. Your function will continue running remotely.")

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
        terminal.error("Serve has not yet been implemented.")

    def _format_args(self, args):
        if isinstance(args, tuple):
            return list(args)
        elif not isinstance(args, list):
            return [args]
        return args

    def _threaded_map(self, inputs: Sequence[Any]) -> Iterator[Any]:
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
            stub_type=self.base_stub_type,
        ):
            terminal.error("Function failed to prepare runtime ❌")

        return self._threaded_map(inputs)


class ScheduleWrapper(_CallableWrapper):
    base_stub_type = SCHEDULE_STUB_TYPE
    deployment_stub_type = SCHEDULE_DEPLOYMENT_STUB_TYPE

    def deploy(self, *args: Any, **kwargs: Any) -> bool:
        deployed = super().deploy(invocation_details_func=self.invocation_details, *args, **kwargs)
        if deployed:
            res = self.parent.function_stub.function_schedule(
                FunctionScheduleRequest(
                    stub_id=self.parent.stub_id,
                    when=self.parent.when,
                    deployment_id=self.parent.deployment_id,
                )
            )
            if not res.ok:
                terminal.error(res.err_msg, exit=False)
                return False
        return deployed

    def invocation_details(self, **kwargs) -> None:
        """
        Print the schedule details.

        Used as an alternative view when deploying a scheduled function.
        """
        from croniter import croniter

        terminal.header("Schedule details")
        terminal.print(f"Schedule: {self.parent.when}")
        terminal.print("Upcoming:")

        current_tz = time.tzname[time.localtime().tm_isdst]
        cron = croniter(self.parent.when, datetime.now())
        cron_utc = croniter(self.parent.when, datetime.now(timezone.utc))
        for i in range(3):
            next_run = cron.get_next(datetime)
            next_run_utc = cron_utc.get_next(datetime)
            terminal.print(
                (
                    f"  [bright_white]{i+1}.[/bright_white] {next_run_utc:%Y-%m-%d %H:%M:%S %Z} "
                    f"({next_run:%Y-%m-%d %H:%M:%S} {current_tz})"
                )
            )


class Schedule(Function):
    """
    Decorator which allows you to run the decorated function as a scheduled job.

    Parameters:
        when (str):
            A cron expression that specifies when the task should be run. For example "*/5 * * * *".
            The timezone is always UTC.
        cpu (Union[int, float, str]):
            The number of CPU cores allocated to the container. Default is 1.0.
        memory (Union[int, str]):
            The amount of memory allocated to the container. It should be specified in
            MiB, or as a string with units (e.g. "1Gi"). Default is 128 MiB.
        gpu (Union[GpuType, str]):
            The type or name of the GPU device to be used for GPU-accelerated tasks. If not
            applicable or no GPU required, leave it empty. Default is [GpuType.NoGPU](#gputype).
        gpu_count (int):
            The number of GPUs allocated to the container. Default is 0. If a GPU is
            specified but this value is set to 0, it will be automatically updated to 1.
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
        from beta9 import schedule

        @schedule(when="*/5 * * * *")
        def task():
            print("Hi, from scheduled task!")

        ```

    Predefined schedules:
        These can be used in the `when` parameter.

        @yearly (or @annually)  Run once a year at midnight of 1 January                    0 0 1 1 *
        @monthly                Run once a month at midnight of the first day of the month  0 0 1 * *
        @weekly                 Run once a week at midnight on Sunday                       0 0 * * 0
        @daily (or @midnight)   Run once a day at midnight                                  0 0 * * *
        @hourly                 Run once an hour at the beginning of the hour               0 * * * *
    """

    def __init__(
        self,
        when: str,
        cpu: Union[int, float, str] = 1.0,
        memory: Union[int, str] = 128,
        gpu: GpuTypeAlias = GpuType.NoGPU,
        gpu_count: int = 0,
        image: Image = Image(),
        timeout: int = 3600,
        retries: int = 3,
        callback_url: Optional[str] = None,
        volumes: Optional[List[Volume]] = None,
        secrets: Optional[List[str]] = None,
        name: Optional[str] = None,
        on_deploy: Optional[AbstractCallableWrapper] = None,
    ) -> None:
        params = inspect.signature(Function.__init__).parameters
        kwargs = {k: v for k, v in locals().items() if k in params and k != "self"}
        super().__init__(**kwargs)

        self.when = when

    def __call__(self, func) -> ScheduleWrapper:
        return ScheduleWrapper(func, self)
