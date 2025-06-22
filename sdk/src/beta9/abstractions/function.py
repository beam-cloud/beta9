import concurrent.futures
import inspect
import os
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Union

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
from ..abstractions.volume import CloudBucket, Volume
from ..channel import with_grpc_error_handling
from ..clients.function import (
    FunctionInvokeRequest,
    FunctionInvokeResponse,
    FunctionScheduleRequest,
    FunctionServiceStub,
)
from ..env import called_on_import, is_local
from ..schema import Schema
from ..sync import FileSyncer
from ..type import GpuType, GpuTypeAlias, PricingPolicy, TaskPolicy
from .mixins import DeployableMixin


class Function(RunnerAbstraction):
    """
    Decorator which allows you to run the decorated function in a remote container.

    Parameters:
        app (str):
            Assign the function to an app. If the app does not exist, it will be created with the given name.
            An app is a group of resources (endpoints, task queues, functions, etc).
        cpu (Union[int, float, str]):
            The number of CPU cores allocated to the container. Default is 0.125.
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
        volumes (Optional[List[Union[Volume, CloudBucket]]]):
            A list of storage volumes and/or cloud buckets to be associated with the function. Default is [].
        secrets (Optional[List[str]):
            A list of secrets that are injected into the container as environment variables. Default is [].
        env (Optional[Dict[str, str]]):
            A dictionary of environment variables to be injected into the container. Default is {}.
        name (Optional[str]):
            An optional app name for this function. If not specified, it will be the name of the
            working directory containing the python file with the decorated function.
        task_policy (TaskPolicy):
            The task policy for the function. This helps manage the lifecycle of an individual task.
            Setting values here will override timeout and retries.
        headless (bool):
            Determines whether the function continues running in the background after the client disconnects. Default: False.
        inputs (Optional[Schema]):
            The input schema for the function. Default is None.
        outputs (Optional[Schema]):
            The output model for the function. Default is None.
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
        app: str = "",
        cpu: Union[int, float, str] = 0.125,
        memory: Union[int, str] = 128,
        gpu: Union[GpuTypeAlias, List[GpuTypeAlias]] = GpuType.NoGPU,
        gpu_count: int = 0,
        image: Image = Image(),
        timeout: int = 3600,
        retries: int = 3,
        callback_url: Optional[str] = None,
        volumes: Optional[List[Union[Volume, CloudBucket]]] = None,
        secrets: Optional[List[str]] = None,
        env: Optional[Dict[str, str]] = {},
        name: Optional[str] = None,
        task_policy: TaskPolicy = TaskPolicy(),
        on_deploy: Optional[AbstractCallableWrapper] = None,
        headless: bool = False,
        pricing: Optional[PricingPolicy] = None,
        inputs: Optional[Schema] = None,
        outputs: Optional[Schema] = None,
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
            env=env,
            name=name,
            task_policy=task_policy,
            on_deploy=on_deploy,
            app=app,
            pricing=pricing,
            inputs=inputs,
            outputs=outputs,
        )

        self._function_stub: Optional[FunctionServiceStub] = None
        self.syncer: FileSyncer = FileSyncer(self.gateway_stub)
        self.headless = headless

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

        try:
            if not self.parent.prepare_runtime(
                func=self.func,
                stub_type=self.base_stub_type,
            ):
                return
        except KeyboardInterrupt:
            terminal.error("Exiting shell. Your build was stopped.")

        try:
            with terminal.progress("Working..."):
                return self._call_remote(*args, **kwargs)
        except KeyboardInterrupt:
            if self.parent.headless:
                terminal.error("Exiting shell. Your function will continue running remotely.")
            else:
                terminal.error("Exiting shell. Your function will be terminated.")

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
                headless=self.parent.headless,
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
        # Sometimes the result is empty (task timed out)
        if not last_response.result:
            return None
        return cloudpickle.loads(last_response.result)

    def local(self, *args, **kwargs) -> Any:
        return self.func(*args, **kwargs)

    def remote(self, *args, **kwargs) -> Any:
        return self(*args, **kwargs)

    def serve(self, **kwargs):
        terminal.error("Serve has not been implemented for functions.")

    def _format_args(self, args):
        if isinstance(args, tuple):
            return list(args)
        elif not isinstance(args, list):
            return [args]
        return args

    def _threaded_map(self, inputs: Sequence[Any]) -> Iterator[Any]:
        with terminal.progress(f"Running {len(inputs)} container(s)..."):
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(inputs)) as pool:
                futures = [
                    pool.submit(self._call_remote, *self._format_args(args)) for args in inputs
                ]
                try:
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            yield future.result()
                        except Exception as e:
                            terminal.error(f"Task failed during map: {e}", exit=False)
                            yield None
                except KeyboardInterrupt:
                    pool.shutdown(wait=False, cancel_futures=True)
                    terminal.error(
                        f"Exiting shell. Mapped functions will {'be terminated.' if not self.parent.headless else 'continue running.'}",
                        exit=False,
                    )
                    os._exit(1)

    def map(self, inputs: Sequence[Any]) -> Iterator[Any]:
        if not self.parent.prepare_runtime(
            func=self.func,
            stub_type=self.base_stub_type,
        ):
            terminal.error("Function failed to prepare runtime ❌")

        iterator = self._threaded_map(inputs)
        yield from iterator


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

        local_tz = datetime.now().astimezone().tzinfo

        cron = croniter(self.parent.when, datetime.now(local_tz))
        cron_utc = croniter(self.parent.when, datetime.now(timezone.utc))
        for i in range(3):
            next_run = cron.get_next(datetime)
            next_run_utc = cron_utc.get_next(datetime)
            terminal.print(
                (
                    f"  [bright_white]{i + 1}.[/bright_white] {next_run_utc:%Y-%m-%d %H:%M:%S %Z} "
                    f"({next_run:%Y-%m-%d %H:%M:%S} {local_tz})"
                )
            )


class Schedule(Function):
    """
    Decorator which allows you to run the decorated function as a scheduled job.

    Parameters:
        app (str):
            Assign the scheduled function to an app. If the app does not exist, it will be created with the given name.
            An app is a group of resources (endpoints, task queues, functions, etc).
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
        volumes (Optional[List[Union[Volume, CloudBucket]]]):
            A list of storage volumes and/or cloud buckets to be associated with the function. Default is [].
        secrets (Optional[List[str]):
            A list of secrets that are injected into the container as environment variables. Default is [].
        env (Optional[Dict[str, str]]):
            A dictionary of environment variables to be injected into the container. Default is {}.
        name (Optional[str]):
            An optional app name for this function. If not specified, it will be the name of the
            working directory containing the python file with the decorated function.

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
        app: str = "",
        cpu: Union[int, float, str] = 1.0,
        memory: Union[int, str] = 128,
        gpu: GpuTypeAlias = GpuType.NoGPU,
        gpu_count: int = 0,
        image: Image = Image(),
        timeout: int = 3600,
        retries: int = 3,
        callback_url: Optional[str] = None,
        volumes: Optional[List[Union[Volume, CloudBucket]]] = None,
        secrets: Optional[List[str]] = None,
        env: Optional[Dict[str, str]] = {},
        name: Optional[str] = None,
        on_deploy: Optional[AbstractCallableWrapper] = None,
    ) -> None:
        params = inspect.signature(Function.__init__).parameters
        kwargs = {k: v for k, v in locals().items() if k in params and k != "self"}
        super().__init__(**kwargs)

        self.when = when

    def __call__(self, func) -> ScheduleWrapper:
        return ScheduleWrapper(func, self)
