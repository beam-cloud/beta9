import asyncio
import concurrent.futures
import inspect
import os
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple, Union

import cloudpickle
import grpc

from .. import terminal
from ..abstractions.base import capacity
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
from ..channel import (
    RECOVERY_TIMEOUT,
    _RecoveryWindow,
    request_metadata,
    rpc_timeout,
    transient_error,
    with_grpc_error_handling,
)
from ..clients.function import (
    FunctionInvokeRequest,
    FunctionInvokeResponse,
    FunctionScheduleRequest,
    FunctionServiceStub,
)
from ..clients.gateway import ListTasksRequest, StopTasksRequest, StringList
from ..env import called_on_import, is_local
from ..exceptions import NoCapacityError, RemoteExecutionError
from ..pending import PendingStatus, PendingTally, Wait
from ..schema import Schema
from ..sync import FileSyncer
from ..type import CpuTier, DurableDisk, GpuType, GpuTypeAlias, Pool, TaskPolicy
from .mixins import DeployableMixin


class _Reserved(Exception):
    """A call that waited was given up for one on a machine its caller reserved."""


class _Invocation:
    def __init__(self, stub, request):
        self.stub = stub
        self.request = request
        self.task_id = ""
        self.output_offset = 0
        self.container_id = ""
        self.resumable = False

    def __iter__(self):
        recovery = _RecoveryWindow(start=False)
        while True:
            try:
                responses = iter(self.stub.function_invoke(self.request))
                while True:
                    token = request_metadata.set(
                        (
                            ("x-beta9-task-id", self.task_id),
                            ("x-beta9-log-offset", str(self.output_offset)),
                            ("x-beta9-container-id", self.container_id),
                        )
                    )
                    try:
                        response = next(responses, None)
                    finally:
                        request_metadata.reset(token)
                    if response is None:
                        break
                    # The registration response advertises support for reattaching.
                    if response.task_id and not response.output and not response.done:
                        self.resumable = True
                    self.task_id = response.task_id or self.task_id
                    if response.container_id and response.container_id != self.container_id:
                        self.container_id = response.container_id
                        self.output_offset = 0
                    if response.output:
                        self.output_offset += len(response.output.encode("utf-8"))
                    recovery.reset()
                    yield response
                    if response.done:
                        return
                raise ConnectionError("Function stream disconnected")
            except Exception as error:
                if (
                    not self.resumable
                    or not self.task_id
                    or not transient_error(error)
                    or not recovery.wait()
                ):
                    raise


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

        @function(cpu=1.0, memory=128, gpu="T4", image=Image(python_packages=["torch"]))
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
        disks: Optional[List[DurableDisk]] = None,
        secrets: Optional[List[str]] = None,
        env: Optional[Dict[str, str]] = {},
        name: Optional[str] = None,
        task_policy: TaskPolicy = TaskPolicy(),
        on_deploy: Optional[AbstractCallableWrapper] = None,
        headless: bool = False,
        inputs: Optional[Schema] = None,
        outputs: Optional[Schema] = None,
        pool: Optional[Union[str, Pool]] = None,
        cpu_tier: Optional[CpuTier] = None,
    ) -> None:
        super().__init__(
            cpu=cpu,
            cpu_tier=cpu_tier,
            memory=memory,
            gpu=gpu,
            gpu_count=gpu_count,
            image=image,
            timeout=timeout,
            retries=retries,
            callback_url=callback_url,
            volumes=volumes,
            disks=disks,
            secrets=secrets,
            env=env,
            name=name,
            task_policy=task_policy,
            on_deploy=on_deploy,
            app=app,
            inputs=inputs,
            outputs=outputs,
            pool=pool,
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
        self._invocations = set()
        self._invocations_lock = threading.Lock()

    @with_grpc_error_handling
    def __call__(self, *args, **kwargs) -> Any:
        if called_on_import():
            return

        if not is_local():
            return self.local(*args, **kwargs)

        try:
            self._prepare_runtime()
        except KeyboardInterrupt:
            terminal.error("Exiting shell. Your build was stopped.")

        try:
            title = f"Running {self.parent.name or self.parent.handler}"
            with terminal.StepTracker().step(title, "Function complete"):
                return self._invoke(args, kwargs, PendingStatus(title), self._wait(), ask=True)
        except KeyboardInterrupt:
            if self.parent.headless:
                terminal.error("Exiting shell. Your function will continue running remotely.")
            else:
                terminal.error("Exiting shell. Your function will be terminated.")
        except NoCapacityError as error:
            # Reported here, so that a script that does not catch it ends
            # with the reason. One that does can run the call another way.
            terminal.error(error.message, hint=error.hint or None, code="CAPACITY", exit=False)
            terminal.reset_terminal()
            raise

    @with_grpc_error_handling
    def _prepare_runtime(self) -> None:
        if self.parent.prepare_runtime(func=self.func, stub_type=self.base_stub_type):
            return
        if called_on_import():
            # Nothing is prepared, or reported, while user code is being imported.
            raise RuntimeError("Unable to prepare function runtime")
        # prepare_runtime already reported the reason; a traceback would bury it.
        raise SystemExit(1)

    def _wait(self) -> Wait:
        """How long a call may go without a machine, and when to say what can be done about it."""
        return Wait(limit=capacity.wait_limit(), ask_after=capacity.ASK_AFTER_S)

    def _invoke(self, args: tuple, kwargs: dict, pending, wait: Wait, ask: bool = False) -> Any:
        """
        Run the function remotely. pending is told what the call waits for
        until it runs, and wait keeps how long that has been. With ask, a
        caller at a terminal is asked what to do about a call there is no
        capacity for.

        Raises NoCapacityError for a call that goes without a machine for
        longer than wait allows.
        """
        args = cloudpickle.dumps(
            {
                "args": args,
                "kwargs": kwargs,
            },
        )
        while True:
            try:
                return self._invoke_once(args, pending, wait, ask)
            except _Reserved:
                # The function now runs on the machine that was reserved.
                wait.placed()

    def _invoke_once(self, args: bytes, pending, wait: Wait, ask: bool) -> Any:
        terminal.debug(f"Running function: {self.parent.handler}")
        output = deque()
        output_size = 0

        request = FunctionInvokeRequest(
            stub_id=self.parent.stub_id,
            args=args,
            headless=self.parent.headless,
        )
        invocation = _Invocation(self.parent.function_stub, request)
        responses = iter(invocation)
        with self._invocations_lock:
            self._invocations.add(invocation)

        def stop() -> None:
            if invocation.task_id:
                with rpc_timeout(RECOVERY_TIMEOUT):
                    self.parent.gateway_stub.stop_tasks(
                        StopTasksRequest(task_ids=[invocation.task_id])
                    )

        def started() -> bool:
            return self._task_status(invocation.task_id) in ("RUNNING", "COMPLETE")

        def waiting() -> None:
            self._while_waiting(wait, pending, ask, stop, started)

        try:
            return self._consume_invocation(responses, output, output_size, pending, wait, waiting)
        except KeyboardInterrupt:
            if not self.parent.headless:
                stop()
            raise
        finally:
            pending.clear()
            with self._invocations_lock:
                self._invocations.discard(invocation)

    def _while_waiting(
        self,
        wait: Wait,
        pending,
        ask: bool,
        stop: Callable[[], None],
        started: Callable[[], bool],
    ) -> None:
        """
        What a call that has no machine yet does, each time the gateway says
        what it waits for: gives up when it has waited as long as it may,
        and says once what can be done about capacity that is not there. At
        a terminal that is a question, and the answer may be to reserve a
        machine and run the call on it.

        stop gives up the call, and started says whether it runs after all.
        """
        if wait.over_limit():
            # The price of a machine is looked up for one call, not for each of a batch.
            error = capacity.gave_up(self.parent, wait, price=ask)
            # A call that is given up on must not run later.
            stop()
            raise error
        if not wait.ask():
            return
        if ask:
            # Whether there is somebody to ask is known only with the
            # spinner off the screen: while it is on, it stands in for the
            # terminal's output.
            with terminal.progress_paused():
                if capacity._runner_interactive(self.parent):
                    self._ask_while_waiting(wait, stop, started)
                    return
        pending.advise(lambda: capacity.still_waiting(self.parent, wait))

    def _ask_while_waiting(
        self, wait: Wait, stop: Callable[[], None], started: Callable[[], bool]
    ) -> None:
        """Ask the caller what to do about a call there is no capacity for, and do it."""
        choice = capacity.ask_while_waiting(self.parent, wait)
        if choice == capacity.CANCEL:
            stop()
            terminal.detail("Cancelled. The function did not run.")
            raise SystemExit(1)
        if choice != capacity.RESERVE:
            return
        # The question may have stood for a while.
        if started():
            terminal.detail("A machine took the call while you were choosing.")
            return

        reserved = capacity.reserve_while_waiting(self.parent, stop)
        if reserved:
            raise _Reserved()
        if reserved is False:
            # A machine was reserved and is not ready. Why was said.
            raise SystemExit(1)
        terminal.detail("No machine was reserved. Still waiting for capacity.")

    def _task_status(self, task_id: str, default: str = "") -> str:
        """The status of a task, or default if it cannot be found out now."""
        try:
            with rpc_timeout(5):
                tasks = self.parent.gateway_stub.list_tasks(
                    ListTasksRequest(filters={"id": StringList(values=[task_id])}, limit=1)
                )
            if tasks.ok and tasks.tasks:
                return tasks.tasks[0].status
        except grpc.RpcError:
            pass
        return default

    def _consume_invocation(self, responses, output, output_size, pending, wait, waiting):
        last_response: Optional[FunctionInvokeResponse] = None
        for r in responses:
            # Until its container runs, the gateway says what the call waits for.
            pending.show(r.pending)
            if r.pending.phase:
                wait.told(r.pending)
                waiting()

            if r.output != "":
                pending.clear()
                wait.placed()
                output.append(r.output[-65536:])
                output_size += len(output[-1])
                while output_size - len(output[0]) >= 65536:
                    output_size -= len(output.popleft())
                terminal.detail(r.output, end="")

            if r.done or r.exit_code != 0:
                last_response = r
                break

        if last_response is None:
            raise RuntimeError("Function invocation ended without a task result")

        if not last_response.done or last_response.exit_code != 0 or not last_response.result:
            task_id = last_response.task_id
            # The execution failure stands if its status cannot be found out.
            status = self._task_status(task_id, default="failed")
            raise RemoteExecutionError(
                task_id, status, last_response.exit_code, "".join(output)[-65536:]
            )

        terminal.debug(f"Function complete: {last_response.task_id}")
        return cloudpickle.loads(last_response.result)

    def local(self, *args, **kwargs) -> Any:
        return self.func(*args, **kwargs)

    def remote(self, *args, **kwargs) -> Any:
        return self(*args, **kwargs)

    async def async_remote(self, *args, **kwargs) -> Any:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: self.remote(*args, **kwargs))

    def serve(self, **kwargs):
        terminal.error("Serve has not been implemented for functions.")

    def _format_args(self, args):
        if isinstance(args, tuple):
            return list(args)
        elif not isinstance(args, list):
            return [args]
        return args

    def _threaded_map(self, inputs: Sequence[Any]) -> Iterator[Any]:
        started = time.monotonic()
        failed = 0
        terminal.header(f"{self.parent.name or self.parent.handler}", f"{len(inputs)} calls")
        with terminal.progress(f"Completed 0/{len(inputs)} calls"):
            # The spinner also counts the calls that wait for a machine.
            waiting = PendingTally(f"Completed 0/{len(inputs)} calls")
            # How long each may wait is read once, here, where a setting
            # that cannot be read is reported once.
            limit = capacity.wait_limit()
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(32, len(inputs))) as pool:
                futures = {
                    pool.submit(
                        self._invoke,
                        tuple(self._format_args(args)),
                        {},
                        waiting.status(),
                        Wait(limit=limit, ask_after=capacity.ASK_AFTER_S),
                    ): index
                    for index, args in enumerate(inputs, 1)
                }
                try:
                    for completed, future in enumerate(concurrent.futures.as_completed(futures), 1):
                        try:
                            result = future.result()
                        except (Exception, NoCapacityError) as error:
                            failed += 1
                            message = (
                                error.details() if isinstance(error, grpc.RpcError) else str(error)
                            )
                            terminal.error(f"Input {futures[future]} failed: {message}", exit=False)
                            result = None
                        waiting.retitle(f"Completed {completed}/{len(inputs)} calls")
                        yield result
                except KeyboardInterrupt:
                    pool.shutdown(wait=False, cancel_futures=True)
                    if not self.parent.headless:
                        with self._invocations_lock:
                            ids = [
                                request.task_id for request in self._invocations if request.task_id
                            ]
                        if ids:
                            with rpc_timeout(RECOVERY_TIMEOUT):
                                self.parent.gateway_stub.stop_tasks(StopTasksRequest(task_ids=ids))
                    terminal.error(
                        f"Exiting shell. Mapped functions will {'be terminated.' if not self.parent.headless else 'continue running.'}",
                        exit=False,
                    )
                    os._exit(1)
        if failed:
            terminal.warn(f"Completed {len(inputs)} calls; {failed} failed")
            return
        terminal.done(f"Completed {len(inputs)} calls", started)

    def map(self, inputs: Sequence[Any]) -> Iterator[Any]:
        """Map positional arguments; wrap a single list argument in a tuple: ``([1, 2],)``.

        Results arrive in completion order. Failed inputs are reported and yield ``None``.
        """
        if not inputs:
            return
        self._prepare_runtime()
        yield from self._threaded_map(inputs)


class ScheduleWrapper(_CallableWrapper):
    base_stub_type = SCHEDULE_STUB_TYPE
    deployment_stub_type = SCHEDULE_DEPLOYMENT_STUB_TYPE

    def deploy(self, *args: Any, **kwargs: Any) -> Tuple[Dict[str, Any], bool]:
        resp, ok = super().deploy(invocation_details_func=self.invocation_details, *args, **kwargs)
        if not ok:
            return {}, False

        res = self.parent.function_stub.function_schedule(
            FunctionScheduleRequest(
                stub_id=self.parent.stub_id,
                when=self.parent.when,
                deployment_id=resp["deployment_id"],
            )
        )
        if not res.ok:
            terminal.error(res.err_msg, exit=False)
            return {}, False

        return {
            "deployment_id": resp["deployment_id"],
            "scheduled_job_id": res.scheduled_job_id,
        }, True

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
        disks: Optional[List[DurableDisk]] = None,
        secrets: Optional[List[str]] = None,
        env: Optional[Dict[str, str]] = {},
        name: Optional[str] = None,
        on_deploy: Optional[AbstractCallableWrapper] = None,
        cpu_tier: Optional[CpuTier] = None,
    ) -> None:
        params = inspect.signature(Function.__init__).parameters
        kwargs = {k: v for k, v in locals().items() if k in params and k != "self"}
        super().__init__(**kwargs)

        self.when = when

    def __call__(self, func) -> ScheduleWrapper:
        return ScheduleWrapper(func, self)
