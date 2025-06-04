import json
import os
from typing import Any, Callable, Dict, List, Optional, Type, Union

from .. import terminal
from ..abstractions.base.container import Container
from ..abstractions.base.runner import (
    TASKQUEUE_DEPLOYMENT_STUB_TYPE,
    TASKQUEUE_SERVE_STUB_TYPE,
    TASKQUEUE_STUB_TYPE,
    AbstractCallableWrapper,
    RunnerAbstraction,
)
from ..abstractions.image import Image
from ..abstractions.volume import CloudBucket, Volume
from ..channel import with_grpc_error_handling
from ..clients.taskqueue import (
    StartTaskQueueServeRequest,
    StartTaskQueueServeResponse,
    TaskQueuePutRequest,
    TaskQueuePutResponse,
    TaskQueueServiceStub,
)
from ..env import is_local
from ..schema import Schema
from ..type import (
    Autoscaler,
    GpuType,
    GpuTypeAlias,
    PricingPolicy,
    QueueDepthAutoscaler,
    TaskPolicy,
)
from .mixins import DeployableMixin


class TaskQueue(RunnerAbstraction):
    """
    Decorator which allows you to create a task queue out of the decorated function. The tasks are executed
    asynchronously, in remote containers. You can interact with the task queue either through an API (when deployed), or directly
    in python through the .put() method.

    Parameters:
        app (str):
            Assign the task queue to an app. If the app does not exist, it will be created with the given name.
            An app is a group of resources (endpoints, task queues, functions, etc).
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
        on_start (Optional[Callable]):
            An optional function to run once (per process) when the container starts. Can be used for downloading data,
            loading models, or anything else computationally expensive.
        callback_url (Optional[str]):
            An optional URL to send a callback to when a task is completed, timed out, or cancelled.
        volumes (Optional[List[Union[Volume, CloudBucket]]]):
            A list of storage volumes and/or cloud buckets to be associated with the taskqueue. Default is [].
        secrets (Optional[List[str]):
            A list of secrets that are injected into the container as environment variables. Default is [].
        env (Optional[Dict[str, str]]):
            A dictionary of environment variables to be injected into the container. Default is {}.
        name (Optional[str]):
            An optional app name for this task queue. If not specified, it will be the name of the
            working directory containing the python file with the decorated function.
        authorized (bool):
            If false, allows the endpoint to be invoked without an auth token.
            Default is True.
        autoscaler (Autoscaler):
            Configure a deployment autoscaler - if specified, you can use scale your function horizontally using
            various autoscaling strategies. Default is QueueDepthAutoscaler().
        task_policy (TaskPolicy):
            The task policy for the function. This helps manage the lifecycle of an individual task.
            Setting values here will override timeout and retries.
        retry_for (Optional[List[BaseException]]):
            A list of exceptions that will trigger a retry if raised by your handler.
        checkpoint_enabled (bool):
            (experimental) Whether to enable checkpointing for the task queue. Default is False.
            If enabled, the app will be checkpointed after the on_start function has completed.
            On next invocation, each container will restore from a checkpoint and resume execution instead of
            booting up from cold.
        inputs (Optional[Schema]):
            The input schema for the task queue. Default is None.
        outputs (Optional[Schema]):
            The output schema for the task queue. Default is None.
    Example:
        ```python
        from beta9 import task_queue, Image

        @task_queue(cpu=1.0, memory=128, gpu="T4", image=Image(python_packages=["torch"]), keep_warm_seconds=1000)
        def transcribe(filename: str):
            print(filename)
            return

        transcribe.put("some_file.mp4")

        ```
    """

    def __init__(
        self,
        app: str = "",
        cpu: Union[int, float, str] = 1.0,
        memory: Union[int, str] = 128,
        gpu: Union[GpuTypeAlias, List[GpuTypeAlias]] = GpuType.NoGPU,
        gpu_count: int = 0,
        image: Image = Image(),
        timeout: int = 3600,
        retries: int = 3,
        workers: int = 1,
        keep_warm_seconds: int = 10,
        max_pending_tasks: int = 100,
        on_start: Optional[Callable] = None,
        on_deploy: Optional[AbstractCallableWrapper] = None,
        callback_url: Optional[str] = None,
        volumes: Optional[List[Union[Volume, CloudBucket]]] = None,
        secrets: Optional[List[str]] = None,
        env: Optional[Dict[str, str]] = {},
        name: Optional[str] = None,
        authorized: bool = True,
        autoscaler: Autoscaler = QueueDepthAutoscaler(),
        task_policy: TaskPolicy = TaskPolicy(),
        checkpoint_enabled: bool = False,
        retry_for: Optional[List[Type[Exception]]] = None,
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
            workers=workers,
            timeout=timeout,
            retries=retries,
            keep_warm_seconds=keep_warm_seconds,
            max_pending_tasks=max_pending_tasks,
            on_start=on_start,
            on_deploy=on_deploy,
            callback_url=callback_url,
            volumes=volumes,
            secrets=secrets,
            env=env,
            name=name,
            authorized=authorized,
            autoscaler=autoscaler,
            task_policy=task_policy,
            checkpoint_enabled=checkpoint_enabled,
            app=app,
            pricing=pricing,
            inputs=inputs,
            outputs=outputs,
        )
        self._taskqueue_stub: Optional[TaskQueueServiceStub] = None
        self.retry_for = retry_for or []

    @property
    def taskqueue_stub(self) -> TaskQueueServiceStub:
        if not self._taskqueue_stub:
            self._taskqueue_stub = TaskQueueServiceStub(self.channel)
        return self._taskqueue_stub

    @taskqueue_stub.setter
    def taskqueue_stub(self, value: TaskQueueServiceStub) -> None:
        self._taskqueue_stub = value

    def __call__(self, func):
        return _CallableWrapper(func, self)


class _CallableWrapper(DeployableMixin):
    deployment_stub_type = TASKQUEUE_DEPLOYMENT_STUB_TYPE

    def __init__(self, func: Callable, parent: TaskQueue):
        self.func: Callable = func
        self.parent: TaskQueue = parent

    def __call__(self, *args, **kwargs) -> Any:
        if not is_local():
            return self.local(*args, **kwargs)

        raise NotImplementedError(
            "Direct calls to TaskQueues are not yet supported."
            + " To enqueue items use .put(*args, **kwargs)"
        )

    def local(self, *args, **kwargs) -> Any:
        return self.func(*args, **kwargs)

    @with_grpc_error_handling
    def serve(self, timeout: int = 0, url_type: str = ""):
        if not self.parent.prepare_runtime(
            func=self.func, stub_type=TASKQUEUE_SERVE_STUB_TYPE, force_create_stub=True
        ):
            return False

        try:
            with terminal.progress("Serving taskqueue..."):
                self.parent.print_invocation_snippet(url_type=url_type)

                return self._serve(dir=os.getcwd(), timeout=timeout)
        except KeyboardInterrupt:
            terminal.header("Stopping serve container")
            terminal.print("Goodbye 👋")
            os._exit(0)  # kills all threads immediately

    def _serve(self, *, dir: str, timeout: int = 0):
        stream = self.parent.taskqueue_stub.start_task_queue_serve(
            StartTaskQueueServeRequest(
                stub_id=self.parent.stub_id,
                timeout=timeout,
            )
        )

        r: StartTaskQueueServeResponse = stream
        if not r.ok:
            return terminal.error(r.error_msg)

        container = Container(container_id=r.container_id)
        container.attach(container_id=r.container_id, sync_dir=dir)

    def put(self, *args, **kwargs) -> bool:
        if not self.parent.prepare_runtime(
            func=self.func,
            stub_type=TASKQUEUE_STUB_TYPE,
        ):
            return False

        payload = {"args": args, "kwargs": kwargs}
        json_payload = json.dumps(payload)

        r: TaskQueuePutResponse = self.parent.taskqueue_stub.task_queue_put(
            TaskQueuePutRequest(stub_id=self.parent.stub_id, payload=json_payload.encode("utf-8"))
        )

        if not r.ok:
            terminal.error("Failed to enqueue task")
            return False

        terminal.detail(f"Enqueued task: {r.task_id}")
        return True
