import json
import os
import threading
from typing import Any, Callable, List, Optional, Union

from .. import terminal
from ..abstractions.base.runner import (
    TASKQUEUE_DEPLOYMENT_STUB_TYPE,
    TASKQUEUE_SERVE_STUB_TYPE,
    TASKQUEUE_STUB_TYPE,
    RunnerAbstraction,
)
from ..abstractions.image import Image
from ..abstractions.volume import Volume
from ..channel import with_grpc_error_handling
from ..clients.gateway import DeployStubRequest, DeployStubResponse
from ..clients.taskqueue import (
    StartTaskQueueServeRequest,
    StartTaskQueueServeResponse,
    StopTaskQueueServeRequest,
    TaskQueuePutRequest,
    TaskQueuePutResponse,
    TaskQueueServeKeepAliveRequest,
    TaskQueueServiceStub,
)
from ..env import is_local
from ..type import Autoscaler, GpuType, GpuTypeAlias, QueueDepthAutoscaler


class TaskQueue(RunnerAbstraction):
    """
    Decorator which allows you to create a task queue out of the decorated function. The tasks are executed
    asynchronously, in remote containers. You can interact with the task queue either through an API (when deployed), or directly
    in python through the .put() method.

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
        volumes (Optional[List[Volume]]):
            A list of storage volumes to be associated with the taskqueue. Default is [].
        secrets (Optional[List[str]):
            A list of secrets that are injected into the container as environment variables. Default is [].
        name (Optional[str]):
            An optional name for this task_queue, used during deployment. If not specified, you must specify the name
            at deploy time with the --name argument
        autoscaler (Optional[Autoscaler]):
            Configure a deployment autoscaler - if specified, you can use scale your function horizontally using
            various autoscaling strategies (Defaults to QueueDepthAutoscaler())
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
        cpu: Union[int, float, str] = 1.0,
        memory: Union[int, str] = 128,
        gpu: GpuTypeAlias = GpuType.NoGPU,
        image: Image = Image(),
        timeout: int = 3600,
        retries: int = 3,
        workers: int = 1,
        keep_warm_seconds: int = 10,
        max_pending_tasks: int = 100,
        on_start: Optional[Callable] = None,
        callback_url: Optional[str] = None,
        volumes: Optional[List[Volume]] = None,
        secrets: Optional[List[str]] = None,
        name: Optional[str] = None,
        autoscaler: Optional[Autoscaler] = QueueDepthAutoscaler(),
    ) -> None:
        super().__init__(
            cpu=cpu,
            memory=memory,
            gpu=gpu,
            image=image,
            workers=workers,
            timeout=timeout,
            retries=retries,
            keep_warm_seconds=keep_warm_seconds,
            max_pending_tasks=max_pending_tasks,
            on_start=on_start,
            callback_url=callback_url,
            volumes=volumes,
            secrets=secrets,
            name=name,
            autoscaler=autoscaler,
        )
        self._taskqueue_stub: Optional[TaskQueueServiceStub] = None

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


class _CallableWrapper:
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

    def deploy(self, name: str) -> bool:
        name = name or self.parent.name
        if not name or name == "":
            terminal.error(
                "You must specify an app name (either in the decorator or via the --name argument)."
            )

        if not self.parent.prepare_runtime(
            func=self.func, stub_type=TASKQUEUE_DEPLOYMENT_STUB_TYPE, force_create_stub=True
        ):
            return False

        terminal.header("Deploying taskqueue")
        deploy_response: DeployStubResponse = self.parent.gateway_stub.deploy_stub(
            DeployStubRequest(stub_id=self.parent.stub_id, name=name)
        )

        if deploy_response.ok:
            terminal.header("Deployed 🎉")
            self.parent.print_invocation_snippet(deploy_response.invoke_url)

        return deploy_response.ok

    @with_grpc_error_handling
    def serve(self, timeout: int = 0) -> bool:
        if not self.parent.prepare_runtime(
            func=self.func, stub_type=TASKQUEUE_SERVE_STUB_TYPE, force_create_stub=True
        ):
            return False

        try:
            with terminal.progress("Serving taskqueue..."):
                base_url = self.parent.settings.api_host
                if not base_url.startswith(("http://", "https://")):
                    base_url = f"http://{base_url}"

                self.parent.print_invocation_snippet(
                    invocation_url=f"{base_url}/taskqueue/id/{self.parent.stub_id}"
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
            self.parent.taskqueue_stub.stop_task_queue_serve(
                StopTaskQueueServeRequest(stub_id=self.parent.stub_id)
            )

        terminal.print("Goodbye 👋")
        os._exit(0)  # kills all threads immediately

    def _serve(self, *, dir: str, object_id: str, timeout: int = 0):
        def notify(*_, **__):
            self.parent.taskqueue_stub.task_queue_serve_keep_alive(
                TaskQueueServeKeepAliveRequest(
                    stub_id=self.parent.stub_id,
                    timeout=timeout,
                )
            )

        threading.Thread(
            target=self.parent.sync_dir_to_workspace,
            kwargs={"dir": dir, "object_id": object_id, "on_event": notify},
            daemon=True,
        ).start()

        r: Optional[StartTaskQueueServeResponse] = None
        for r in self.parent.taskqueue_stub.start_task_queue_serve(
            StartTaskQueueServeRequest(
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

        terminal.warn("Taskqueue serve timed out. Container has been stopped.")

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
