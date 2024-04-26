import json
import os
from typing import Any, Callable, Optional, Union

from .. import terminal
from ..abstractions.base.runner import (
    TASKQUEUE_DEPLOYMENT_STUB_TYPE,
    TASKQUEUE_SERVE_STUB_TYPE,
    TASKQUEUE_STUB_TYPE,
    RunnerAbstraction,
)
from ..abstractions.image import Image
from ..clients.gateway import DeployStubRequest, DeployStubResponse
from ..clients.taskqueue import (
    StartTaskQueueServeRequest,
    StopTaskQueueServeRequest,
    TaskQueuePutRequest,
    TaskQueuePutResponse,
    TaskQueueServiceStub,
)
from ..config import GatewayConfig, get_gateway_config
from ..env import is_local


class TaskQueue(RunnerAbstraction):
    """
    Decorator which allows you to create a task queue out of the decorated function. The tasks are executed
    asynchronously, in remote containers. You can interact with the task queue either through an API (when deployed), or directly
    in python through the .put() method.

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
        on_start (Optional[Callable]):
            An optional function to run once (per process) when the container starts. Can be used for downloading data,
            loading models, or anything else computationally expensive.
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
        memory: int = 128,
        gpu: str = "",
        image: Image = Image(),
        timeout: int = 3600,
        retries: int = 3,
        concurrency: int = 1,
        max_containers: int = 1,
        keep_warm_seconds: int = 10,
        max_pending_tasks: int = 100,
        on_start: Optional[Callable] = None,
    ) -> None:
        super().__init__(
            cpu=cpu,
            memory=memory,
            gpu=gpu,
            image=image,
            concurrency=concurrency,
            max_containers=max_containers,
            timeout=timeout,
            retries=retries,
            keep_warm_seconds=keep_warm_seconds,
            max_pending_tasks=max_pending_tasks,
            on_start=on_start,
        )

        self.taskqueue_stub: TaskQueueServiceStub = TaskQueueServiceStub(self.channel)

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
        if not self.parent.prepare_runtime(
            func=self.func, stub_type=TASKQUEUE_DEPLOYMENT_STUB_TYPE, force_create_stub=True
        ):
            return False

        terminal.header("Deploying task queue")
        deploy_response: DeployStubResponse = self.parent.run_sync(
            self.parent.gateway_stub.deploy_stub(
                DeployStubRequest(stub_id=self.parent.stub_id, name=name)
            )
        )

        if deploy_response.ok:
            gateway_config: GatewayConfig = get_gateway_config()
            gateway_url = f"{gateway_config.gateway_host}:{gateway_config.gateway_port}"

            terminal.header("Deployed 🎉")
            terminal.detail(
                f"Call your deployment at: {gateway_url}/api/v1/taskqueue/{name}/v{deploy_response.version}"
            )

        return deploy_response.ok

    def serve(self):
        if not self.parent.prepare_runtime(
            func=self.func, stub_type=TASKQUEUE_SERVE_STUB_TYPE, force_create_stub=True
        ):
            return False

        try:
            with terminal.progress("Serving taskqueue..."):
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
                    self.parent.taskqueue_stub.stop_task_queue_serve(
                        StopTaskQueueServeRequest(stub_id=self.parent.stub_id)
                    )
                )

        terminal.print("Goodbye 👋")

    async def _serve(self, *, dir: str, object_id: str):
        sync_task = self.parent.loop.create_task(
            self.parent.sync_dir_to_workspace(dir=dir, object_id=object_id)
        )
        try:
            async for r in self.parent.taskqueue_stub.start_task_queue_serve(
                StartTaskQueueServeRequest(
                    stub_id=self.parent.stub_id,
                )
            ):
                if r.output != "":
                    terminal.detail(r.output.strip())

                if r.done or r.exit_code != 0:
                    last_response = r
                    break

            if last_response is None or not last_response.done or last_response.exit_code != 0:
                terminal.error("Serve container failed ☠️")
        finally:
            sync_task.cancel()

    def put(self, *args, **kwargs) -> bool:
        if not self.parent.prepare_runtime(
            func=self.func,
            stub_type=TASKQUEUE_STUB_TYPE,
        ):
            return False

        payload = {"args": args, "kwargs": kwargs}
        json_payload = json.dumps(payload)

        r: TaskQueuePutResponse = self.parent.run_sync(
            self.parent.taskqueue_stub.task_queue_put(
                TaskQueuePutRequest(
                    stub_id=self.parent.stub_id, payload=json_payload.encode("utf-8")
                )
            )
        )
        if not r.ok:
            terminal.error("Failed to enqueue task")
            return False

        terminal.detail(f"Enqueued task: {r.task_id}")
        return True
