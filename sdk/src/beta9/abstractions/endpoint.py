import os
import traceback
import types
from typing import Any, Callable, Dict, List, Optional, Union

from uvicorn.protocols.utils import ClientDisconnected

from .. import terminal
from ..abstractions.base.container import Container
from ..abstractions.base.runner import (
    ASGI_DEPLOYMENT_STUB_TYPE,
    ASGI_SERVE_STUB_TYPE,
    ASGI_STUB_TYPE,
    ENDPOINT_DEPLOYMENT_STUB_TYPE,
    ENDPOINT_SERVE_STUB_TYPE,
    ENDPOINT_STUB_TYPE,
    AbstractCallableWrapper,
    RunnerAbstraction,
)
from ..abstractions.image import Image
from ..abstractions.volume import CloudBucket, Volume
from ..channel import with_grpc_error_handling
from ..clients.endpoint import (
    EndpointServiceStub,
    StartEndpointServeRequest,
    StartEndpointServeResponse,
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


class Endpoint(RunnerAbstraction):
    """
    Decorator which allows you to create a web endpoint out of the decorated function.
    Tasks are invoked synchronously as HTTP requests.

    Parameters:
        app (str):
            Assign the endpoint to an app. If the app does not exist, it will be created with the given name.
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
        volumes (Optional[List[Union[Volume, CloudBucket]]]):
            A list of volumes and/or cloud buckets to be mounted to the endpoint. Default is None.
        timeout (Optional[int]):
            The maximum number of seconds a task can run before it times out.
            Default is 180. Set it to -1 to disable the timeout.
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
        env (Optional[Dict[str, str]]):
            A dictionary of environment variables to be injected into the container. Default is {}.
        name (Optional[str]):
            An optional app name for this endpoint. If not specified, it will be the name of the
            working directory containing the python file with the decorated function.
        authorized (bool):
            If false, allows the endpoint to be invoked without an auth token.
            Default is True.
        autoscaler (Optional[Autoscaler]):
            Configure a deployment autoscaler - if specified, you can use scale your function horizontally using
            various autoscaling strategies (Defaults to QueueDepthAutoscaler())
        callback_url (Optional[str]):
            An optional URL to send a callback to when a task is completed, timed out, or cancelled.
        task_policy (TaskPolicy):
            The task policy for the function. This helps manage the lifecycle of an individual task.
            Setting values here will override timeout and retries.
        checkpoint_enabled (bool):
            (experimental) Whether to enable checkpointing for the endpoint. Default is False.
            If enabled, the app will be checkpointed after the on_start function has completed.
            On next invocation, each container will restore from a checkpoint and resume execution instead of
            booting up from cold.
        inputs (Optional[Schema]):
            The input schema for the endpoint. Default is None.
        outputs (Optional[Schema]):
            The output model for the endpoint. Default is None.
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

    concurrent_requests = 1

    def __init__(
        self,
        cpu: Union[int, float, str] = 1.0,
        memory: Union[int, str] = 128,
        app: str = "",
        gpu: Union[GpuTypeAlias, List[GpuTypeAlias]] = GpuType.NoGPU,
        gpu_count: int = 0,
        image: Image = Image(),
        timeout: int = 180,
        workers: int = 1,
        keep_warm_seconds: int = 180,
        max_pending_tasks: int = 100,
        on_start: Optional[Callable] = None,
        on_deploy: Optional[AbstractCallableWrapper] = None,
        volumes: Optional[List[Union[Volume, CloudBucket]]] = None,
        secrets: Optional[List[str]] = None,
        env: Optional[Dict[str, str]] = {},
        name: Optional[str] = None,
        authorized: bool = True,
        autoscaler: Autoscaler = QueueDepthAutoscaler(),
        callback_url: Optional[str] = None,
        task_policy: TaskPolicy = TaskPolicy(),
        checkpoint_enabled: bool = False,
        pricing: Optional[PricingPolicy] = None,
        inputs: Optional[Schema] = None,
        outputs: Optional[Schema] = None,
    ):
        super().__init__(
            cpu=cpu,
            memory=memory,
            gpu=gpu,
            gpu_count=gpu_count,
            image=image,
            workers=workers,
            timeout=timeout,
            retries=0,
            keep_warm_seconds=keep_warm_seconds,
            max_pending_tasks=max_pending_tasks,
            on_start=on_start,
            on_deploy=on_deploy,
            volumes=volumes,
            secrets=secrets,
            env=env,
            name=name,
            authorized=authorized,
            autoscaler=autoscaler,
            callback_url=callback_url,
            task_policy=task_policy,
            concurrent_requests=self.concurrent_requests,
            checkpoint_enabled=checkpoint_enabled,
            app=app,
            pricing=pricing,
            inputs=inputs,
            outputs=outputs,
        )

        self._endpoint_stub: Optional[EndpointServiceStub] = None

    @property
    def endpoint_stub(self) -> EndpointServiceStub:
        if not self._endpoint_stub:
            self._endpoint_stub = EndpointServiceStub(self.channel)
        return self._endpoint_stub

    def __call__(self, func):
        return _CallableWrapper(func, self)


class ASGI(Endpoint):
    """
    Decorator which allows you to create an ASGI application.

    Parameters:
        app (str):
            Assign the ASGI endpoint to an app. If the app does not exist, it will be created with the given name.
            An app is a group of resources (endpoints, task queues, functions, etc).
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
        volumes (Optional[List[Union[Volume, CloudBucket]]]):
            A list of volumes and/or cloud buckets to be mounted to the ASGI application. Default is None.
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
        concurrent_requests (int):
            The maximum number of concurrent requests the ASGI application can handle.
            Unlike regular endpoints that process requests synchronously, ASGI applications
            can handle multiple requests concurrently. This parameter allows you to specify
            the level of concurrency. For applications with blocking operations, this can
            improve throughput by allowing the application to process other requests while
            waiting for blocking operations to complete. Default is 1.
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
        env (Optional[Dict[str, str]]):
            A dictionary of environment variables to be injected into the container. Default is {}.
        name (Optional[str]):
            An optional app name for this ASGI application. If not specified, it will be the name of the
            working directory containing the python file with the decorated function.
        authorized (bool):
            If false, allows the ASGI application to be invoked without an auth token.
            Default is True.
        autoscaler (Optional[Autoscaler]):
            Configure a deployment autoscaler - if specified, you can use scale your function horizontally using
            various autoscaling strategies (Defaults to QueueDepthAutoscaler())
        callback_url (Optional[str]):
            An optional URL to send a callback to when a task is completed, timed out, or cancelled.
        task_policy (TaskPolicy):
            The task policy for the function. This helps manage the lifecycle of an individual task.
            Setting values here will override timeout and retries.
        checkpoint_enabled (bool):
            (experimental) Whether to enable checkpointing for the endpoint. Default is False.
            If enabled, the app will be checkpointed after the on_start function has completed.
            On next invocation, each container will restore from a checkpoint and resume execution instead of
            booting up from cold.
    Example:
        ```python
        from beta9 import asgi, Image

        @asgi(
            cpu=1.0,
            memory=128,
            gpu="T4"
        )
        def web_server(context):
            from fastapi import FastAPI

            app = FastAPI()

            @app.post("/hello")
            async def something():
                return {"hello": True}

            @app.post("/warmup")
            async def warmup():
                return {"status": "warm"}

            return app
        ```
    """

    def __init__(
        self,
        app: str = "",
        cpu: Union[int, float, str] = 1.0,
        memory: Union[int, str] = 128,
        gpu: GpuTypeAlias = GpuType.NoGPU,
        gpu_count: int = 0,
        image: Image = Image(),
        timeout: int = 180,
        workers: int = 1,
        concurrent_requests: int = 1,
        keep_warm_seconds: int = 180,
        max_pending_tasks: int = 100,
        on_start: Optional[Callable] = None,
        on_deploy: Optional[AbstractCallableWrapper] = None,
        volumes: Optional[List[Union[Volume, CloudBucket]]] = None,
        secrets: Optional[List[str]] = None,
        env: Optional[Dict[str, str]] = {},
        name: Optional[str] = None,
        authorized: bool = True,
        autoscaler: Autoscaler = QueueDepthAutoscaler(),
        callback_url: Optional[str] = None,
        checkpoint_enabled: bool = False,
        pricing: Optional[PricingPolicy] = None,
    ):
        self.concurrent_requests = concurrent_requests
        super().__init__(
            cpu=cpu,
            memory=memory,
            gpu=gpu,
            gpu_count=gpu_count,
            image=image,
            timeout=timeout,
            workers=workers,
            keep_warm_seconds=keep_warm_seconds,
            max_pending_tasks=max_pending_tasks,
            on_start=on_start,
            on_deploy=on_deploy,
            volumes=volumes,
            secrets=secrets,
            env=env,
            name=name,
            authorized=authorized,
            autoscaler=autoscaler,
            callback_url=callback_url,
            checkpoint_enabled=checkpoint_enabled,
            app=app,
            pricing=pricing,
        )

        self.is_asgi = True


REALTIME_ASGI_SLEEP_INTERVAL_SECONDS = 0.1
REALTIME_ASGI_HEALTH_CHECK_INTERVAL_SECONDS = 5


class RealtimeASGI(ASGI):
    """
    Decorator which allows you to create a realtime application built on top of ASGI / websockets.
    Your handler function will run every time a message is received over the websocket.

    Parameters:
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
        volumes (Optional[List[Union[Volume, CloudBucket]]]):
            A list of volumes and/or cloud buckets to be mounted to the ASGI application. Default is None.
        timeout (Optional[int]):
            The maximum number of seconds a task can run before it times out.
            Default is 3600. Set it to -1 to disable the timeout.
        workers (Optional[int]):
            The number of processes handling tasks per container.
            Modifying this parameter can improve throughput for certain workloads.
            Workers will share the CPU, Memory, and GPU defined.
            You may need to increase these values to increase concurrency.
            Default is 1.
        concurrent_requests (int):
            The maximum number of concurrent requests the ASGI application can handle.
            Unlike regular endpoints that process requests synchronously, ASGI applications
            can handle multiple requests concurrently. This parameter allows you to specify
            the level of concurrency. For applications with blocking operations, this can
            improve throughput by allowing the application to process other requests while
            waiting for blocking operations to complete. Default is 1.
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
        env (Optional[Dict[str, str]]):
            A dictionary of environment variables to be injected into the container. Default is {}.
        name (Optional[str]):
            An optional app name for this ASGI application. If not specified, it will be the name of the
            working directory containing the python file with the decorated function.
        authorized (bool):
            If false, allows the ASGI application to be invoked without an auth token.
            Default is True.
        autoscaler (Optional[Autoscaler]):
            Configure a deployment autoscaler - if specified, you can use scale your function horizontally using
            various autoscaling strategies (Defaults to QueueDepthAutoscaler())
        callback_url (Optional[str]):
            An optional URL to send a callback to when a task is completed, timed out, or cancelled.
        checkpoint_enabled (bool):
            (experimental) Whether to enable checkpointing for the endpoint. Default is False.
            If enabled, the app will be checkpointed after the on_start function has completed.
            On next invocation, each container will restore from a checkpoint and resume execution instead of
            booting up from cold.
    Example:
        ```python
        from beta9 import realtime

        def generate_text():
            return ["this", "could", "be", "anything"]

        @realtime(
            cpu=1.0,
            memory=128,
            gpu="T4"
        )
        def handler(context):
            return generate_text()
        ```
    """

    def __init__(
        self,
        cpu: Union[int, float, str] = 1.0,
        memory: Union[int, str] = 128,
        gpu: GpuTypeAlias = GpuType.NoGPU,
        gpu_count: int = 0,
        image: Image = Image(),
        timeout: int = 180,
        workers: int = 1,
        concurrent_requests: int = 1,
        keep_warm_seconds: int = 180,
        max_pending_tasks: int = 100,
        on_start: Optional[Callable] = None,
        on_deploy: Optional[AbstractCallableWrapper] = None,
        volumes: Optional[List[Union[Volume, CloudBucket]]] = None,
        secrets: Optional[List[str]] = None,
        env: Optional[Dict[str, str]] = {},
        name: Optional[str] = None,
        authorized: bool = True,
        autoscaler: Autoscaler = QueueDepthAutoscaler(),
        callback_url: Optional[str] = None,
        checkpoint_enabled: bool = False,
        pricing: Optional[PricingPolicy] = None,
    ):
        super().__init__(
            cpu=cpu,
            memory=memory,
            gpu=gpu,
            gpu_count=gpu_count,
            image=image,
            timeout=timeout,
            workers=workers,
            keep_warm_seconds=keep_warm_seconds,
            max_pending_tasks=max_pending_tasks,
            on_start=on_start,
            on_deploy=on_deploy,
            volumes=volumes,
            secrets=secrets,
            env=env,
            name=name,
            authorized=authorized,
            autoscaler=autoscaler,
            callback_url=callback_url,
            concurrent_requests=concurrent_requests,
            checkpoint_enabled=checkpoint_enabled,
            pricing=pricing,
        )
        self.is_websocket = True

    def __call__(self, func):
        import asyncio
        from queue import Queue

        from fastapi import FastAPI, WebSocket, WebSocketDisconnect, WebSocketException

        internal_asgi_app = FastAPI()
        internal_asgi_app.input_queue = Queue()

        @internal_asgi_app.websocket("/")
        async def stream(websocket: WebSocket):
            async def _heartbeat():
                while True:
                    try:
                        await websocket.send_json({"type": "ping"})
                        await asyncio.sleep(REALTIME_ASGI_HEALTH_CHECK_INTERVAL_SECONDS)
                    except (WebSocketDisconnect, WebSocketException, RuntimeError):
                        break

            await websocket.accept()

            heartbeat_task = asyncio.create_task(_heartbeat())
            try:
                while True:
                    try:
                        message = await websocket.receive()

                        if "text" in message:
                            data = message["text"]
                        elif "bytes" in message:
                            data = message["bytes"]
                        elif "json" in message:
                            data = message["json"]
                        elif message.get("type") == "websocket.disconnect":
                            return
                        else:
                            print(f"WS stream error - unknown message type: {message}")
                            continue

                        internal_asgi_app.input_queue.put(data)

                        async def _handle_output(output):
                            if isinstance(output, str):
                                await websocket.send_text(output)
                            elif isinstance(output, dict) or isinstance(output, list):
                                await websocket.send_json(output)
                            else:
                                await websocket.send(output)

                        while not internal_asgi_app.input_queue.empty():
                            output = internal_asgi_app.handler(
                                context=internal_asgi_app.context,
                                event=internal_asgi_app.input_queue.get(),
                            )

                            if isinstance(output, types.GeneratorType):
                                for o in output:
                                    await _handle_output(o)
                            else:
                                await _handle_output(output)

                        await asyncio.sleep(REALTIME_ASGI_SLEEP_INTERVAL_SECONDS)
                    except (
                        WebSocketDisconnect,
                        WebSocketException,
                        RuntimeError,
                        ClientDisconnected,
                    ):
                        return
                    except BaseException:
                        print(f"Unhandled exception in websocket stream: {traceback.format_exc()}")
            finally:
                heartbeat_task.cancel()

        func.internal_asgi_app = internal_asgi_app
        return _CallableWrapper(func, self)


class _CallableWrapper(DeployableMixin):
    deployment_stub_type = ENDPOINT_DEPLOYMENT_STUB_TYPE
    base_stub_type = ENDPOINT_STUB_TYPE

    def __init__(self, func: Callable, parent: Union[Endpoint, ASGI]):
        self.func: Callable = func
        self.parent: Union[Endpoint, ASGI] = parent

        if getattr(self.parent, "is_asgi", None):
            self.deployment_stub_type = ASGI_DEPLOYMENT_STUB_TYPE
            self.base_stub_type = ASGI_STUB_TYPE

    def __call__(self, *args, **kwargs) -> Any:
        if not is_local():
            return self.local(*args, **kwargs)

        raise NotImplementedError("Direct calls to Endpoints are not supported.")

    def local(self, *args, **kwargs) -> Any:
        return self.func(*args, **kwargs)

    @with_grpc_error_handling
    def serve(self, timeout: int = 0, url_type: str = ""):
        stub_type = ENDPOINT_SERVE_STUB_TYPE

        if getattr(self.parent, "is_asgi", None):
            stub_type = ASGI_SERVE_STUB_TYPE

        if not self.parent.prepare_runtime(
            func=self.func, stub_type=stub_type, force_create_stub=True
        ):
            return False

        try:
            with terminal.progress("Serving endpoint..."):
                self.parent.print_invocation_snippet(url_type=url_type)

                return self._serve(dir=os.getcwd(), timeout=timeout)
        except KeyboardInterrupt:
            terminal.header("Stopping serve container")
            terminal.print("Goodbye 👋")
            os._exit(0)  # kills all threads immediately

    def _serve(self, *, dir: str, timeout: int = 0):
        r: StartEndpointServeResponse = self.parent.endpoint_stub.start_endpoint_serve(
            StartEndpointServeRequest(
                stub_id=self.parent.stub_id,
                timeout=timeout,
            )
        )
        if not r.ok:
            return terminal.error(r.error_msg)

        container = Container(container_id=r.container_id)
        container.attach(container_id=r.container_id, sync_dir=dir)
