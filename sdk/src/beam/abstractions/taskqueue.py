import json
import os
from typing import Any, Callable

from beam import terminal
from beam.abstractions.base.runner import (
    TASKQUEUE_DEPLOYMENT_STUB_TYPE,
    TASKQUEUE_STUB_TYPE,
    RunnerAbstraction,
)
from beam.abstractions.image import Image
from beam.clients.gateway import DeployStubResponse
from beam.clients.taskqueue import TaskQueuePutResponse, TaskQueueServiceStub
from beam.config import GatewayConfig, get_gateway_config


class TaskQueue(RunnerAbstraction):
    def __init__(
        self,
        image: Image,
        cpu: int = 100,
        memory: int = 128,
        gpu="",
        timeout: int = 3600,
        retries: int = 3,
        concurrency: int = 1,
        max_pending_tasks: int = 100,
        max_containers: int = 1,
        keep_warm_seconds: int = 10,
    ) -> None:
        super().__init__(
            image=image,
            cpu=cpu,
            memory=memory,
            gpu=gpu,
            concurrency=concurrency,
            max_containers=max_containers,
            max_pending_tasks=max_pending_tasks,
            timeout=timeout,
            retries=retries,
            keep_warm_seconds=keep_warm_seconds,
        )

        self.taskqueue_stub: TaskQueueServiceStub = TaskQueueServiceStub(self.channel)

    def __call__(self, func):
        return _CallableWrapper(func, self)


class _CallableWrapper:
    def __init__(self, func: Callable, parent: TaskQueue):
        self.func: Callable = func
        self.parent: TaskQueue = parent

    def __call__(self, *args, **kwargs) -> Any:
        container_id = os.getenv("CONTAINER_ID")
        if container_id is not None:
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
            self.parent.gateway_stub.deploy_stub(stub_id=self.parent.stub_id, name=name)
        )

        if deploy_response.ok:
            gateway_config: GatewayConfig = get_gateway_config()
            gateway_url = f"{gateway_config.gateway_host}:{gateway_config.gateway_port}"

            terminal.header("Deployed 🎉")
            terminal.detail(
                f"Call your deployment at: {gateway_url}/api/v1/taskqueue/{name}/v{deploy_response.version}"
            )

        return deploy_response.ok

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
                stub_id=self.parent.stub_id, payload=json_payload.encode("utf-8")
            )
        )
        if not r.ok:
            terminal.error("Failed to enqueue task")
            return False

        terminal.detail(f"Enqueued task: {r.task_id}")
        return True
