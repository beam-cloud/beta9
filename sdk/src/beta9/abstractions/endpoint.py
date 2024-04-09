import os
from typing import Any, Callable, Union

from .. import terminal
from ..abstractions.base.runner import (
    ENDPOINT_DEPLOYMENT_STUB_TYPE,
    ENDPOINT_SERVE_STUB_TYPE,
    RunnerAbstraction,
)
from ..abstractions.image import Image
from ..clients.endpoint import EndpointServeRequest, EndpointServiceStub
from ..clients.gateway import DeployStubRequest, DeployStubResponse
from ..config import GatewayConfig, get_gateway_config


class Endpoint(RunnerAbstraction):
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
        keep_warm_seconds: int = 300,
        max_pending_tasks: int = 100,
    ):
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
        )

        self.endpoint_stub: EndpointServiceStub = EndpointServiceStub(self.channel)

    def __call__(self, func):
        return _CallableWrapper(func, self)


class _CallableWrapper:
    def __init__(self, func: Callable, parent: Endpoint):
        self.func: Callable = func
        self.parent: Endpoint = parent

    def __call__(self, *args, **kwargs) -> Any:
        container_id = os.getenv("CONTAINER_ID")
        if container_id is not None:
            return self.local(*args, **kwargs)

        raise NotImplementedError("Direct calls to Endpoints are not supported.")

    def deploy(self, name: str) -> bool:
        if not self.parent.prepare_runtime(
            func=self.func, stub_type=ENDPOINT_DEPLOYMENT_STUB_TYPE, force_create_stub=True
        ):
            return False

        terminal.header("Deploying endpoint")
        deploy_response: DeployStubResponse = self.parent.run_sync(
            self.parent.gateway_stub.deploy_stub(
                DeployStubRequest(stub_id=self.parent.stub_id, name=name)
            )
        )

        if deploy_response.ok:
            gateway_config: GatewayConfig = get_gateway_config()
            gateway_url = f"{gateway_config.gateway_host}:{gateway_config.http_port}"

            terminal.header("Deployed 🎉")
            terminal.detail(
                f"Call your deployment at: {gateway_url}/endpoint/{name}/v{deploy_response.version}"
            )

        return deploy_response.ok

    def serve(self):
        if not self.parent.prepare_runtime(
            func=self.func, stub_type=ENDPOINT_SERVE_STUB_TYPE, force_create_stub=True
        ):
            return False

        with terminal.progress("Serving endpoint..."):
            return self.parent.run_sync(self._serve())

    async def _serve(self):
        async for r in self.parent.endpoint_stub.endpoint_serve(
            EndpointServeRequest(
                stub_id=self.parent.stub_id,
            )
        ):
            print("R:", r)
            if r.output != "":
                terminal.detail(r.output.strip())

            if r.done or r.exit_code != 0:
                last_response = r
                break

        if last_response is None or not last_response.done or last_response.exit_code != 0:
            terminal.error("Serve container failed ☠️")
            return None

        # self.parent.sync_dir_to_workspace(dir=os.getcwd(), object_id=self.parent.object_id)
