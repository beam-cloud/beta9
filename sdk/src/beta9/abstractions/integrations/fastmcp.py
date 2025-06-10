import os
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Union

from ... import terminal
from ...abstractions.base.container import Container
from ...abstractions.base.runner import ASGI_DEPLOYMENT_STUB_TYPE, ASGI_SERVE_STUB_TYPE
from ...abstractions.endpoint import ASGI
from ...abstractions.image import Image
from ...abstractions.volume import CloudBucket, Volume
from ...channel import with_grpc_error_handling
from ...clients.endpoint import StartEndpointServeRequest, StartEndpointServeResponse
from ...clients.gateway import DeployStubRequest, DeployStubResponse
from ...config import ConfigContext
from ...type import Autoscaler, GpuType, GpuTypeAlias, PricingPolicy, QueueDepthAutoscaler


@dataclass
class MCPServerArgs:
    """
    Configuration for the underlying FastMCP server.
    """

    fastmcp_version: str = "2.7.1"


class MCPServer(ASGI):
    """
    FastMCP is a wrapper around the FastMCP library that allows you to deploy it as an ASGI app.

    Parameters:
        server (FastMCP):
            The FastMCP class instance to serve/deploy.
        args (Optional[MCPServerArgs]):
            The arguments for the underlying FastMCP server. If not specified, the default arguments will be used.
        cpu (Union[int, float, str]):
            The number of CPU cores allocated to the container. Default is 1.0.
        memory (Union[int, str]):
            The amount of memory allocated to the container. It should be specified in
            MiB, or as a string with units (e.g. "1Gi"). Default is 128 MiB.
        gpu (Union[GpuType, str]):
            The type or name of the GPU device to be used for GPU-accelerated tasks. If not
            applicable or no GPU required, leave it empty. Default is [GpuType.NoGPU](#gputype).
        image (Union[Image, dict]):
            The container image used for the task execution. Whatever you pass here will have an additional `add_python_packages` call
            with `["fastapi", "vllm", "huggingface_hub"]` added to it to ensure that we can run vLLM in the container.
        workers (int):
            The number of workers to run in the container. Default is 1.
        concurrent_requests (int):
            The maximum number of concurrent requests to handle. Default is 10.
        keep_warm_seconds (int):
            The number of seconds to keep the container warm after the last request. Default is 60.
        max_pending_tasks (int):
            The maximum number of pending tasks to allow in the container. Default is 100.
        timeout (int):
            The maximum number of seconds to wait for the container to start. Default is 3600.
        authorized (bool):
            Whether the endpoints require authorization. Default is False.
        name (str):
            The name of the MCP server app. Default is none, which means you must provide it during deployment.
        volumes (List[Union[Volume, CloudBucket]]):
            The volumes and/or cloud buckets to mount into the MCP server container. Default is an empty list.
        secrets (List[str]):
            The secrets to pass to the MCP server container.
        autoscaler (Autoscaler):
            The autoscaler to use. Default is a queue depth autoscaler.
        on_start (Callable[..., None]):
            A function to call when the MCP server app is started. Default is None.

    Example:
        ```python
        from beta9 import integrations, MCPServerArgs, MCPServer

        args = MCPServerArgs(fastmcp_version="2.7.1")
        fastmcp_app = MCPServer(name="fastmcp-abstraction-1", fastmcp_args=args)
        ```
    """

    def __init__(
        self,
        server: "FastMCP",  # type: ignore  # noqa: F821
        args: MCPServerArgs = MCPServerArgs(),
        cpu: Union[int, float, str] = 1.0,
        memory: Union[int, str] = 128,
        gpu: Union[GpuTypeAlias, List[GpuTypeAlias]] = GpuType.NoGPU,
        gpu_count: int = 0,
        image: Image = Image(python_version="python3.11"),
        workers: int = 1,
        concurrent_requests: int = 10,
        keep_warm_seconds: int = 60,
        max_pending_tasks: int = 100,
        timeout: int = 3600,
        authorized: bool = False,
        name: Optional[str] = None,
        volumes: Optional[List[Union[Volume, CloudBucket]]] = [],
        secrets: Optional[List[str]] = None,
        autoscaler: Autoscaler = QueueDepthAutoscaler(),
        on_start: Optional[Callable[..., None]] = None,
        pricing: Optional[PricingPolicy] = None,
    ):
        image = image.add_python_packages(
            [
                f"fastmcp=={args.fastmcp_version}",
            ]
        )

        super().__init__(
            cpu=cpu,
            memory=memory,
            gpu=gpu,
            gpu_count=gpu_count,
            image=image,
            workers=workers,
            concurrent_requests=concurrent_requests,
            keep_warm_seconds=keep_warm_seconds,
            max_pending_tasks=max_pending_tasks,
            timeout=timeout,
            authorized=authorized,
            name=name,
            volumes=volumes,
            secrets=secrets,
            autoscaler=autoscaler,
            on_start=on_start,
            pricing=pricing,
        )

        self.fastmcp_server = server
        self.fastmcp_args = args

    def __name__(self) -> str:
        return self.name or f"fastmcp=={self.fastmcp_args.fastmcp_version}"

    def set_handler(self, handler: str):
        self.handler = handler

    def func(self, *args: Any, **kwargs: Any):
        pass

    def __call__(self, *args: Any, **kwargs: Any):
        return self.fastmcp_server.http_app(transport="sse", path="/sse")

    def deploy(
        self,
        name: Optional[str] = None,
        context: Optional[ConfigContext] = None,
        invocation_details_func: Optional[Callable[..., None]] = None,
        **invocation_details_options: Any,
    ) -> bool:
        self.name = name or self.name
        if not self.name:
            terminal.error(
                "You must specify an app name (either in the decorator or via the --name argument)."
            )

        if context is not None:
            self.config_context = context

        if not self.prepare_runtime(
            stub_type=ASGI_DEPLOYMENT_STUB_TYPE,
            force_create_stub=True,
        ):
            return False

        terminal.header("Deploying")
        deploy_response: DeployStubResponse = self.gateway_stub.deploy_stub(
            DeployStubRequest(stub_id=self.stub_id, name=self.name)
        )

        self.deployment_id = deploy_response.deployment_id
        if deploy_response.ok:
            terminal.header("Deployed 🎉")
            if invocation_details_func:
                invocation_details_func(**invocation_details_options)
            else:
                self.print_invocation_snippet(**invocation_details_options)

        return deploy_response.ok

    @with_grpc_error_handling
    def serve(self, timeout: int = 0, url_type: str = ""):
        stub_type = ASGI_SERVE_STUB_TYPE

        if not self.prepare_runtime(func=self.func, stub_type=stub_type, force_create_stub=True):
            return False

        try:
            with terminal.progress("Serving endpoint..."):
                self.print_invocation_snippet(url_type=url_type)
                return self._serve(dir=os.getcwd(), timeout=timeout)
        except KeyboardInterrupt:
            terminal.header("Stopping serve container")
            terminal.print("Goodbye 👋")
            os._exit(0)  # kills all threads immediately

    def _serve(self, *, dir: str, timeout: int = 0):
        r: StartEndpointServeResponse = self.endpoint_stub.start_endpoint_serve(
            StartEndpointServeRequest(
                stub_id=self.stub_id,
                timeout=timeout,
            )
        )
        if not r.ok:
            return terminal.error(r.error_msg)

        container = Container(container_id=r.container_id)
        container.attach(container_id=r.container_id, sync_dir=dir)
