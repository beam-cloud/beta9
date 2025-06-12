from dataclasses import dataclass, field
from typing import List, Optional, Union

from .. import terminal
from ..abstractions.base.runner import (
    POD_RUN_STUB_TYPE,
    BaseAbstraction,
)
from ..abstractions.image import Image
from ..abstractions.pod import Pod
from ..abstractions.volume import CloudBucket, Volume
from ..clients.gateway import GatewayServiceStub, StopContainerRequest, StopContainerResponse
from ..clients.pod import (
    CreatePodRequest,
    CreatePodResponse,
    PodExecRequest,
    PodServiceStub,
)
from ..type import GpuType, GpuTypeAlias


class Sandbox(Pod):
    """

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
            The container image used for the task execution. Whatever you pass here will have an additional `add_python_packages` call
            with `["fastapi", "vllm", "huggingface_hub"]` added to it to ensure that we can run vLLM in the container.
        keep_warm_seconds (int):
            The number of seconds to keep the container warm after the last request. Default is 60.
        timeout (int):
            The maximum number of seconds to wait for the container to start. Default is 3600.
        name (str):
            The name of the Sandbox app. Default is none, which means you must provide it during deployment.
        volumes (List[Union[Volume, CloudBucket]]):
            The volumes and/or cloud buckets to mount into the Sandbox container. Default is an empty list.
        secrets (List[str]):
            The secrets to pass to the Sandbox container.
    """

    def __init__(
        self,
        cpu: Union[int, float, str] = 1.0,
        memory: Union[int, str] = 128,
        gpu: Union[GpuTypeAlias, List[GpuTypeAlias]] = GpuType.NoGPU,
        gpu_count: int = 0,
        image: Image = Image(python_version="python3.11"),
        keep_warm_seconds: int = 60,
        authorized: bool = False,
        name: Optional[str] = None,
        volumes: Optional[List[Union[Volume, CloudBucket]]] = [],
        secrets: Optional[List[str]] = None,
    ):
        super().__init__(
            cpu=cpu,
            memory=memory,
            gpu=gpu,
            gpu_count=gpu_count,
            image=image,
            keep_warm_seconds=keep_warm_seconds,
            authorized=authorized,
            name=name,
            volumes=volumes,
            secrets=secrets,
        )

    def create(self) -> "SandboxInstance":
        """
        Create a new sandbox instance.

        """

        self.entrypoint = ["tail", "-f", "/dev/null"]

        if not self.prepare_runtime(stub_type=POD_RUN_STUB_TYPE, force_create_stub=True):
            return SandboxInstance(
                container_id="",
                url="",
                ok=False,
                error_msg="Failed to prepare runtime",
            )

        terminal.header("Creating container")
        create_response: CreatePodResponse = self.stub.create_pod(
            CreatePodRequest(
                stub_id=self.stub_id,
            )
        )

        url = ""
        if create_response.ok:
            terminal.header(f"Container created successfully ===> {create_response.container_id}")

            if self.keep_warm_seconds < 0:
                terminal.header("This container has no timeout, it will run until it completes.")
            else:
                terminal.header(
                    f"This container will timeout after {self.keep_warm_seconds} seconds."
                )

            url_res = self.print_invocation_snippet()
            url = url_res.url

        return SandboxInstance(
            container_id=create_response.container_id,
            url=url,
            ok=create_response.ok,
            error_msg=create_response.error_msg,
        )


@dataclass
class SandboxInstance(BaseAbstraction):
    """
    Stores the result of creating a Sandbox.

    Attributes:
        container_id: The unique ID of the created sandbox container.
        url: The URL for accessing the container over HTTP (if ports were exposed).
    """

    container_id: str
    url: str
    ok: bool = field(default=False)
    error_msg: str = field(default="")
    gateway_stub: "GatewayServiceStub" = field(init=False)
    stub: "PodServiceStub" = field(init=False)

    def __post_init__(self):
        super().__init__()
        self.gateway_stub = GatewayServiceStub(self.channel)
        self.stub = PodServiceStub(self.channel)
        self.fs = SandboxFileSystem(self)
        self.process = SandboxProcess(self)

    def terminate(self) -> bool:
        """
        Terminate the container associated with this sandbox instance. Returns True if the container was terminated, False otherwise.
        """
        res: "StopContainerResponse" = self.gateway_stub.stop_container(
            StopContainerRequest(container_id=self.container_id)
        )
        return res.ok


class SandboxProcess:
    def __init__(self, sandbox_instance: SandboxInstance):
        self.sandbox_instance = sandbox_instance

    def run_code(self, code: str):
        pass

    def exec(self, command: List[str]):
        for response in self.sandbox_instance.stub.exec(
            PodExecRequest(
                container_id=self.sandbox_instance.container_id, command=" ".join(command)
            )
        ):
            if response.exit_code == 0:
                return response.data
            else:
                raise Exception(response.error_msg)


class SandboxFileSystem:
    """
    A SandboxFileSystem is a wrapper around the SandboxFileSystem library that allows you to deploy it as an ASGI app.
    """

    def __init__(self, sandbox_instance: SandboxInstance):
        self.sandbox_instance = sandbox_instance

    def upload_file(self, local_path: str, container_path: str):
        pass

    def download_file(self, container_path: str, local_path: str):
        pass

    def list_files(self, container_path: str):
        pass
