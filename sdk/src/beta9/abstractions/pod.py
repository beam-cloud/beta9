import os
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union

from .. import terminal
from ..abstractions.base.runner import (
    POD_DEPLOYMENT_STUB_TYPE,
    POD_RUN_STUB_TYPE,
    RunnerAbstraction,
)
from ..abstractions.image import Image
from ..abstractions.mixins import DeployableMixin
from ..abstractions.volume import CloudBucket, Volume
from ..channel import with_grpc_error_handling
from ..clients.gateway import (
    DeployStubRequest,
    DeployStubResponse,
    GatewayServiceStub,
    StopContainerRequest,
    StopContainerResponse,
)
from ..clients.pod import (
    CreatePodRequest,
    CreatePodResponse,
    PodServiceStub,
)
from ..config import ConfigContext, get_settings
from ..sync import FileSyncer
from ..type import GpuType, GpuTypeAlias
from ..utils import get_init_args_kwargs
from .base import BaseAbstraction


@dataclass
class PodInstance(BaseAbstraction):
    """
    Stores the result of creating a Pod.

    Attributes:
        container_id: The unique ID of the created container.
        url: The URL for accessing the container over HTTP (if ports were exposed).
    """

    container_id: str
    url: str
    ok: bool = field(default=False)
    error_msg: str = field(default="")
    gateway_stub: "GatewayServiceStub" = field(init=False)

    def __post_init__(self):
        super().__init__()
        self.gateway_stub = GatewayServiceStub(self.channel)

    def terminate(self) -> bool:
        """
        Terminate the container associated with this pod instance. Returns True if the container was terminated, False otherwise.
        """
        res: "StopContainerResponse" = self.gateway_stub.stop_container(
            StopContainerRequest(container_id=self.container_id)
        )
        return res.ok


class Pod(RunnerAbstraction, DeployableMixin):
    """
    Pod allows you to run arbitrary services in fast, scalable, and secure remote containers.

    Parameters:
        app (str):
            Assign the pod to an app. If the app does not exist, it will be created with the given name.
            An app is a group of resources (endpoints, task queues, functions, etc).
        entrypoint (Optional[List[str]]):
            The command to run in the container. Default is [].
        ports (Optional[List[int]]):
            The ports to expose the container to. Default is [].
        name (Optional[str]):
            An optional app name for this pod. If not specified, it will be the name of the
            working directory containing the python file with the pod class.
        cpu (Union[int, float, str]):
            The number of CPU cores allocated to the pod. Default is 1.0.
        memory (Union[int, str]):
            The amount of memory allocated to the pod. It should be specified in
            MiB, or as a string with units (e.g. "1Gi"). Default is 128 MiB.
        gpu (Union[GpuTypeAlias, List[GpuTypeAlias]]):
            The type or name of the GPU device to be used for GPU-accelerated tasks. If not
            applicable or no GPU required, leave it empty.
            You can specify multiple GPUs by providing a list of GpuTypeAlias. If you specify several GPUs,
            the scheduler prioritizes their selection based on their order in the list.
        gpu_count (int):
            The number of GPUs allocated to the pod. Default is 0. If a GPU is
            specified but this value is set to 0, it will be automatically updated to 1.
        image (Union[Image, dict]):
            The container image used for the task execution. Default is [Image](#image).
        volumes (Optional[List[Union[Volume, CloudBucket]]]):
            A list of volumes and/or cloud buckets to be mounted to the pod. Default is None.
        secrets (Optional[List[str]):
            A list of secrets that are injected into the pod as environment variables. Default is [].
        env (Optional[Dict[str, str]]):
            A dictionary of environment variables to be injected into the container. Default is {}.
        keep_warm_seconds (int):
            The number of seconds to keep the container up the last request. -1 means never scale down to zero.
            Default is 600 seconds (10 minutes).
        authorized (bool):
            If false, allows the pod to be accessed without an auth token.
            Default is False.

    Example usage:
        ```
        from beta9 import Image, Pod

        image = Image()
        pod = Pod(cpu=2, memory=512, image=image, ports=[8080])
        result = pod.create(entrypoint=["python", "-c", "\"print('Hello, World!')\""])
        print(result.container_id)
        print(result.url)

        ```
    """

    def __init__(
        self,
        app: str = "",
        entrypoint: List[str] = [],
        ports: Optional[List[int]] = [],
        name: Optional[str] = None,
        cpu: Union[int, float, str] = 1.0,
        memory: Union[int, str] = 128,
        gpu: Union[GpuTypeAlias, List[GpuTypeAlias]] = GpuType.NoGPU,
        gpu_count: int = 0,
        image: Image = Image(),
        volumes: Optional[List[Union[Volume, CloudBucket]]] = None,
        secrets: Optional[List[str]] = None,
        env: Optional[Dict[str, str]] = {},
        keep_warm_seconds: int = 600,
        authorized: bool = False,
    ) -> None:
        super().__init__(
            cpu=cpu,
            memory=memory,
            gpu=gpu,
            gpu_count=gpu_count,
            image=image,
            volumes=volumes,
            secrets=secrets,
            env=env,
            entrypoint=entrypoint,
            ports=ports,
            name=name,
            authorized=authorized,
            keep_warm_seconds=keep_warm_seconds,
            app=app,
        )
        self.parent = self
        self.func = None
        self.task_id = ""
        self._pod_stub: Optional[PodServiceStub] = None
        self.syncer: FileSyncer = FileSyncer(self.gateway_stub)
        self.image.ignore_python = True

        # This a temporary id generated by each class during each runtime
        self._id = str(uuid.uuid4())[:8]

    @property
    def stub(self) -> PodServiceStub:
        if not self._pod_stub:
            self._pod_stub = PodServiceStub(self.channel)
        return self._pod_stub

    @stub.setter
    def stub(self, value: PodServiceStub) -> None:
        self._pod_stub = value

    def parse_image(self, image: Image) -> Image:
        image.ignore_python = True
        return image

    def create(self, entrypoint: List[str] = []) -> PodInstance:
        """
        Create a new container that will run until either it completes normally, or times out.

        Args:
            entrypoint (List[str]): The command to run in the pod container (overrides the entrypoint specified in the Pod constructor).
        """
        if entrypoint:
            self.entrypoint = entrypoint

        if not self.entrypoint:
            terminal.error("You must specify an entrypoint.")

        if not self.prepare_runtime(stub_type=POD_RUN_STUB_TYPE, force_create_stub=True):
            return PodInstance(
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

        return PodInstance(
            container_id=create_response.container_id,
            url=url,
            ok=create_response.ok,
            error_msg=create_response.error_msg,
        )

    def deploy(
        self,
        name: Optional[str] = None,
        context: Optional[ConfigContext] = None,
        **invocation_details_options: Any,
    ):
        self.name = name or self.name
        if not self.name:
            terminal.error(
                "You must specify an app name (either in the decorator or via the --name argument)."
            )

        if not self.entrypoint:
            terminal.error("You must specify an entrypoint.")
            return False

        if context is not None:
            self.config_context = context

        if not self.prepare_runtime(stub_type=POD_DEPLOYMENT_STUB_TYPE, force_create_stub=True):
            return False

        terminal.header("Deploying")
        deploy_response: DeployStubResponse = self.gateway_stub.deploy_stub(
            DeployStubRequest(stub_id=self.stub_id, name=self.name)
        )

        self.deployment_id = deploy_response.deployment_id
        if deploy_response.ok:
            terminal.header("Deployed 🎉")

            if len(self.ports) > 0:
                self.print_invocation_snippet()

        return deploy_response.ok

    def generate_deployment_artifacts(self, **kwargs) -> str:
        imports = ["Pod"]

        pod_py = """
from {module} import {import_string}

app = Pod(
{arguments}
)
"""
        arguments = []
        argkwargs = get_init_args_kwargs(self.__class__)
        for key, value in kwargs.items():
            if key not in argkwargs or value is None:
                continue

            if isinstance(value, Image):
                imports.append("Image")
                value = f'Image(base_image="{value.base_image}")'
            elif isinstance(value, tuple):
                value = list(value)
            elif isinstance(value, str):
                value = f'"{value}"'

            arguments.append(f"    {key}={value}")

        content = pod_py.format(
            module=get_settings().name.lower(),
            import_string=", ".join(imports),
            arguments=",\n".join(arguments),
        )

        with open(f"pod-{self._id}.py", "w") as f:
            f.write(content)

    def cleanup_deployment_artifacts(self):
        if os.path.exists(f"pod-{self._id}.py"):
            os.remove(f"pod-{self._id}.py")

    @with_grpc_error_handling
    def shell(
        self, url_type: str = "", sync_dir: Optional[str] = None, container_id: Optional[str] = None
    ):
        self.authorized = True
        super().shell(url_type=url_type, sync_dir=sync_dir, container_id=container_id)

    def serve(self, **kwargs):
        terminal.error("Serve has not yet been implemented for Pods.")
