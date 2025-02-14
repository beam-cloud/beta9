from typing import Any, List, Optional, Union

from .. import terminal
from ..abstractions.base.runner import (
    POD_DEPLOYMENT_STUB_TYPE,
    POD_RUN_STUB_TYPE,
    RunnerAbstraction,
)
from ..abstractions.image import Image
from ..abstractions.volume import Volume
from ..clients.gateway import (
    DeployStubRequest,
    DeployStubResponse,
)
from ..clients.pod import (
    CreatePodRequest,
    CreatePodResponse,
    PodServiceStub,
)
from ..config import ConfigContext
from ..sync import FileSyncer
from ..type import GpuType, GpuTypeAlias


class Pod(RunnerAbstraction):
    """
    Pod allows you to run arbitrary services in fast, scalable, and secure remote containers.

    Parameters:
        entrypoint (List[str]): Required
            The command to run in the container.
        port (Optional[int]):
            The port to expose the container to. Default is None.
        name (Optional[str]):
            A name for the pod. Default is None.
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
        volumes (Optional[List[Volume]]):
            A list of volumes to be mounted to the pod. Default is None.
        secrets (Optional[List[str]):
            A list of secrets that are injected into the pod as environment variables. Default is [].

    Example usage:
        ```
        from beta9 import Image, Pod

        image = Image()
        pod = Pod(cpu=2, memory=512, image=image)
        exit_code = pod.run((["python", "-c", "\"print('Hello, World!')\""]))
        print(exit_code)
        ```
    """

    def __init__(
        self,
        entrypoint: List[str],
        port: Optional[int] = None,
        name: Optional[str] = None,
        cpu: Union[int, float, str] = 1.0,
        memory: Union[int, str] = 128,
        gpu: Union[GpuTypeAlias, List[GpuTypeAlias]] = GpuType.NoGPU,
        gpu_count: int = 0,
        image: Image = Image(),
        volumes: Optional[List[Volume]] = None,
        secrets: Optional[List[str]] = None,
    ) -> None:
        super().__init__(
            cpu=cpu,
            memory=memory,
            gpu=gpu,
            gpu_count=gpu_count,
            image=image,
            volumes=volumes,
            secrets=secrets,
            entrypoint=entrypoint,
            port=port,
            name=name,
        )

        self.task_id = ""
        self._pod_stub: Optional[PodServiceStub] = None
        self.syncer: FileSyncer = FileSyncer(self.gateway_stub)

    @property
    def stub(self) -> PodServiceStub:
        if not self._pod_stub:
            self._pod_stub = PodServiceStub(self.channel)
        return self._pod_stub

    @stub.setter
    def stub(self, value: PodServiceStub) -> None:
        self._pod_stub = value

    def create(self) -> str:
        if not self.prepare_runtime(stub_type=POD_RUN_STUB_TYPE, force_create_stub=True):
            return ""

        terminal.header("Creating")
        create_response: CreatePodResponse = self.stub.create_pod(
            CreatePodRequest(
                stub_id=self.stub_id,
            )
        )

        if create_response.ok:
            terminal.header(f"Container created successfully ===> {create_response.container_id}")
            self.print_invocation_snippet(url_type="path")

        return create_response.container_id

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
            self.print_invocation_snippet(**invocation_details_options)

        return deploy_response.ok
