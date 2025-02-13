from typing import List, Optional, Union

from .. import terminal
from ..abstractions.base.runner import (
    RunnerAbstraction,
)
from ..abstractions.image import Image
from ..abstractions.volume import Volume
from ..clients.pod import (
    PodServiceStub,
    RunPodRequest,
    RunPodResponse,
)
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

    def run(self):
        if not self.prepare_runtime(stub_type="pod"):
            return False

        terminal.header("Running")
        run_response: RunPodResponse = self.stub.run_pod(
            RunPodRequest(
                stub_id=self.stub_id,
            )
        )

        if run_response.ok:
            terminal.header("Running pod 🎉")

        return run_response.ok
