from typing import List, Optional, Union

from .. import terminal
from ..abstractions.base.runner import (
    CONTAINER_STUB_TYPE,
    RunnerAbstraction,
)
from ..abstractions.image import Image
from ..abstractions.volume import Volume
from ..channel import with_grpc_error_handling
from ..clients.container import (
    CommandExecutionRequest,
    CommandExecutionResponse,
    ContainerServiceStub,
)
from ..sync import FileSyncer


class Container(RunnerAbstraction):
    """
    Container allows you to run arbitrary commands in a remote container.

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
        volumes (Optional[List[Volume]]):
            A list of volumes to be mounted to the container. Default is None.
        secrets (Optional[List[str]):
            A list of secrets that are injected into the container as environment variables. Default is [].
        name (Optional[str]):
            A name for the container. Default is None.
        callback_url (Optional[str]):
            An optional URL to send a callback to when a task is completed, timed out, or cancelled.

    Example usage:
        ```
        from beta9 import Image, Container


        def run_container():
            image = Image()
            container = Container(cpu=2, memory=512, image=image)
            exit_code = container.run((["python", "-c", "\"print('Hello, World!')\""]))
            print(exit_code)
        ```
    """

    def __init__(
        self,
        cpu: Union[int, float, str] = 1.0,
        memory: Union[int, str] = 128,
        gpu: str = "",
        image: Image = Image(),
        volumes: Optional[List[Volume]] = None,
        secrets: Optional[List[str]] = None,
        callback_url: Optional[str] = None,
    ) -> None:
        super().__init__(
            cpu=cpu, memory=memory, gpu=gpu, image=image, volumes=volumes, secrets=secrets, callback_url=callback_url
        )

        self.task_id = ""
        self._container_stub: Optional[ContainerServiceStub] = None
        self.syncer: FileSyncer = FileSyncer(self.gateway_stub)

    @property
    def stub(self) -> ContainerServiceStub:
        if not self._container_stub:
            self._container_stub = ContainerServiceStub(self.channel)
        return self._container_stub

    @stub.setter
    def stub(self, value: ContainerServiceStub) -> None:
        self._container_stub = value

    @with_grpc_error_handling
    def run(self, command: List[str]) -> int:
        """Run a command in a container and return the exit code"""
        if not self.prepare_runtime(
            stub_type=CONTAINER_STUB_TYPE,
        ):
            return 1

        with terminal.progress("Working..."):
            return self._run_remote(command)

    def _run_remote(self, command: List[str]) -> int:
        terminal.header("Running command")
        last_response = CommandExecutionResponse(done=False)

        for r in self.stub.execute_command(
            CommandExecutionRequest(stub_id=self.stub_id, command=" ".join(command).encode())
        ):
            if r.task_id != "":
                self.task_id = r.task_id

            if r.output != "":
                terminal.detail(r.output.strip())

            if r.done or r.exit_code != 0:
                last_response = r
                break

        if not last_response.done or last_response.exit_code != 0:
            terminal.warn(f"Command execution failed with exit code {last_response.exit_code} ❌")
            return last_response.exit_code

        terminal.header("Command execution complete 🎉")
        return last_response.exit_code
