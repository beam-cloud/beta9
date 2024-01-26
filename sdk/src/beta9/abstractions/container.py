import asyncio
from typing import Any, List, Optional, Union

from beta9 import terminal
from beta9.abstractions.base.runner import (
    CONTAINER_STUB_TYPE,
    RunnerAbstraction,
)
from beta9.abstractions.image import Image
from beta9.abstractions.volume import Volume
from beta9.clients.container import CommandExecutionResponse, ContainerServiceStub, StopContainerRunResponse
from beta9.sync import FileSyncer

class PrepareRuntimeError(RuntimeError):
    """Exception raised when an external system setup fails."""
    pass

class ContainerHandle:
    """
    A handle to manage the asynchronous container process.
    """
    def __init__(self, task: asyncio.Task, container: 'Container'):
        self.task = task
        self.container = container

    def stop(self) -> bool:
        if self.container._stop():
            self.task.cancel()
            return True
        return False

class Container(RunnerAbstraction):
    """
    Container allows you to run arbitrary commands in a remote container.

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

    Example usage:
        ```python
        >>> from beta9.abstractions.image import Image
        >>> from beta9.abstractions.volume import Volume
        >>> image = Image(name="python", tag="3.8")
        >>> container = Container(cpu=2, memory=512, image=image))
        >>> exit_code = container.run(["python", "-c", "\"print('Hello, World!')\""])
        >>> print(exit_code)
        0
        ```
    """
    def __init__(
        self,
        cpu: Union[int, float, str] = 1.0,
        memory: int = 128,
        gpu: str = "",
        image: Image = Image(),
        volumes: Optional[List[Volume]] = None,
    ) -> "Container":
        super().__init__(cpu=cpu, memory=memory, gpu=gpu, image=image, volumes=volumes)

        self.container_id = ""
        self.container_stub = ContainerServiceStub(self.channel)
        self.syncer: FileSyncer = FileSyncer(self.gateway_stub)

    def run(self, command: List[str]) -> int: 
        """Run a command in a container and return the exit code"""
        if not self.prepare_runtime(
            stub_type=CONTAINER_STUB_TYPE,
        ):
            return 1

        with terminal.progress("Working..."):
            return self.run_sync(self._run_remote(command))
    
    async def run_async(self, command: List[str]) -> ContainerHandle:
        """Run a command in a container and return a task that resolves to the exit code"""
        # FIXME: prepare_runtime_async is in rough shape and could use some love
        result: bool = await self.prepare_runtime_async(stub_type=CONTAINER_STUB_TYPE)
        if not result:
            terminal.error("Failed to prepare runtime for async container command execution")

        task = asyncio.create_task(self._run_remote(command))
        return ContainerHandle(task, self)

    def _stop(self) -> bool:
        if self.container_id == "":
            terminal.warn(f"Failed to stop container command execution: container_id not found")
            return False
        stop_result: StopContainerRunResponse = self.run_sync(self.container_stub.stop_container(container_id=self.container_id))
        if not stop_result.success:
            terminal.warn(f"Failed to stop container command execution: {stop_result.message}")
        return stop_result.success

    async def _run_remote(self, command: List[str]) -> int:
        terminal.header("Running command")

        last_response: Union[None, CommandExecutionResponse] = None

        async for r in self.container_stub.execute_command(
            stub_id=self.stub_id,
            command=' '.join(command).encode()
        ):
            if r.task_id != "":
                self.container_id = r.task_id
            # checking response to see if its output or done/error
            if r.output != "":
                terminal.detail(r.output)

            if r.done or r.exit_code != 0:
                last_response = r
                break

        if not last_response.done or last_response.exit_code != 0:
            terminal.warn(f"Command execution failed with exit code {last_response.exit_code} ☠️")
            return last_response.exit_code

        terminal.header("Command execution complete 🎉")
        return last_response.exit_code