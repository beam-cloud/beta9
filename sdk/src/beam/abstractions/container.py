import asyncio
import os
from typing import Any, Callable, Iterator, List, Optional, Sequence, Union

import cloudpickle

from beam import terminal
from beam.abstractions.base.runner import (
    CONTAINER_STUB_TYPE,
    RunnerAbstraction,
)
from beam.abstractions.image import Image
from beam.abstractions.volume import Volume
from beam.clients.container import CommandExecutionResponse, ContainerServiceStub
from beam.config import GatewayConfig, get_gateway_config
from beam.sync import FileSyncer

class Container(RunnerAbstraction):
    """
    Container
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

        self.container_stub = ContainerServiceStub(self.channel)
        self.syncer: FileSyncer = FileSyncer(self.gateway_stub)

    def run(self, command: List[str]) -> int: 
        """Run a command in a container"""
        if not self.prepare_runtime(
            func=None,
            stub_type=CONTAINER_STUB_TYPE,
        ):
            return 1

        with terminal.progress("Working..."):
            return self.run_sync(self._run_remote(command))
    
    async def _run_remote(self, command: List[str]) -> Any:
        terminal.header("Running command")

        last_response: Union[None, CommandExecutionResponse] = None

        async for r in self.container_stub.execute_command(
            stub_id=self.stub_id,
            command=' '.join(command).encode()
        ):
            # checking response to see if its output or done/error
            if r.output != "":
                terminal.detail(r.output)

            if r.done or r.exit_code != 0:
                last_response = r
                break

        if not last_response.done or last_response.exit_code != 0:
            terminal.error(f"Command execution failed with exit code {last_response.exit_code} ☠️")
            return None

        terminal.header("Command execution complete 🎉")
        return last_response.exit_code