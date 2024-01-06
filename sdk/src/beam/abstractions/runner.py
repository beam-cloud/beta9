import inspect
import os
from typing import Callable

from beam.abstractions.base import BaseAbstraction
from beam.abstractions.image import Image, ImageBuildResult
from beam.clients.gateway import GatewayServiceStub, GetOrCreateStubResponse
from beam.sync import FileSyncer


class RunnerAbstraction(BaseAbstraction):
    def __init__(
        self,
        image: Image,
        cpu: int = 100,
        memory: int = 128,
        gpu="",
    ) -> None:
        super().__init__()

        if image is None:
            image = Image()

        self.image: Image = image
        self.image_available: bool = False
        self.files_synced: bool = False
        self.stub_created: bool = False
        self.runtime_ready: bool = False
        self.object_id: str = ""
        self.image_id: str = ""
        self.stub_id: str = ""
        self.handler: str = ""
        self.cpu = cpu
        self.memory = memory
        self.gpu = gpu

        self.gateway_stub: GatewayServiceStub = GatewayServiceStub(self.channel)
        self.syncer: FileSyncer = FileSyncer(self.gateway_stub)

    def prepare_runtime(self, *, func: Callable, stub_type: str, stub_name: str) -> bool:
        if self.runtime_ready:
            return True

        module = inspect.getmodule(func)  # Determine module / function name
        if module:
            module_file = os.path.basename(module.__file__)
            module_name = os.path.splitext(module_file)[0]
        else:
            module_name = "__main__"

        function_name = func.__name__
        self.handler = f"{module_name}:{function_name}"

        if not self.image_available:
            image_build_result: ImageBuildResult = self.image.build()

            if image_build_result and image_build_result.success:
                self.image_available = True
                self.image_id = image_build_result.image_id
            else:
                return False

        if not self.files_synced:
            sync_result = self.syncer.sync()

            if sync_result.success:
                self.files_synced = True
                self.object_id = sync_result.object_id
            else:
                return False

        if not self.stub_created:
            stub_response: GetOrCreateStubResponse = self.run_sync(
                self.gateway_stub.get_or_create_stub(
                    object_id=self.object_id,
                    image_id=self.image_id,
                    stub_type=stub_type,
                    name=stub_name,
                    python_version=self.image.python_version,
                    cpu=self.cpu,
                    memory=self.memory,
                    gpu=self.gpu,
                    handler=self.handler,
                )
            )

            if stub_response.ok:
                self.stub_created = True
                self.stub_id = stub_response.stub_id
            else:
                return False

        self.runtime_ready = True
        return True
