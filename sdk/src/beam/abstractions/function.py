import asyncio
import os
from typing import Any, Callable, Iterable, List, Optional, Union

import cloudpickle

from beam import terminal
<<<<<<< HEAD
from beam.abstractions.image import Image
from beam.abstractions.runner import RunnerAbstraction
=======
from beam.abstractions.base import BaseAbstraction
from beam.abstractions.image import Image, ImageBuildResult
from beam.abstractions.volume import Volume
>>>>>>> master
from beam.clients.function import (
    FunctionInvokeResponse,
    FunctionServiceStub,
)
from beam.sync import FileSyncer

FUNCTION_STUB_TYPE = "FUNCTION"
FUNCTION_STUB_PREFIX = "function"


<<<<<<< HEAD
class Function(RunnerAbstraction):
    def __init__(self, image: Image, cpu: int = 100, memory: int = 128, gpu="") -> None:
        super().__init__(image=image, cpu=cpu, memory=memory, gpu=gpu)

=======
class Function(BaseAbstraction):
    def __init__(
        self,
        image: Image,
        cpu: int = 100,
        memory: int = 128,
        gpu="",
        volumes: Optional[List[Volume]] = None,
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
        self.volumes = volumes or []

        self.gateway_stub: GatewayServiceStub = GatewayServiceStub(self.channel)
>>>>>>> master
        self.function_stub: FunctionServiceStub = FunctionServiceStub(self.channel)
        self.syncer: FileSyncer = FileSyncer(self.gateway_stub)

    def __call__(self, func):
        return _CallableWrapper(func, self)


class _CallableWrapper:
    def __init__(self, func: Callable, parent: Function):
        self.func: Callable = func
        self.parent: Function = parent

<<<<<<< HEAD
=======
    def _prepare_runtime(self) -> bool:
        module = inspect.getmodule(self.func)  # Determine module / function name
        if module:
            module_file = os.path.basename(module.__file__)
            module_name = os.path.splitext(module_file)[0]
        else:
            module_name = "__main__"

        function_name = self.func.__name__
        self.parent.handler = f"{module_name}:{function_name}"

        if not self.parent.image_available:
            image_build_result: ImageBuildResult = self.parent.image.build()

            if image_build_result and image_build_result.success:
                self.parent.image_available = True
                self.parent.image_id = image_build_result.image_id
            else:
                return False

        if not self.parent.files_synced:
            sync_result = self.parent.syncer.sync()

            if sync_result.success:
                self.parent.files_synced = True
                self.parent.object_id = sync_result.object_id
            else:
                return False

        for volume in self.parent.volumes:
            if not volume.ready and not volume.get_or_create():
                return False

        if not self.parent.stub_created:
            stub_response: GetOrCreateStubResponse = self.parent.run_sync(
                self.parent.gateway_stub.get_or_create_stub(
                    object_id=self.parent.object_id,
                    image_id=self.parent.image_id,
                    stub_type=FUNCTION_STUB_TYPE,
                    name=f"{FUNCTION_STUB_PREFIX}/{self.parent.handler}",
                    python_version=self.parent.image.python_version,
                    cpu=self.parent.cpu,
                    memory=self.parent.memory,
                    gpu=self.parent.gpu,
                    volumes=[volume.export() for volume in self.parent.volumes],
                )
            )

            if stub_response.ok:
                self.parent.stub_created = True
                self.parent.stub_id = stub_response.stub_id
            else:
                return False

        self.parent.runtime_ready = True
        return True

>>>>>>> master
    def __call__(self, *args, **kwargs) -> Any:
        container_id = os.getenv("CONTAINER_ID")
        if container_id:
            return self.local(*args, **kwargs)

        self.parent.load_handler(self.func)

        if not self.parent.prepare_runtime(
            stub_type=FUNCTION_STUB_TYPE,
            stub_name=f"{FUNCTION_STUB_PREFIX}/{self.parent.handler}",
        ):
            return

        with terminal.progress("Working..."):
            return self.parent.run_sync(self._call_remote(*args, **kwargs))

    async def _call_remote(self, *args, **kwargs) -> Any:
        args = cloudpickle.dumps(
            {
                "args": args,
                "kwargs": kwargs,
            },
        )

        terminal.header("Running function")
        last_response: Union[None, FunctionInvokeResponse] = None

        async for r in self.parent.function_stub.function_invoke(
            stub_id=self.parent.stub_id,
            args=args,
<<<<<<< HEAD
=======
            handler=self.parent.handler,
            python_version=self.parent.image.python_version,
            cpu=self.parent.cpu,
            memory=self.parent.memory,
            gpu=self.parent.gpu,
            volumes=[volume.export() for volume in self.parent.volumes],
>>>>>>> master
        ):
            if r.output != "":
                terminal.detail(r.output)

            if r.done:
                last_response = r
                break

        if not last_response.done or last_response.exit_code != 0:
            terminal.error("Function failed ☠️")
            return None

        terminal.header("Function complete 🎉")
        return cloudpickle.loads(last_response.result)

    def local(self, *args, **kwargs) -> Any:
        return self.func(*args, **kwargs)

    def remote(self, *args, **kwargs) -> Any:
        return self(*args, **kwargs)

    def _gather_and_yield_results(self, inputs: Iterable):
        async def _gather_async():
            tasks = [asyncio.create_task(self._call_remote(input)) for input in inputs]
            for task in asyncio.as_completed(tasks):
                yield await task

        async_gen = _gather_async()
        while True:
            try:
                yield self.parent.loop.run_until_complete(async_gen.__anext__())
            except StopAsyncIteration:
                break

    def map(self, inputs: Iterable):
        self.parent.load_handler(self.func)

        if not self.parent.prepare_runtime(
            stub_type=FUNCTION_STUB_TYPE,
            stub_name=f"{FUNCTION_STUB_PREFIX}/{self.parent.handler}",
        ):
            return

        return self._gather_and_yield_results(inputs)
