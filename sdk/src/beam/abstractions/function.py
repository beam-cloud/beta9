import asyncio
import inspect
import os
from typing import Any, Callable, Iterable, Union

import cloudpickle

from beam import terminal
from beam.abstractions.base import BaseAbstraction
from beam.abstractions.image import Image, ImageBuildResult
from beam.clients.function import (
    FunctionInvokeResponse,
    FunctionServiceStub,
)
from beam.clients.gateway import GatewayServiceStub
from beam.sync import FileSyncer


class Function(BaseAbstraction):
    def __init__(self, image: Image = Image(), cpu: int = 100, memory: int = 128, gpu="") -> None:
        super().__init__()

        self.image: Image = image
        self.image_available: bool = False
        self.files_synced: bool = False
        self.runtime_ready: bool = False
        self.object_id: str = ""
        self.image_id: str = ""
        self.cpu = cpu
        self.memory = memory
        self.gpu = gpu

        self.gateway_stub: GatewayServiceStub = GatewayServiceStub(self.channel)
        self.function_stub: FunctionServiceStub = FunctionServiceStub(self.channel)
        self.syncer: FileSyncer = FileSyncer(self.gateway_stub)

    def __call__(self, func):
        return _CallableWrapper(func, self)


class _CallableWrapper:
    def __init__(self, func: Callable, parent: Function):
        self.func: Callable = func
        self.parent: Function = parent

    def _prepare_runtime(self) -> bool:
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

        self.parent.runtime_ready = True
        return True

    def __call__(self, *args, **kwargs) -> Any:
        task_id = os.getenv("TASK_ID")
        if task_id:
            return self.local(*args, **kwargs)

        if not self.parent.runtime_ready and not self._prepare_runtime():
            return

        with terminal.progress("Working..."):
            return self.parent.run_sync(self._call_remote(*args, **kwargs))

    async def _call_remote(self, *args, **kwargs) -> Any:
        module = inspect.getmodule(self.func)  # Determine module / function name
        if module:
            module_file = os.path.basename(module.__file__)
            module_name = os.path.splitext(module_file)[0]
        else:
            module_name = "__main__"

        function_name = self.func.__name__
        handler = f"{module_name}:{function_name}"

        args = cloudpickle.dumps(
            {
                "args": args,
                "kwargs": kwargs,
            },
        )

        terminal.header("Running function")
        last_response: Union[None, FunctionInvokeResponse] = None

        async for r in self.parent.function_stub.function_invoke(
            object_id=self.parent.object_id,
            image_id=self.parent.image_id,
            args=args,
            handler=handler,
            python_version=self.parent.image.python_version,
            cpu=self.parent.cpu,
            memory=self.parent.memory,
            gpu=self.parent.gpu,
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
        if not self.parent.runtime_ready and not self._prepare_runtime():
            return

        return self._gather_and_yield_results(inputs)
