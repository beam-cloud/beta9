import inspect
import os
from typing import Any, Callable, Union

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

    def __call__(self, *args, **kwargs):
        invocation_id = os.getenv("INVOCATION_ID")
        if invocation_id:
            return self.local(*args, **kwargs)

        if not self.parent.image_available:
            image_build_result: ImageBuildResult = self.parent.image.build()

            if image_build_result and image_build_result.success:
                self.parent.image_available = True
                self.parent.image_id = image_build_result.image_id
            else:
                return

        if not self.parent.files_synced:
            sync_result = self.parent.syncer.sync()

            if sync_result.success:
                self.parent.files_synced = True
                self.parent.object_id = sync_result.object_id
            else:
                return

        # Determine module / function name
        module = inspect.getmodule(self.func)
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

        return self._invoke_remote(args=args, handler=handler)

    def _invoke_remote(self, *, args: bytes, handler: str):
        terminal.header("Running function")

        async def _call() -> FunctionInvokeResponse:
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
                terminal.detail(r.output)

                if r.done:
                    last_response = r
                    break

            return last_response

        with terminal.progress("Working..."):
            last_response: FunctionInvokeResponse = self.parent.loop.run_until_complete(_call())

        if not last_response.done:
            terminal.error("Function failed ☠️")
            return False

        terminal.header("Function complete 🎉")
        return True

    def local(self, *args, **kwargs) -> Any:
        return self.func(*args, **kwargs)

    def remote(self, *args, **kwargs) -> Any:
        return self(*args, **kwargs)

    def map(self):
        raise NotImplementedError
