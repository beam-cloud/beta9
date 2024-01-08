import asyncio
import os
from typing import Any, Callable, Iterable, Union

import cloudpickle

from beam import terminal
from beam.abstractions.image import Image
from beam.abstractions.runner import RunnerAbstraction
from beam.clients.function import (
    FunctionInvokeResponse,
    FunctionServiceStub,
)
from beam.sync import FileSyncer

FUNCTION_STUB_TYPE = "FUNCTION"
FUNCTION_STUB_PREFIX = "function"


class Function(RunnerAbstraction):
    def __init__(self, image: Image, cpu: int = 100, memory: int = 128, gpu="") -> None:
        super().__init__(image=image, cpu=cpu, memory=memory, gpu=gpu)

        self.function_stub: FunctionServiceStub = FunctionServiceStub(self.channel)
        self.syncer: FileSyncer = FileSyncer(self.gateway_stub)

    def __call__(self, func):
        return _CallableWrapper(func, self)


class _CallableWrapper:
    def __init__(self, func: Callable, parent: Function):
        self.func: Callable = func
        self.parent: Function = parent

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
