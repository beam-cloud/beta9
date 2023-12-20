import os
from typing import Any, Callable, Union

import cloudpickle
from grpclib.client import Channel

from beam.abstractions.base import BaseAbstraction, GatewayConfig, get_gateway_config
from beam.abstractions.image import Image, ImageBuildResult
from beam.clients.function import (
    FunctionGetArgsResponse,
    FunctionInvokeResponse,
    FunctionServiceStub,
)
from beam.clients.gateway import GatewayServiceStub
from beam.sync import FileSyncer
from beam.terminal import Terminal


class Function(BaseAbstraction):
    def __init__(self, image: Image = Image()) -> None:
        super().__init__()

        self.image: Image = image
        self.image_available: bool = False
        self.files_synced: bool = False
        self.object_id: str = ""
        self.image_id: str = ""

        config: GatewayConfig = get_gateway_config()
        self.channel: Channel = Channel(
            host=config.host,
            port=config.port,
            ssl=True if config.port == 443 else False,
        )
        self.gateway_stub: GatewayServiceStub = GatewayServiceStub(self.channel)
        self.function_stub: FunctionServiceStub = FunctionServiceStub(self.channel)
        self.syncer: FileSyncer = FileSyncer(self.gateway_stub)

    def __call__(self, func):
        return _CallableWrapper(func, self)

    def __del__(self):
        self.channel.close()

    def remote(self):
        invocation_id = os.getenv("INVOCATION_ID")
        if invocation_id is None:
            return

        print("Invocation ID: ", invocation_id)

        r: FunctionGetArgsResponse = self.run_sync(
            self.function_stub.function_get_args(invocation_id=invocation_id)
        )

        if not r.ok:
            return

        args: dict = cloudpickle.loads(r.args)
        print(args)

        # TODO: load the handler module and pass args
        pass


class _CallableWrapper:
    def __init__(self, func: Callable, parent: Function):
        self.func: Callable = func
        self.parent: Function = parent

    def __call__(self, *args, **kwargs):
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

        args = cloudpickle.dumps(
            {
                "args": args,
                "kwargs": kwargs,
            }
        )

        return self._invoke_remote(args=args, handler="test.test.test")

    def _invoke_remote(self, *, args: bytes, handler: str):
        Terminal.header("Running function")
        print("Image ID:", self.parent.image_id)
        print("Object ID:", self.parent.object_id)

        async def _call() -> FunctionInvokeResponse:
            last_response: Union[None, FunctionInvokeResponse] = None

            async for r in self.parent.function_stub.function_invoke(
                object_id=self.parent.object_id,
                image_id=self.parent.image_id,
                args=args,
                handler=handler,
            ):
                Terminal.detail(r.output)

                if r.done:
                    last_response = r
                    break

            return last_response

        with Terminal.progress("Working..."):
            last_response: FunctionInvokeResponse = self.parent.loop.run_until_complete(_call())

        if not last_response.done:
            Terminal.error("Function failed ☠️")
            return False

        Terminal.header("Function complete 🎉")
        return True

    def local(self, *args, **kwargs) -> Any:
        return self.func(*args, **kwargs)

    def remote(self, *args, **kwargs) -> Any:
        return self(*args, **kwargs)

    def map(self):
        raise NotImplementedError
