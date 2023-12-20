from typing import Any, Callable, Union

import cloudpickle
from grpclib.client import Channel

from beam.abstractions.base import GatewayConfig, get_gateway_config
from beam.abstractions.image import Image
from beam.clients.function import FunctionServiceStub
from beam.clients.gateway import GatewayServiceStub
from beam.sync import FileSyncer, FileSyncResult


class Function:
    def __init__(self, image: Image = Image()) -> None:
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


class _CallableWrapper:
    def __init__(self, func: Callable, parent: Function):
        self.func: Callable = func
        self.parent: Function = parent

    def __call__(self, *args, **kwargs):
        if not self.parent.image_available and not self.parent.image.build():
            return

        self.parent.image_available = True

        sync_result: Union[FileSyncResult, None] = None
        if not self.parent.files_synced:
            sync_result = self.parent.syncer.sync()

            if sync_result and sync_result.success:
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

        self.parent.function_stub.function_invoke(
            object_id=self.parent.object_id,
            image_id=self.parent.image_id,
            args=args,
            entry_point="test.test.test",
        )

        return  # self.func()

    def local(self, *args, **kwargs) -> Any:
        return self.func(*args, **kwargs)

    def remote(self, *args, **kwargs) -> Any:
        return self(*args, **kwargs)

    def map(self):
        raise NotImplementedError
