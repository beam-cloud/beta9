from typing import Any, Callable, Union

from grpclib.client import Channel

from beam.abstractions.base import GatewayConfig, get_gateway_config
from beam.abstractions.image import Image
from beam.clients.gateway import GatewayServiceStub
from beam.sync import FileSyncer, SyncResult


class CallableWrapper:
    def __init__(self, func: Callable, parent: Any):
        self.func = func
        self.parent = parent

    def __call__(self, *args, **kwargs):
        if not self.parent.image_available and not self.parent.image.build():
            return

        self.parent.image_available = True

        sync_result: Union[SyncResult, None] = None
        if not self.parent.files_synced:
            sync_result = self.parent.syncer.sync()

            if sync_result and sync_result.success:
                self.parent.files_synced = True
                self.parent.object_id = sync_result.object_id
            else:
                return

        return self.func(*args, **kwargs)

    def local(self, *args, **kwargs) -> Any:
        return self.func(*args, **kwargs)

    def remote(self, *args, **kwargs) -> Any:
        return self(*args, **kwargs)

    def map(self):
        raise NotImplementedError


class Function:
    def __init__(self, image: Image = Image()) -> None:
        self.image: Image = image
        self.image_available: bool = False
        self.files_synced: bool = False
        self.object_id: str = ""

        config: GatewayConfig = get_gateway_config()
        self.channel: Channel = Channel(
            host=config.host,
            port=config.port,
            ssl=True if config.port == 443 else False,
        )
        self.stub: GatewayServiceStub = GatewayServiceStub(self.channel)
        self.syncer: FileSyncer = FileSyncer(self.stub)

    def __call__(self, func):
        return CallableWrapper(func, self)

    def __del__(self):
        self.channel.close()
