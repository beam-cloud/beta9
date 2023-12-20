from typing import Callable, Union

from grpclib.client import Channel

from beam.abstractions.base import GatewayConfig, get_gateway_config
from beam.abstractions.image import Image
from beam.clients.gateway import GatewayServiceStub
from beam.sync import FileSyncer, SyncResult


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
        def _closure(*args, **kwargs) -> Callable:
            if not self.image_available and not self.image.build():
                return

            self.image_available = True

            sync_result: Union[SyncResult, None] = None
            if not self.files_synced:
                sync_result = self.syncer.sync()

                if sync_result.success:
                    self.files_synced = True
                    self.object_id = sync_result.object_id
                else:
                    return

            result = func(*args, **kwargs)
            return result

        return _closure

    def __del__(self):
        self.channel.close()
