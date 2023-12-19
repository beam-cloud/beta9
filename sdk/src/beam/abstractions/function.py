from typing import Callable

from grpclib.client import Channel

from beam.abstractions.base import GatewayConfig, get_gateway_config
from beam.abstractions.image import Image
from beam.clients.gateway import GatewayServiceStub
from beam.sync import FileSyncer


class Function:
    def __init__(self, image: Image = Image()) -> None:
        self.image: Image = image
        self.image_available: bool = False
        self.files_synced: bool = False

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

            if not self.files_synced and not self.syncer.sync():
                return

            self.files_synced = True

            result = func(*args, **kwargs)
            return result

        return _closure

    def __del__(self):
        self.channel.close()
