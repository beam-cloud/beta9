from contextlib import contextmanager
from typing import Iterator

from beta9.clients.volume import VolumeServiceStub

from .. import config
from ..clients.gateway import GatewayServiceStub


class ServiceClient:
    def __init__(self) -> None:
        self.channel = config.get_gateway_channel()
        self.gateway = GatewayServiceStub(self.channel)
        self.volume = VolumeServiceStub(self.channel)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.channel.close()

    def close(self) -> None:
        self.channel.close()


@contextmanager
def get_gateway_service() -> Iterator[GatewayServiceStub]:
    channel = config.get_gateway_channel()
    service = GatewayServiceStub(channel)

    try:
        yield service
    finally:
        channel.close()
