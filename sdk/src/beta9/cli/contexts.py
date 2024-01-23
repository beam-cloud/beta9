from contextlib import contextmanager
from typing import Iterator

from beta9 import config
from beta9.clients.gateway import GatewayServiceStub


@contextmanager
def get_gateway_service() -> Iterator[GatewayServiceStub]:
    channel = config.get_gateway_channel()
    service = GatewayServiceStub(channel)

    try:
        yield service
    finally:
        channel.close()
