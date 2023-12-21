import asyncio
from abc import ABC

from grpclib.client import Channel

from beam.config import get_gateway_channel


class BaseAbstraction(ABC):
    def __init__(self) -> None:
        self.loop = asyncio.get_event_loop()
        self.channel: Channel = get_gateway_channel()

    def run_sync(self, coroutine):
        return self.loop.run_until_complete(coroutine)

    def __del__(self):
        self.channel.close()
