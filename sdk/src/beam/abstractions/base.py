import asyncio
from abc import ABC
from asyncio import AbstractEventLoop
from typing import Any, Coroutine

from grpclib.client import Channel

from beam.config import get_gateway_channel
from beam.exceptions import grpc_debug


@grpc_debug()
class BaseAbstraction(ABC):
    def __init__(self) -> None:
        self.loop: AbstractEventLoop = asyncio.get_event_loop()
        self.channel: Channel = get_gateway_channel()

    def run_sync(self, coroutine: Coroutine) -> Any:
        return self.loop.run_until_complete(coroutine)

    def __del__(self) -> None:
        self.channel.close()
