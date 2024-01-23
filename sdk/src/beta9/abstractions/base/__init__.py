import asyncio
from abc import ABC
from asyncio import AbstractEventLoop
from typing import Any, Coroutine

from beta9.config import get_gateway_channel
from grpclib.client import Channel


class BaseAbstraction(ABC):
    def __init__(self) -> None:
        self.loop: AbstractEventLoop = asyncio.get_event_loop()

        self.channel: Channel = get_gateway_channel()

    def run_sync(self, coroutine: Coroutine) -> Any:
        return self.loop.run_until_complete(coroutine)

    def __del__(self) -> None:
        try:
            self.channel.close()
        except AttributeError:
            return
