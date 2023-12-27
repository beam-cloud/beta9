import asyncio
from abc import ABC
from asyncio import AbstractEventLoop
from typing import Any, Coroutine

import grpclib
from grpclib.client import Channel

from beam.config import get_gateway_channel


class BaseAbstraction(ABC):
    def __init__(self) -> None:
        self.loop: AbstractEventLoop = asyncio.get_event_loop()
        self.channel: Channel = get_gateway_channel()

    def run_sync(self, coroutine: Coroutine) -> Any:
        try:
            return self.loop.run_until_complete(coroutine)
        except (
            grpclib.exceptions.GRPCError,
            grpclib.exceptions.StreamTerminatedError,
        ):
            raise ConnectionError from None

    def __del__(self) -> None:
        self.channel.close()
