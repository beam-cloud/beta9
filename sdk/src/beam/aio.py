import asyncio
from asyncio import AbstractEventLoop
from typing import Any, Coroutine, Union

import grpclib


def run_sync(coroutine: Coroutine, loop: Union[AbstractEventLoop, None] = None) -> Any:
    try:
        if loop is None:
            loop = asyncio.get_event_loop()

        return loop.run_until_complete(coroutine)
    except (
        grpclib.exceptions.GRPCError,
        grpclib.exceptions.StreamTerminatedError,
    ):
        raise ConnectionError from None
