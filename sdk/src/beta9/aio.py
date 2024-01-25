import asyncio
from asyncio import AbstractEventLoop
from typing import Any, Coroutine, Union


def run_sync(coroutine: Coroutine, loop: Union[AbstractEventLoop, None] = None) -> Any:
    if loop is None:
        loop = asyncio.get_event_loop()

    return loop.run_until_complete(coroutine)
