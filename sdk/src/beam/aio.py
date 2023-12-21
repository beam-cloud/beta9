import asyncio
from asyncio import AbstractEventLoop
from typing import Any, Union


def run_sync(coroutine, loop: Union[AbstractEventLoop, None]) -> Any:
    if loop is None:
        loop = asyncio.get_event_loop()

    return loop.run_until_complete(coroutine)
