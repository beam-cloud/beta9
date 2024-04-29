import asyncio
from abc import ABC
from asyncio import AbstractEventLoop
from typing import Any, Coroutine, Optional

from grpclib.client import Channel

from ...channel import get_channel as _get_channel
from ...config import (
    ConfigContext,
    get_config_context,
)

# Global channel
_channel: Optional[Channel] = None


def set_channel(
    channel: Optional[Channel] = None,
    context: Optional[ConfigContext] = None,
) -> None:
    """
    Sets the channel globally for the SDK.

    Use this before importing any abstraction to control which
    gateway to connect to. When you provide a channel, it should already be
    authenticated. When you provide a context, this will authenticate for you.
    If neithe are provided, this uses the default context and will create a
    channel. If there is no default context (or config file), then we will
    prompt the user for it.

    Args:
        channel: gRPC channel. Defaults to None.
        context: Config context that defines the channel credentials. Defaults to None.
    """
    global _channel

    if channel:
        _channel = channel
        return

    if context:
        _channel = _get_channel(context)
        return

    context = get_config_context()
    _channel = _get_channel(context)


def get_channel() -> Channel:
    global _channel

    if not _channel:
        set_channel()

    return _channel  # type: ignore


class BaseAbstraction(ABC):
    def __init__(self) -> None:
        self.loop: AbstractEventLoop = asyncio.get_event_loop()

    @property
    def channel(self) -> Channel:
        return get_channel()

    def run_sync(self, coroutine: Coroutine) -> Any:
        return self.loop.run_until_complete(coroutine)

    def __del__(self) -> None:
        if _channel:
            _channel.close()
