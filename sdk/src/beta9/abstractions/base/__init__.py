import asyncio
import inspect
import os
from abc import ABC
from asyncio import AbstractEventLoop
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Coroutine, Optional

from ... import config, terminal
from ...channel import Channel
from ...channel import get_channel as _get_channel
from ...config import ConfigContext, get_config_context, set_settings

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
        try:
            return self.loop.run_until_complete(coroutine)
        except Exception:
            terminal.error("Lost connection to gateway 🔌")

    def __init_subclass__(cls, /, **kwargs):
        """
        Dynamically load settings depending on if this library is being used
        by beta9 or beam. This is done by inspecting the first frame loaded
        onto the stack.
        """
        frames = inspect.stack()
        frame = frames[-1]

        if frame.code_context and any(
            substr in frame.code_context[0] for substr in ("import beam", "from beam")
        ):

            @dataclass
            class SDKSettings(config.SDKSettings):
                realtime_host: str = os.getenv("REALTIME_HOST", "wss://rt.beam.cloud")

            settings = SDKSettings(
                name="Beam",
                api_host=os.getenv("API_HOST", "api.beam.cloud"),
                gateway_host=os.getenv("GATEWAY_HOST", "gateway.beam.cloud"),
                gateway_port=int(os.getenv("GATEWAY_PORT", 443)),
                config_path=Path("~/.beam/config.ini").expanduser(),
            )
            set_settings(settings)

        super().__init_subclass__(**kwargs)
