import os

os.environ["GRPC_VERBOSITY"] = os.getenv("GRPC_VERBOSITY") or "NONE"

import sys
from abc import ABC
from typing import Optional

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
    If neither is provided, this uses the default context and will create a
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


def unset_channel():
    global _channel
    _channel = None


def get_channel() -> Channel:
    global _channel

    if not _channel:
        set_channel()

    return _channel  # type: ignore


class BaseAbstraction(ABC):
    @property
    def channel(self) -> Channel:
        return get_channel()

    def __init_subclass__(cls, /, **kwargs):
        """
        Dynamically load settings depending on if this library is being used
        by beta9 or beam.
        """
        if "beam" in sys.modules:
            # Settings will be configured in SDKSettings.__post_init__
            set_settings()

        super().__init_subclass__(**kwargs)
