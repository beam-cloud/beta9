import functools
import os
import sys
from contextlib import contextmanager
from typing import Any, Callable, NamedTuple

from grpclib.client import Channel


class GatewayConfig(NamedTuple):
    host: str = "0.0.0.0"
    port: int = 1993


def get_gateway_config() -> GatewayConfig:
    host = os.getenv("BEAM_GATEWAY_HOST", "0.0.0.0")
    port = os.getenv("BEAM_GATEWAY_PORT", 1993)

    # TODO: add token to this
    config = GatewayConfig(host=host, port=port)
    return config


def get_gateway_channel() -> Channel:
    config: GatewayConfig = get_gateway_config()
    return Channel(
        host=config.host,
        port=config.port,
        ssl=True if config.port == 443 else False,
    )


@contextmanager
def runner_context():
    exit_code = 0

    try:
        channel: Channel = get_gateway_channel()
        yield channel
    except SystemExit as exc:
        exit_code = exc.code
        raise
    finally:
        channel.close()

        if exit_code != 0:
            sys.exit(exit_code)


def with_runner_context(func: Callable) -> Callable:
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        with runner_context() as c:
            return func(*args, **kwargs, channel=c)

    return wrapper
