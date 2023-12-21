import asyncio
import os
from abc import ABC
from typing import NamedTuple


class BaseAbstraction(ABC):
    def __init__(self) -> None:
        self.loop = asyncio.get_event_loop()

    def run_sync(self, coroutine):
        return self.loop.run_until_complete(coroutine)


class GatewayConfig(NamedTuple):
    host: str = "0.0.0.0"
    port: int = 1993


def get_gateway_config() -> GatewayConfig:
    host = os.getenv("BEAM_GATEWAY_HOST", "0.0.0.0")
    port = os.getenv("BEAM_GATEWAY_PORT", 1993)
    # TODO: add token to this
    config = GatewayConfig(host=host, port=port)
    return config
