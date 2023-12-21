import asyncio
import os
from abc import ABC, abstractmethod
from typing import NamedTuple


class BaseAbstraction(ABC):
    def __init__(self) -> None:
        self.loop = asyncio.get_event_loop()

    def run_sync(self, coroutine):
        return self.loop.run_until_complete(coroutine)

    @abstractmethod
    def remote(self):
        raise NotImplementedError


class GatewayConfig(NamedTuple):
    host: str = "0.0.0.0"
    port: int = 1993


def get_gateway_config() -> GatewayConfig:
    host = os.getenv("BEAM_GATEWAY_HOST", "0.0.0.0")
    port = os.getenv("BEAM_GATEWAY_PORT", 1993)
    config = GatewayConfig(host=host, port=port)
    return config
