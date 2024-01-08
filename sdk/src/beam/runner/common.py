import importlib
import os
import sys
from dataclasses import dataclass
from typing import Callable, Optional

from beam.exceptions import RunnerException

USER_CODE_VOLUME = "/mnt/code"


@dataclass
class Config:
    container_id: Optional[str]
    container_hostname: Optional[str]
    stub_id: Optional[str]
    concurrency: Optional[int]
    scale_down_delay: Optional[float]
    handler: str

    @classmethod
    def load_from_env(cls) -> "Config":
        container_id = os.getenv("CONTAINER_ID")
        container_hostname = os.getenv("CONTAINER_HOSTNAME")
        stub_id = os.getenv("STUB_ID")
        concurrency = int(os.getenv("CONCURRENCY", 1))
        scale_down_delay = float(os.getenv("SCALE_DOWN_DELAY", 10))

        if concurrency <= 0:
            concurrency = 1

        if not container_id or not stub_id:
            raise RunnerException("Invalid runner environment")

        handler = os.getenv("HANDLER")
        if not handler:
            raise RunnerException("Invalid handler")

        return cls(
            container_id=container_id,
            container_hostname=container_hostname,
            stub_id=stub_id,
            concurrency=concurrency,
            scale_down_delay=scale_down_delay,
            handler=handler,
        )


config: Config = Config.load_from_env()


def load_handler() -> Callable:
    sys.path.insert(0, USER_CODE_VOLUME)

    try:
        module, func = config.handler.split(":")
        target_module = importlib.import_module(module)
        method = getattr(target_module, func)
        return method
    except BaseException:
        raise RunnerException()
