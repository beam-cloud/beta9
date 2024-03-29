import importlib
import os
import sys
from dataclasses import dataclass
from typing import Callable, Optional

from ..exceptions import RunnerException

USER_CODE_VOLUME = "/mnt/code"


@dataclass
class Config:
    container_id: Optional[str]
    container_hostname: Optional[str]
    stub_id: Optional[str]
    concurrency: Optional[int]
    keep_warm_seconds: Optional[int]
    handler: str
    task_id: Optional[str]

    @classmethod
    def load_from_env(cls) -> "Config":
        container_id = os.getenv("CONTAINER_ID")
        container_hostname = os.getenv("CONTAINER_HOSTNAME")
        stub_id = os.getenv("STUB_ID")
        concurrency = int(os.getenv("CONCURRENCY", 1))
        keep_warm_seconds = float(os.getenv("KEEP_WARM_SECONDS", 10))
        handler = os.getenv("HANDLER")
        task_id = os.getenv("TASK_ID")

        if concurrency <= 0:
            concurrency = 1

        if not container_id or not stub_id:
            raise RunnerException("Invalid runner environment")

        return cls(
            container_id=container_id,
            container_hostname=container_hostname,
            stub_id=stub_id,
            concurrency=concurrency,
            keep_warm_seconds=keep_warm_seconds,
            handler=handler,
            task_id=task_id,
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
