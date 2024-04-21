import importlib
import os
import sys
from dataclasses import dataclass
from typing import Callable, Optional, Union

from ..exceptions import RunnerException

USER_CODE_VOLUME = "/mnt/code"


@dataclass
class Config:
    container_id: Optional[str]
    container_hostname: Optional[str]
    stub_id: Optional[str]
    stub_type: Optional[str]
    concurrency: Optional[int]
    keep_warm_seconds: Optional[int]
    timeout: Optional[int]
    python_version: str
    handler: str
    loader: str
    task_id: Optional[str]
    bind_port: int

    @classmethod
    def load_from_env(cls) -> "Config":
        container_id = os.getenv("CONTAINER_ID")
        container_hostname = os.getenv("CONTAINER_HOSTNAME")
        stub_id = os.getenv("STUB_ID")
        stub_type = os.getenv("STUB_TYPE")
        concurrency = int(os.getenv("CONCURRENCY", 1))
        keep_warm_seconds = float(os.getenv("KEEP_WARM_SECONDS", 10))
        python_version = os.getenv("PYTHON_VERSION")
        handler = os.getenv("HANDLER")
        loader = os.getenv("LOADER")
        task_id = os.getenv("TASK_ID")
        bind_port = int(os.getenv("BIND_PORT"))
        timeout = int(os.getenv("TIMEOUT", 180))

        if concurrency <= 0:
            concurrency = 1

        if not container_id or not stub_id:
            raise RunnerException("Invalid runner environment")

        return cls(
            container_id=container_id,
            container_hostname=container_hostname,
            stub_id=stub_id,
            stub_type=stub_type,
            concurrency=concurrency,
            keep_warm_seconds=keep_warm_seconds,
            python_version=python_version,
            handler=handler,
            loader=loader,
            task_id=task_id,
            bind_port=bind_port,
            timeout=timeout,
        )


config: Config = Config.load_from_env()


def load_handler() -> Callable:
    sys.path.insert(0, USER_CODE_VOLUME)

    try:
        module, func = config.handler.split(":")
        target_module = importlib.import_module(module)
        method = getattr(target_module, func)
        print(f"Handler {config.handler} loaded.")
        return method
    except BaseException:
        raise RunnerException()


def load_loader() -> Union[Callable, None]:
    sys.path.insert(0, USER_CODE_VOLUME)

    if config.loader == "" or config.loader is None:
        return None

    try:
        module, func = config.loader.split(":")
        target_module = importlib.import_module(module)
        method = getattr(target_module, func)
        return method
    except BaseException:
        raise RunnerException()
