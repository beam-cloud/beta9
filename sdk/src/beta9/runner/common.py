import importlib
import inspect
import os
import sys
from dataclasses import dataclass
from typing import Any, Callable, Optional, Union

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
    on_start: Optional[str]
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
        on_start = os.getenv("ON_START")
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
            on_start=on_start,
            task_id=task_id,
            bind_port=bind_port,
            timeout=timeout,
        )


config: Config = Config.load_from_env()


@dataclass
class FunctionContext:
    """
    A dataclass used to store various useful fields you might want to access in your entry point logic
    """

    container_id: Optional[str] = None
    stub_id: Optional[str] = None
    stub_type: Optional[str] = None
    task_id: Optional[str] = None
    timeout: Optional[int] = None
    on_start_value: Optional[Any] = None
    bind_port: int = 0
    python_version: str = ""

    @classmethod
    def new(cls, *, config: Config, task_id: str, on_start_value: Any) -> "FunctionContext":
        """
        Create a new instance of FunctionContext, to be passed directly into a function handler
        """
        return cls(
            container_id=config.container_id,
            stub_id=config.stub_id,
            stub_type=config.stub_type,
            python_version=config.python_version,
            task_id=task_id,
            bind_port=config.bind_port,
            timeout=config.timeout,
            on_start_value=on_start_value,
        )


class FunctionHandler:
    """
    Helper class for loading user entry point functions
    """

    def __init__(self) -> None:
        self.pass_context: bool = False
        self.handler: Union[Callable, None] = None
        self._load()

    def _load(self) -> Callable:
        if sys.path[0] != USER_CODE_VOLUME:
            sys.path.insert(0, USER_CODE_VOLUME)

        try:
            module, func = config.handler.split(":")
            target_module = importlib.import_module(module)
            self.handler = getattr(target_module, func)
            sig = inspect.signature(self.handler.func)
            self.pass_context = "context" in sig.parameters
        except BaseException:
            raise RunnerException()

    def __call__(self, context: FunctionContext, *args: Any, **kwargs: Any) -> Any:
        if self.pass_context:
            kwargs["context"] = context

        return self.handler(*args, **kwargs)


def execute_lifecycle_method(*, name: str) -> Union[Any, None]:
    """Executes a container lifecycle method defined by the user and return it's value"""

    if sys.path[0] != USER_CODE_VOLUME:
        sys.path.insert(0, USER_CODE_VOLUME)

    func: str = getattr(config, name)
    if func == "" or func is None:
        return None

    print(f"Running {name} func: {func}")
    try:
        module, func = func.split(":")
        target_module = importlib.import_module(module)
        method = getattr(target_module, func)
        return method()
    except BaseException:
        raise RunnerException()
