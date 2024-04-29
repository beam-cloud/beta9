import importlib
import inspect
import json
import os
import sys
import time
import traceback
from dataclasses import dataclass
from typing import Any, Callable, Optional, Union

import requests
from starlette.responses import Response

from ..clients.gateway import GatewayServiceStub, SignPayloadRequest, SignPayloadResponse
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
    callback_url: Optional[str]
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
        callback_url = os.getenv("CALLBACK_URL")
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
            callback_url=callback_url,
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
    callback_url: Optional[str] = None
    task_id: Optional[str] = None
    timeout: Optional[int] = None
    on_start_value: Optional[Any] = None
    bind_port: int = 0
    python_version: str = ""

    @classmethod
    def new(
        cls,
        *,
        config: Config,
        task_id: str,
        on_start_value: Optional[Any] = None,
    ) -> "FunctionContext":
        """
        Create a new instance of FunctionContext, to be passed directly into a function handler
        """
        return cls(
            container_id=config.container_id,
            stub_id=config.stub_id,
            stub_type=config.stub_type,
            callback_url=config.callback_url,
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

    def _load(self):
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


async def send_callback(
    *, gateway_stub: GatewayServiceStub, context: FunctionContext, payload: Any, task_status: str
) -> None:
    """
    Send a signed callback request to an external host defined by the user
    """
    if context.callback_url == "" or context.callback_url is None:
        return

    body = {}
    headers = {}

    # Serialize callback payload to correct format
    use_json = True
    body = {"data": payload}
    if isinstance(payload, Response):
        body = {"data": payload.body}
        headers = payload.headers
        use_json = False

    # Sign callback payload
    sign_payload_resp: SignPayloadResponse = await gateway_stub.sign_payload(
        SignPayloadRequest(payload=bytes(json.dumps(body), "utf-8"))
    )

    print(f"Sending data to callback: {context.callback_url}")
    headers = {}
    headers = {
        **headers,
        "X-Task-ID": str(context.task_id),
        "X-Task-Status": str(task_status),
        "X-Task-Signature": sign_payload_resp.signature,
        "X-Task-Timestamp": str(sign_payload_resp.timestamp),
    }

    try:
        start = time.time()
        if use_json:
            requests.post(context.callback_url, json=body, headers=headers)
        else:
            requests.post(context.callback_url, data=body, headers=headers)

        print(f"Callback request took {time.time() - start} seconds")
    except BaseException:
        print(f"Unable to send callback: {traceback.format_exc()}")
