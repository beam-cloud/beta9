import asyncio
import importlib
import inspect
import json
import os
import sys
import time
import traceback
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Optional, Union

import requests
from starlette.responses import Response

from ..clients.gateway import (
    EndTaskRequest,
    GatewayServiceStub,
    SignPayloadRequest,
    SignPayloadResponse,
)
from ..exceptions import RunnerException

USER_CODE_VOLUME = "/mnt/code"


@dataclass
class Config:
    container_id: Optional[str]
    container_hostname: Optional[str]
    stub_id: Optional[str]
    stub_type: Optional[str]
    workers: Optional[int]
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
        workers = int(os.getenv("WORKERS", 1))
        keep_warm_seconds = float(os.getenv("KEEP_WARM_SECONDS", 10))
        python_version = os.getenv("PYTHON_VERSION")
        handler = os.getenv("HANDLER")
        on_start = os.getenv("ON_START")
        callback_url = os.getenv("CALLBACK_URL")
        task_id = os.getenv("TASK_ID")
        bind_port = int(os.getenv("BIND_PORT"))
        timeout = int(os.getenv("TIMEOUT", 180))

        if workers <= 0:
            workers = 1

        if not container_id or not stub_id:
            raise RunnerException("Invalid runner environment")

        return cls(
            container_id=container_id,
            container_hostname=container_hostname,
            stub_id=stub_id,
            stub_type=stub_type,
            workers=workers,
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
        self.handler: Optional[Callable] = None
        self.is_async: bool = False
        self._load()

    @contextmanager
    def importing_user_code(self):
        os.environ["BETA9_IMPORTING_USER_CODE"] = "true"
        yield
        del os.environ["BETA9_IMPORTING_USER_CODE"]

    def _load(self):
        if sys.path[0] != USER_CODE_VOLUME:
            sys.path.insert(0, USER_CODE_VOLUME)

        try:
            module, func = config.handler.split(":")

            with self.importing_user_code():
                target_module = importlib.import_module(module)

            self.handler = getattr(target_module, func)
            sig = inspect.signature(self.handler.func)
            self.pass_context = "context" in sig.parameters
            self.is_async = asyncio.iscoroutinefunction(self.handler.func)
        except BaseException:
            raise RunnerException()

    def __call__(self, context: FunctionContext, *args: Any, **kwargs: Any) -> Any:
        if self.handler is None:
            raise Exception("Handler not configured.")

        if self.pass_context:
            kwargs["context"] = context

        os.environ["TASK_ID"] = context.task_id or ""

        return self.handler(*args, **kwargs)


def execute_lifecycle_method(name: str) -> Union[Any, None]:
    """Executes a container lifecycle method defined by the user and return it's value"""

    if sys.path[0] != USER_CODE_VOLUME:
        sys.path.insert(0, USER_CODE_VOLUME)

    func: str = getattr(config, name)
    if func == "" or func is None:
        return None

    start_time = time.time()
    print(f"Running {name} func: {func}")
    try:
        module, func = func.split(":")
        target_module = importlib.import_module(module)
        method = getattr(target_module, func)
        result = method()
        duration = time.time() - start_time

        print(f"{name} func complete, took: {duration}s")
        return result
    except BaseException:
        raise RunnerException()


def end_task_and_send_callback(
    *,
    gateway_stub: GatewayServiceStub,
    payload: Any,
    end_task_request: EndTaskRequest,
    override_callback_url: Optional[str] = None,
):
    resp = gateway_stub.end_task(end_task_request)

    send_callback(
        gateway_stub=gateway_stub,
        context=FunctionContext.new(
            config=config,
            task_id=end_task_request.task_id,
            on_start_value=None,
        ),
        payload=payload,
        task_status=end_task_request.task_status,
        override_callback_url=override_callback_url,
    )

    return resp


def send_callback(
    *,
    gateway_stub: GatewayServiceStub,
    context: FunctionContext,
    payload: Any,
    task_status: str,
    override_callback_url: Optional[str] = None,
) -> None:
    """
    Send a signed callback request to an external host defined by the user
    """

    callback_url = override_callback_url or context.callback_url
    if not callback_url:
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
    sign_payload_resp: SignPayloadResponse = gateway_stub.sign_payload(
        SignPayloadRequest(payload=bytes(json.dumps(body), "utf-8"))
    )

    print(f"Sending data to callback: {callback_url}")
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
            requests.post(callback_url, json=body, headers=headers)
        else:
            requests.post(callback_url, data=body, headers=headers)

        print(f"Callback request took {time.time() - start} seconds")
    except BaseException:
        print(f"Unable to send callback: {traceback.format_exc()}")
