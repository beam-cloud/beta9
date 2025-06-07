import asyncio
import importlib
import inspect
import json
import os
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from functools import wraps
from multiprocessing import Value
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Union

import cloudpickle
import requests
from starlette.responses import Response

from ..clients.gateway import (
    EndTaskRequest,
    EndTaskResponse,
    GatewayServiceStub,
    SignPayloadRequest,
    SignPayloadResponse,
)
from ..env import is_remote
from ..exceptions import RunnerException
from ..schema import Schema, ValidationError

USER_CODE_DIR = "/mnt/code"
USER_VOLUMES_DIR = "/volumes"
USER_OUTPUTS_DIR = "/outputs"
USER_CACHE_DIR = "/cache"

PICKLE_SUFFIX = ".pkl"


@dataclass
class Config:
    container_id: str
    container_hostname: str
    stub_id: str
    stub_type: str
    workers: int
    keep_warm_seconds: int
    timeout: int
    python_version: str
    handler: str
    on_start: str
    callback_url: str
    task_id: str
    bind_port: int
    checkpoint_enabled: bool
    volume_cache_map: Dict
    inputs: Dict
    outputs: Dict

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
        checkpoint_enabled = os.getenv("CHECKPOINT_ENABLED", "false").lower() == "true"
        volume_cache_map = json.loads(os.getenv("VOLUME_CACHE_MAP", "{}"))
        inputs = json.loads(os.getenv("BETA9_INPUTS", "{}"))
        outputs = json.loads(os.getenv("BETA9_OUTPUTS", "{}"))

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
            checkpoint_enabled=checkpoint_enabled,
            volume_cache_map=volume_cache_map,
            inputs=inputs,
            outputs=outputs,
        )


config: Union[Config, None] = None
if is_remote():
    config: Config = Config.load_from_env()


class ParentAbstractionProxy:
    """
    Class to allow handlers to access parent class variables through attribute or dictionary access
    """

    def __init__(self, parent):
        self._parent = parent

    def __getitem__(self, key):
        return getattr(self._parent, key)

    def __setitem__(self, key, value):
        setattr(self._parent, key, value)

    def __getattr__(self, key):
        return getattr(self._parent, key)

    def __setattr__(self, key, value):
        if key == "_parent":
            super().__setattr__(key, value)
        else:
            setattr(self._parent, key, value)


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
        task_id: Optional[str],
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


workers_ready = None
if is_remote():
    workers_ready = Value("i", 0)


class FunctionHandler:
    """
    Helper class for loading user entry point functions
    """

    def __init__(self, handler_path: Optional[str] = None) -> None:
        self.pass_context: bool = False
        self.handler_path: Optional[str] = handler_path
        self.handler: Optional[Callable] = None
        self.is_async: bool = False
        self.inputs: Optional[Schema] = None
        self.outputs: Optional[Schema] = None
        self._load()

    @contextmanager
    def importing_user_code(self):
        os.environ["BETA9_IMPORTING_USER_CODE"] = "true"
        yield
        del os.environ["BETA9_IMPORTING_USER_CODE"]

    def _load_pickled_function(self, module_path: str) -> Callable:
        """Load a pickled function using cloudpickle."""
        try:
            with open(module_path, "rb") as f:
                func = cloudpickle.load(f)
                if not callable(func):
                    raise RunnerException("Loaded object is not callable")
                return func
        except RunnerException:
            raise
        except BaseException as e:
            raise RunnerException(
                f"Failed to load pickled function: {traceback.format_exc()}"
            ) from e

    def _load(self):
        if sys.path[0] != USER_CODE_DIR:
            sys.path.insert(0, USER_CODE_DIR)

        if config.inputs:
            self.inputs = Schema.from_dict(config.inputs)

        if config.outputs:
            self.outputs = Schema.from_dict(config.outputs)

        try:
            module = None
            func = None

            if self.handler_path is not None:
                module, func = self.handler_path.split(":")
            else:
                module, func = config.handler.split(":")

            with self.importing_user_code():
                if Path(module).suffix == PICKLE_SUFFIX:
                    # Handle pickled functions
                    self.handler = self._load_pickled_function(module)
                else:
                    # Handle standard modules (i.e. .py files)
                    target_module = importlib.import_module(module)
                    self.handler = getattr(target_module, func)

            # Check if handler is a wrapped function or direct function
            target_func = getattr(self.handler, "func", self.handler)
            self.signature = inspect.signature(target_func)
            self.pass_context = "context" in self.signature.parameters
            self.is_async = asyncio.iscoroutinefunction(target_func)
        except BaseException:
            raise RunnerException(f"Error loading handler: {traceback.format_exc()}")

    def __call__(self, context: FunctionContext, *args: Any, **kwargs: Any) -> Any:
        if self.handler is None:
            raise Exception("Handler not configured.")

        handler_args = args
        handler_kwargs = kwargs

        if self.inputs is not None:
            if len(kwargs) == 1:
                key, value = next(iter(kwargs.items()))
                if isinstance(value, dict):
                    input_data = value
                else:
                    # Wrap the value in a dict with the expected field name
                    input_data = {key: value}
            else:
                input_data = kwargs

            try:
                parsed_inputs = self.inputs.new(input_data)
            except ValidationError as e:
                print(f"Input validation error: {e}")
                return e.to_dict()

            handler_args = (parsed_inputs,)
            handler_kwargs = {}

        if self.pass_context:
            handler_kwargs["context"] = context

        os.environ["TASK_ID"] = context.task_id or ""
        result = self.handler(*handler_args, **handler_kwargs)

        if self.outputs is not None:
            if result is None:
                result = {}

            try:
                parsed_outputs = self.outputs.new(result)
            except ValidationError as e:
                print(f"Output validation error: {e}")
                return e.to_dict()

            return parsed_outputs.dump()

        return result

    @property
    def parent_abstraction(self) -> ParentAbstractionProxy:
        if not hasattr(self, "_parent_abstraction"):
            self._parent_abstraction = ParentAbstractionProxy(self.handler.parent)
        return self._parent_abstraction


def execute_lifecycle_method(name: str) -> Union[Any, None]:
    """Executes a container lifecycle method defined by the user and return it's value"""

    if sys.path[0] != USER_CODE_DIR:
        sys.path.insert(0, USER_CODE_DIR)

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


# TODO: add retry behavior directly in dynamically generated GRPC stubs
def retry_grpc_call(
    *, exception_to_check: Exception, tries: int = 4, delay: int = 5, backoff: int = 2
) -> Any:
    def _retry_decorator(f):
        @wraps(f)
        def f_to_retry(*args, **kwargs):
            mtries, mdelay = tries, delay

            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except exception_to_check:
                    print(f"Unexpected GRPC error, retrying in {mdelay} seconds...")
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff

            return f(*args, **kwargs)

        return f_to_retry

    return _retry_decorator


@retry_grpc_call(exception_to_check=BaseException, tries=4, delay=5, backoff=2)
def end_task_and_send_callback(
    *,
    gateway_stub: GatewayServiceStub,
    payload: Any,
    end_task_request: EndTaskRequest,
    override_callback_url: Optional[str] = None,
) -> EndTaskResponse:
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


def serialize_result(result: Any) -> bytes:
    try:
        return json.dumps(result).encode("utf-8")
    except Exception:
        print(f"Warning - Error serializing task result: {traceback.format_exc()}")
        return None


def has_asgi3_signature(func) -> bool:
    sig = inspect.signature(func)
    own_parameters = {name for name in sig.parameters if name != "self"}
    return own_parameters == {"scope", "receive", "send"}


def is_asgi3(app: Any) -> bool:
    """Return whether 'app' corresponds to an ASGI3 callable."""
    if inspect.isclass(app):
        constructor = app.__init__
        return has_asgi3_signature(constructor) and hasattr(app, "__await__")

    if inspect.isfunction(app):
        return inspect.iscoroutinefunction(app) and has_asgi3_signature(app)

    try:
        call = app.__call__
    except AttributeError:
        return False
    else:
        return inspect.iscoroutinefunction(call) and has_asgi3_signature(call)


class ThreadPoolExecutorOverride(ThreadPoolExecutor):
    def __exit__(self, *_, **__):
        try:
            # cancel_futures added in 3.9
            self.shutdown(cancel_futures=True)
        except Exception:
            pass


CHECKPOINT_SIGNAL_FILE = "/criu/READY_FOR_CHECKPOINT"
CHECKPOINT_COMPLETE_FILE = "/criu/CHECKPOINT_COMPLETE"
CHECKPOINT_CONTAINER_ID_FILE = "/criu/CONTAINER_ID"
CHECKPOINT_CONTAINER_HOSTNAME_FILE = "/criu/CONTAINER_HOSTNAME"


def wait_for_checkpoint():
    def _reload_config():
        # Once we have set the checkpoint signal file, wait for checkpoint to be complete before reloading the config
        while not Path(CHECKPOINT_COMPLETE_FILE).exists():
            time.sleep(1)

        # Reload config that may have changed during restore
        config.container_id = Path(CHECKPOINT_CONTAINER_ID_FILE).read_text()
        config.container_hostname = Path(CHECKPOINT_CONTAINER_HOSTNAME_FILE).read_text()

    with workers_ready.get_lock():
        workers_ready.value += 1

    if workers_ready.value == config.workers:
        Path(CHECKPOINT_SIGNAL_FILE).touch(exist_ok=True)
        return _reload_config()

    while True:
        with workers_ready.get_lock():
            if workers_ready.value == config.workers:
                break
        time.sleep(1)

    return _reload_config()
