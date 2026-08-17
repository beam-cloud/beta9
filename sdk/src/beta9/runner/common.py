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
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple, Union

import cloudpickle

from ..clients.gateway import (
    EndTaskRequest,
    EndTaskResponse,
    GatewayServiceStub,
    SignPayloadRequest,
    SignPayloadResponse,
)
from ..env import is_remote
from ..exceptions import RunnerException

USER_CODE_DIR = "/mnt/code"
USER_VOLUMES_DIR = "/volumes"
USER_OUTPUTS_DIR = "/outputs"
USER_CACHE_DIR = "/cache"
LIFECYCLE_RPC_TIMEOUT_SECONDS = 5
END_TASK_MAX_ATTEMPTS = 3
END_TASK_RETRY_DELAY_SECONDS = 1
CALLBACK_HTTP_CONNECT_TIMEOUT_SECONDS = 10
CALLBACK_HTTP_READ_TIMEOUT_SECONDS = 30
CALLBACK_MAX_ATTEMPTS = 3
CALLBACK_RETRY_INITIAL_DELAY_SECONDS = 0.5

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


class FunctionHandler:
    """
    Helper class for loading user entry point functions
    """

    def __init__(self, handler_path: Optional[str] = None) -> None:
        self.pass_context: bool = False
        self.handler_path: Optional[str] = handler_path
        self.handler: Optional[Callable] = None
        self.is_async: bool = False
        self.inputs: Optional[Any] = None
        self.outputs: Optional[Any] = None
        self.validation_error = ()
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

        if config.inputs or config.outputs:
            from ..schema import Schema, ValidationError

            self.validation_error = ValidationError

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

    def _prepare_handler_call(
        self, context: FunctionContext, *args: Any, **kwargs: Any
    ) -> Tuple[Tuple, Dict]:
        """Prepare handler arguments and kwargs, handling input validation and context injection."""

        if self.handler is None:
            raise Exception("Handler not configured.")

        handler_args = args
        handler_kwargs = kwargs

        if self.inputs is not None:
            if len(kwargs) == 1:
                key, value = next(iter(kwargs.items()))
                input_data = value if isinstance(value, dict) else {key: value}
            else:
                input_data = kwargs

            parsed_inputs = self.inputs.new(input_data)
            handler_args = (parsed_inputs,)
            handler_kwargs = {}

        if self.pass_context:
            handler_kwargs["context"] = context

        os.environ["TASK_ID"] = context.task_id or ""
        return handler_args, handler_kwargs

    def _process_result(self, result: Any) -> Any:
        """Process and validate the handler result."""

        if self.outputs is not None:
            if result is None:
                result = {}

            try:
                parsed_outputs = self.outputs.new(result)
            except self.validation_error as e:
                print(f"Output validation error: {e}")
                return e.to_dict()

            return parsed_outputs.dump()

        return result

    def __call__(self, context: FunctionContext, *args: Any, **kwargs: Any) -> Any:
        try:
            handler_args, handler_kwargs = self._prepare_handler_call(context, *args, **kwargs)
        except self.validation_error as e:
            print(f"Input validation error: {e}")
            return e.to_dict()

        result = self.handler(*handler_args, **handler_kwargs)
        return self._process_result(result)

    async def __acall__(self, context: FunctionContext, *args: Any, **kwargs: Any) -> Any:
        try:
            handler_args, handler_kwargs = self._prepare_handler_call(context, *args, **kwargs)
        except self.validation_error as e:
            print(f"Input validation error: {e}")
            return e.to_dict()

        result = await self.handler(*handler_args, **handler_kwargs)
        return self._process_result(result)

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


async def execute_lifecycle_method_async(name: str) -> Union[Any, None]:
    """Async version of execute_lifecycle_method for use in async contexts"""

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

        if asyncio.iscoroutinefunction(method):
            result = await method()
        else:
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(None, method)

        duration = time.time() - start_time

        print(f"{name} func complete, took: {duration}s")
        return result
    except BaseException:
        raise RunnerException()


def _call_gateway_unary(
    gateway_stub: GatewayServiceStub,
    route: str,
    request_type,
    response_type,
    request,
    fallback,
):
    unary_unary = getattr(gateway_stub, "_unary_unary", None)
    if not callable(unary_unary):
        return fallback(request)
    return unary_unary(route, request_type, response_type)(
        request, timeout=LIFECYCLE_RPC_TIMEOUT_SECONDS
    )


def _end_task(gateway_stub: GatewayServiceStub, request: EndTaskRequest) -> EndTaskResponse:
    last_error: Optional[Exception] = None
    for attempt in range(END_TASK_MAX_ATTEMPTS):
        try:
            response = _call_gateway_unary(
                gateway_stub,
                "/gateway.GatewayService/EndTask",
                EndTaskRequest,
                EndTaskResponse,
                request,
                gateway_stub.end_task,
            )
            if response.ok:
                return response
            last_error = RuntimeError("Gateway rejected task completion")
        except Exception as exc:
            last_error = exc

        if attempt < END_TASK_MAX_ATTEMPTS - 1:
            delay = END_TASK_RETRY_DELAY_SECONDS * (2**attempt)
            print(f"Failed to finalize task <{request.task_id}>; retrying in {delay}s")
            time.sleep(delay)

    assert last_error is not None
    raise last_error


def end_task(gateway_stub: GatewayServiceStub, request: EndTaskRequest) -> EndTaskResponse:
    return _end_task(gateway_stub, request)


def send_task_callback(
    *,
    gateway_stub: GatewayServiceStub,
    payload: Any,
    end_task_request: EndTaskRequest,
    override_callback_url: Optional[str] = None,
) -> None:
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


def end_task_and_send_callback(
    *,
    gateway_stub: GatewayServiceStub,
    payload: Any,
    end_task_request: EndTaskRequest,
    override_callback_url: Optional[str] = None,
) -> EndTaskResponse:
    end_task_error: Optional[Exception] = None
    try:
        resp = end_task(gateway_stub, end_task_request)
    except Exception as exc:
        # Callback delivery is independent of gateway task finalization. User
        # code has already completed, so an ambiguous or transient EndTask
        # failure must not silently suppress its callback.
        end_task_error = exc
        resp = None

    send_task_callback(
        gateway_stub=gateway_stub,
        payload=payload,
        end_task_request=end_task_request,
        override_callback_url=override_callback_url,
    )

    if end_task_error is not None:
        raise end_task_error
    assert resp is not None
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

    import requests
    from starlette.responses import Response

    body = {}
    headers = {}

    # Serialize callback payload to correct format
    use_json = True
    body = {"data": payload}
    if isinstance(payload, Response):
        body = {"data": payload.body}
        headers = payload.headers
        use_json = False

    try:
        sign_request = SignPayloadRequest(payload=bytes(json.dumps(body), "utf-8"))
        sign_payload_resp: SignPayloadResponse = _call_gateway_unary(
            gateway_stub,
            "/gateway.GatewayService/SignPayload",
            SignPayloadRequest,
            SignPayloadResponse,
            sign_request,
            gateway_stub.sign_payload,
        )

        print(f"Sending data to callback: {callback_url}")
        headers = {
            **headers,
            "X-Task-ID": str(context.task_id),
            "X-Task-Status": str(task_status),
            "X-Task-Signature": sign_payload_resp.signature,
            "X-Task-Timestamp": str(sign_payload_resp.timestamp),
        }
        request_body = {"json": body} if use_json else {"data": body}

        for attempt in range(1, CALLBACK_MAX_ATTEMPTS + 1):
            start = time.time()
            response = None
            try:
                response = requests.post(
                    callback_url,
                    **request_body,
                    headers=headers,
                    timeout=(
                        CALLBACK_HTTP_CONNECT_TIMEOUT_SECONDS,
                        CALLBACK_HTTP_READ_TIMEOUT_SECONDS,
                    ),
                )
                response.raise_for_status()
                print(
                    f"Callback request attempt {attempt}/{CALLBACK_MAX_ATTEMPTS} "
                    f"took {time.time() - start} seconds"
                )
                return
            except requests.ReadTimeout:
                # A read timeout is ambiguous: the receiver may have committed
                # the callback while its response was lost. Do not duplicate it.
                print(
                    f"Callback delivery status unknown for task <{context.task_id}> after "
                    "read timeout; not retrying to avoid duplicates"
                )
                return
            except requests.RequestException as exc:
                status_code = response.status_code if response is not None else None
                retryable = (
                    isinstance(exc, requests.ConnectionError)
                    or status_code in (408, 425, 429)
                    or (status_code is not None and status_code >= 500)
                )
                if not retryable or attempt == CALLBACK_MAX_ATTEMPTS:
                    raise

                delay = CALLBACK_RETRY_INITIAL_DELAY_SECONDS * (2 ** (attempt - 1))
                print(
                    f"Callback request attempt {attempt}/{CALLBACK_MAX_ATTEMPTS} failed; "
                    f"retrying in {delay:g}s: {exc}"
                )
                time.sleep(delay)
    except Exception:
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
