import asyncio
import json
import os
import signal
import time
from dataclasses import dataclass
from pathlib import Path
from threading import Thread
from typing import Any, Optional

import cloudpickle

from ..channel import Channel, get_channel, handle_error
from ..clients.function import (
    FunctionGetArgsRequest,
    FunctionMonitorRequest,
    FunctionServiceStub,
    FunctionSetResultRequest,
)
from ..clients.gateway import (
    EndTaskRequest,
    GatewayServiceStub,
    StartTaskRequest,
    StartTaskResponse,
)
from ..config import get_config_context
from ..exceptions import (
    FunctionSetResultError,
    InvalidFunctionArgumentsError,
    InvalidRunnerEnvironmentError,
    TaskEndError,
    TaskStartError,
)
from ..logging import json_output_interceptor
from ..runner.common import (
    monitor_task,
    FunctionContext,
    FunctionHandler,
    config,
    end_task_and_send_callback,
    serialize_result,
)
from ..type import TaskExitCode, TaskStatus


@dataclass
class InvokeResult:
    result: Optional[str] = None
    pickled_result: Optional[bytes] = None
    callback_url: Optional[str] = None
    exception: Optional[BaseException] = None


def _load_args(args: bytes) -> dict:
    try:
        return cloudpickle.loads(args)
    except BaseException:
        # If cloudpickle fails, fall back to JSON
        try:
            return json.loads(args.decode("utf-8"))
        except json.JSONDecodeError:
            raise InvalidFunctionArgumentsError


def _monitor_task(
    *,
    function_context: FunctionContext,
    runner_pid: int,
) -> None:
    with get_channel(get_config_context()) as channel:
        function_stub = FunctionServiceStub(channel)
        gateway_stub = GatewayServiceStub(channel)
        request = FunctionMonitorRequest(
            task_id=function_context.task_id,
            stub_id=function_context.stub_id,
            container_id=function_context.container_id,
        )
        monitor_task(
            function_context,
            lambda: function_stub.function_monitor(request),
            gateway_stub,
            lambda _: os.kill(runner_pid, signal.SIGTERM),
        )


def _handle_sigterm(*args: Any, **kwargs: Any) -> None:
    os._exit(TaskExitCode.Success)


def _handle_sigabort(*args: Any, **kwargs: Any) -> None:
    os._exit(TaskExitCode.Error)


@json_output_interceptor(task_id=config.task_id)
@handle_error()
def main():
    with get_channel(get_config_context()) as channel:
        run(channel)


def run(channel: Channel):
    function_stub: FunctionServiceStub = FunctionServiceStub(channel)
    gateway_stub: GatewayServiceStub = GatewayServiceStub(channel)
    task_id = config.task_id

    if not task_id:
        raise InvalidRunnerEnvironmentError

    container_id = config.container_id
    container_hostname = config.container_hostname

    # Start the task
    start_time = time.time()
    start_task_response = start_task(gateway_stub, task_id, container_id)
    if not start_task_response.ok:
        raise TaskStartError

    context = FunctionContext.new(config=config, task_id=task_id)

    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGABRT, _handle_sigabort)

    monitor_thread = Thread(
        target=_monitor_task,
        kwargs={
            "function_context": context,
            "runner_pid": os.getpid(),
        },
        daemon=True,
    )
    monitor_thread.start()

    # Invoke the function and handle its result
    result = asyncio.run(invoke_function(function_stub, context, task_id))
    if result.exception:
        handle_task_failure(gateway_stub, result, task_id, container_id, container_hostname)
        raise result.exception

    # End the task and send callback
    complete_task(
        gateway_stub,
        result,
        task_id,
        container_id,
        container_hostname,
        start_time,
    )


def start_task(
    gateway_stub: GatewayServiceStub, task_id: str, container_id: str
) -> StartTaskResponse:
    return gateway_stub.start_task(StartTaskRequest(task_id=task_id, container_id=container_id))


async def invoke_function(
    function_stub: FunctionServiceStub,
    context: FunctionContext,
    task_id: str,
    handler: Optional[FunctionHandler] = None,
) -> InvokeResult:
    result: Any = None
    callback_url = None
    pickled_result = None

    try:
        if handler is None:
            handler = FunctionHandler()

        get_args_resp = function_stub.function_get_args(FunctionGetArgsRequest(task_id=task_id))
        if not get_args_resp.ok:
            raise InvalidFunctionArgumentsError

        payload = _load_args(get_args_resp.args)
        args = payload.get("args") or []
        kwargs = payload.get("kwargs") or {}
        callback_url = kwargs.pop("callback_url", None)

        if ready_path := os.getenv("BETA9_RUNNER_READY_PATH"):
            try:
                Path(ready_path).touch()
            except OSError as exc:
                print(f"Unable to signal runner readiness: {exc}")

        if handler.is_async:
            result = await handler.__acall__(
                context,
                *args,
                **kwargs,
            )
        else:
            result = handler(
                context,
                *args,
                **kwargs,
            )
        pickled_result = cloudpickle.dumps(result)
        set_result_resp = function_stub.function_set_result(
            FunctionSetResultRequest(task_id=task_id, result=pickled_result)
        )
        if not set_result_resp.ok:
            raise FunctionSetResultError

        return InvokeResult(
            result=result,
            pickled_result=pickled_result,
            callback_url=callback_url,
        )
    except BaseException as e:
        return InvokeResult(
            result=result,
            pickled_result=pickled_result,
            exception=e,
            callback_url=callback_url,
        )


def complete_task(
    gateway_stub: GatewayServiceStub,
    result: InvokeResult,
    task_id: str,
    container_id: str,
    container_hostname: str,
    start_time: float,
):
    task_status = TaskStatus.Complete
    task_duration = time.time() - start_time
    keep_warm_seconds = 0

    end_task_response = end_task_and_send_callback(
        gateway_stub=gateway_stub,
        payload=result.result,
        end_task_request=EndTaskRequest(
            task_id=task_id,
            task_duration=task_duration,
            task_status=task_status,
            container_id=container_id,
            container_hostname=container_hostname,
            keep_warm_seconds=keep_warm_seconds,
            result=result.pickled_result,
        ),
        override_callback_url=result.callback_url,
    )

    if not end_task_response.ok:
        raise TaskEndError


def handle_task_failure(
    gateway_stub: GatewayServiceStub,
    result: InvokeResult,
    task_id: str,
    container_id: str,
    container_hostname: str,
):
    payload = {"error": str(result.exception)}
    task_status = TaskStatus.Error
    task_duration = 0
    keep_warm_seconds = 0

    end_task_and_send_callback(
        gateway_stub=gateway_stub,
        payload=payload,
        end_task_request=EndTaskRequest(
            task_id=task_id,
            task_duration=task_duration,
            task_status=task_status,
            container_id=container_id,
            container_hostname=container_hostname,
            keep_warm_seconds=keep_warm_seconds,
            result=serialize_result(payload),
        ),
        override_callback_url=result.callback_url,
    )


if __name__ == "__main__":
    main()
