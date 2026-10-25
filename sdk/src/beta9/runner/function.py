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
import grpc

from ..channel import RECOVERY_TIMEOUT, Channel, get_channel, handle_error, rpc_timeout
from ..clients.function import (
    FunctionGetArgsRequest,
    FunctionMonitorRequest,
    FunctionMonitorResponse,
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
    TaskMonitor,
    config,
    end_task_and_send_callback,
    send_callback,
    serialize_result,
)
from ..type import TaskExitCode, TaskStatus


@dataclass
class InvokeResult:
    result: Optional[str] = None
    pickled_result: Optional[bytes] = None
    callback_url: Optional[str] = None
    exception: Optional[BaseException] = None
    cancelled: bool = False


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
    monitor: Optional[TaskMonitor] = None,
) -> None:
    with get_channel(get_config_context()) as channel:
        function_stub = FunctionServiceStub(channel)
        gateway_stub = GatewayServiceStub(channel)
        request = FunctionMonitorRequest(
            task_id=function_context.task_id,
            stub_id=function_context.stub_id,
            container_id=function_context.container_id,
        )

        def open_stream():
            # The generated iterator hides the RPC's cancel method.
            unary_stream = getattr(function_stub, "_unary_stream", None)
            if callable(unary_stream):
                return unary_stream(
                    "/function.FunctionService/FunctionMonitor",
                    FunctionMonitorRequest,
                    FunctionMonitorResponse,
                )(request)
            return function_stub.function_monitor(request)

        monitor_task(
            function_context,
            open_stream,
            gateway_stub,
            lambda _: os.kill(runner_pid, signal.SIGTERM),
            monitor,
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
    try:
        start_task_response = start_task(gateway_stub, task_id, container_id)
    except grpc.RpcError as error:
        if error.code() == grpc.StatusCode.CANCELLED:
            return
        raise
    if not start_task_response.ok:
        raise TaskStartError

    context = FunctionContext.new(config=config, task_id=task_id)

    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGABRT, _handle_sigabort)

    monitor = TaskMonitor()
    monitor_thread = Thread(
        target=_monitor_task,
        kwargs={
            "function_context": context,
            "runner_pid": os.getpid(),
            "monitor": monitor,
        },
        daemon=True,
    )
    monitor_thread.start()

    # Invoke the function and handle its result
    try:
        result = asyncio.run(invoke_function(function_stub, context, task_id))
    finally:
        # Finalization owns liveness once execution ends.
        monitor.stop()
        monitor_thread.join()
    if result.cancelled:
        send_callback(
            gateway_stub=gateway_stub,
            context=context,
            payload={},
            task_status=TaskStatus.Cancelled,
            override_callback_url=result.callback_url,
        )
        return
    try:
        if result.exception:
            handle_task_failure(gateway_stub, result, task_id, container_id, container_hostname)
        else:
            complete_task(
                gateway_stub, result, task_id, container_id, container_hostname, start_time
            )
    except grpc.RpcError as error:
        if error.code() == grpc.StatusCode.CANCELLED:
            return
        raise
    if result.exception:
        raise result.exception


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
        try:
            with rpc_timeout(RECOVERY_TIMEOUT):
                set_result_resp = function_stub.function_set_result(
                    FunctionSetResultRequest(
                        task_id=task_id, result=pickled_result, container_id=context.container_id
                    )
                )
        except grpc.RpcError as error:
            if error.code() == grpc.StatusCode.CANCELLED:
                return InvokeResult(cancelled=True, callback_url=callback_url)
            raise
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
