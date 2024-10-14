import json
import os
import time
import traceback
from concurrent.futures import CancelledError, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Optional

import cloudpickle
import grpc

from ..channel import Channel, handle_error, pass_channel
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
from ..exceptions import (
    FunctionSetResultError,
    InvalidFunctionArgumentsError,
    InvalidRunnerEnvironmentError,
    TaskEndError,
    TaskStartError,
)
from ..logging import json_output_interceptor
from ..runner.common import (
    FunctionContext,
    FunctionHandler,
    config,
    end_task_and_send_callback,
    send_callback,
)
from ..type import TaskExitCode, TaskStatus


@dataclass
class InvokeResult:
    result: Optional[str] = None
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
    context: FunctionContext,
    stub_id: str,
    task_id: str,
    container_id: str,
    function_stub: FunctionServiceStub,
    gateway_stub: GatewayServiceStub,
) -> None:
    initial_backoff = 5
    max_retries = 5
    backoff = initial_backoff
    retry = 0

    while retry <= max_retries:
        try:
            for response in function_stub.function_monitor(
                FunctionMonitorRequest(
                    task_id=task_id,
                    stub_id=stub_id,
                    container_id=container_id,
                )
            ):
                response: FunctionMonitorResponse
                if response.cancelled:
                    print(f"Task cancelled: {task_id}")

                    send_callback(
                        gateway_stub=gateway_stub,
                        context=context,
                        payload={},
                        task_status=TaskStatus.Cancelled,
                    )
                    os._exit(TaskExitCode.Cancelled)

                if response.complete:
                    return

                if response.timed_out:
                    print(f"Task timed out: {task_id}")

                    send_callback(
                        gateway_stub=gateway_stub,
                        context=context,
                        payload={},
                        task_status=TaskStatus.Timeout,
                    )
                    os._exit(TaskExitCode.Timeout)

                retry = 0
                backoff = initial_backoff

            # If successful, it means the stream is finished.
            # Break out of the retry loop
            break

        except (
            grpc.RpcError,
            ConnectionRefusedError,
        ):
            if retry == max_retries:
                print("Lost connection to task monitor, exiting")
                os._exit(0)

            time.sleep(backoff)
            backoff *= 2
            retry += 1

        except (CancelledError, ValueError):
            return

        except BaseException:
            print(f"Unexpected error occurred in task monitor: {traceback.format_exc()}")
            os._exit(0)


@json_output_interceptor(task_id=config.task_id)
@handle_error(print_traceback=False)
@pass_channel
def main(channel: Channel):
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

    # Start monitoring the task
    thread_pool = ThreadPoolExecutor()
    thread_pool.submit(
        _monitor_task,
        context=context,
        stub_id=config.stub_id,
        task_id=task_id,
        container_id=container_id,
        function_stub=function_stub,
        gateway_stub=gateway_stub,
    )

    # Invoke the function and handle its result
    result = invoke_function(function_stub, context, task_id)
    if result.exception:
        thread_pool.shutdown(wait=False)
        handle_task_failure(gateway_stub, result, task_id, container_id, container_hostname)
        print(result.exception)
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

    thread_pool.shutdown(wait=False)


def start_task(
    gateway_stub: GatewayServiceStub, task_id: str, container_id: str
) -> StartTaskResponse:
    return gateway_stub.start_task(StartTaskRequest(task_id=task_id, container_id=container_id))


def invoke_function(
    function_stub: FunctionServiceStub, context: FunctionContext, task_id: str
) -> InvokeResult:
    result: Any = None
    callback_url = None

    try:
        get_args_resp = function_stub.function_get_args(FunctionGetArgsRequest(task_id=task_id))
        if not get_args_resp.ok:
            raise InvalidFunctionArgumentsError

        payload = _load_args(get_args_resp.args)
        args = payload.get("args") or []
        kwargs = payload.get("kwargs") or {}
        callback_url = kwargs.pop("callback_url", None)

        handler = FunctionHandler()
        result = handler(context, *args, **kwargs)

        pickled_result = cloudpickle.dumps(result)
        set_result_resp = function_stub.function_set_result(
            FunctionSetResultRequest(task_id=task_id, result=pickled_result)
        )
        if not set_result_resp.ok:
            raise FunctionSetResultError

        return InvokeResult(
            result=result,
            callback_url=callback_url,
        )
    except BaseException as e:
        return InvokeResult(
            result=result,
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
        ),
        override_callback_url=result.callback_url,
    )


if __name__ == "__main__":
    main()
