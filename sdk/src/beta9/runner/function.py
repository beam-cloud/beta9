import json
import os
import signal
import time
import traceback
from dataclasses import dataclass
from multiprocessing import Process
from typing import Any, Optional

import cloudpickle
import grpc

from ..channel import Channel, get_channel, handle_error, pass_channel
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
    FunctionContext,
    FunctionHandler,
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
    env: dict,
) -> None:
    os.environ.update(env)
    config = get_config_context()
    parent_pid = os.getppid()

    def _monitor_stream() -> bool:
        """
        Returns True if the stream ended with no errors (and should be restarted),
        or False if a exit event occurred (cancellation, completion, timeout,
        or a connection issue that caused us to kill the parent process)
        """
        with get_channel(config) as channel:
            function_stub = FunctionServiceStub(channel)
            gateway_stub = GatewayServiceStub(channel)

            initial_backoff = 5
            max_retries = 5
            backoff = initial_backoff
            retry = 0

            while retry <= max_retries:
                try:
                    for response in function_stub.function_monitor(
                        FunctionMonitorRequest(
                            task_id=function_context.task_id,
                            stub_id=function_context.stub_id,
                            container_id=function_context.container_id,
                        )
                    ):
                        # If the task is cancelled then send a callback and exit
                        if response.cancelled:
                            print(f"Task cancelled: {function_context.task_id}")
                            send_callback(
                                gateway_stub=gateway_stub,
                                context=function_context,
                                payload={},
                                task_status=TaskStatus.Cancelled,
                            )
                            os.kill(parent_pid, signal.SIGTERM)
                            return False

                        # If the task is complete, exit
                        if response.complete:
                            return False

                        # If the task has timed out, send a timeout callback and exit
                        if response.timed_out:
                            print(f"Task timed out: {function_context.task_id}")
                            send_callback(
                                gateway_stub=gateway_stub,
                                context=function_context,
                                payload={},
                                task_status=TaskStatus.Timeout,
                            )
                            os.kill(parent_pid, signal.SIGTERM)
                            return False

                        # Reset retry state if a valid response was received
                        retry = 0
                        backoff = initial_backoff

                    # Reaching here means that the stream ended with no errors,
                    # which can occur during a rollout restart of the gateway
                    # returning True here tells the outer loop to restart the stream
                    return True

                except (grpc.RpcError, ConnectionRefusedError):
                    if retry == max_retries:
                        print("Lost connection to task monitor, exiting")
                        os.kill(parent_pid, signal.SIGABRT)
                        return False

                    time.sleep(backoff)
                    backoff *= 2
                    retry += 1

                except BaseException:
                    print(f"Unexpected error occurred in task monitor: {traceback.format_exc()}")
                    os.kill(parent_pid, signal.SIGABRT)
                    return False

    # Outer loop: restart only if the stream ended with no errors
    while True:
        should_restart = _monitor_stream()
        if not should_restart:
            # Exit condition encountered; exit the monitor task completely
            return

        # If we reached here, the stream ended with no errors;
        # so we should restart the monitoring stream


def _handle_sigterm(*args: Any, **kwargs: Any) -> None:
    os._exit(TaskExitCode.Success)


def _handle_sigabort(*args: Any, **kwargs: Any) -> None:
    os._exit(TaskExitCode.Error)


@json_output_interceptor(task_id=config.task_id)
@handle_error()
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

    # Start monitor_task process to send health checks
    env = os.environ.copy()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGABRT, _handle_sigabort)

    monitor_process = Process(
        target=_monitor_task,
        kwargs={
            "function_context": context,
            "env": env,
        },
        daemon=True,
    )
    monitor_process.start()

    try:
        # Invoke the function and handle its result
        result = invoke_function(function_stub, context, task_id)
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
    finally:
        monitor_process.terminate()
        monitor_process.join(timeout=1)


def start_task(
    gateway_stub: GatewayServiceStub, task_id: str, container_id: str
) -> StartTaskResponse:
    return gateway_stub.start_task(StartTaskRequest(task_id=task_id, container_id=container_id))


def invoke_function(
    function_stub: FunctionServiceStub, context: FunctionContext, task_id: str
) -> InvokeResult:
    result: Any = None
    callback_url = None
    pickled_result = None

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
