import json
import os
import time
import traceback
from concurrent.futures import CancelledError, ThreadPoolExecutor

import cloudpickle
import grpc

from ..channel import Channel, with_runner_context
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
)
from ..exceptions import InvalidFunctionArgumentsException, RunnerException
from ..logging import StdoutJsonInterceptor
from ..runner.common import (
    FunctionContext,
    FunctionHandler,
    config,
    end_task_and_send_callback,
    send_callback,
)
from ..type import TaskExitCode, TaskStatus


def _load_args(args: bytes) -> dict:
    try:
        return cloudpickle.loads(args)
    except BaseException:
        # If cloudpickle fails, fall back to JSON
        try:
            return json.loads(args.decode("utf-8"))
        except json.JSONDecodeError:
            raise InvalidFunctionArgumentsException


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


@with_runner_context
def main(channel: Channel):
    function_stub: FunctionServiceStub = FunctionServiceStub(channel)
    gateway_stub: GatewayServiceStub = GatewayServiceStub(channel)
    task_id = config.task_id

    if not task_id:
        raise RunnerException("Invalid runner environment")

    start_time = time.time()

    with StdoutJsonInterceptor(task_id=task_id):
        container_id = config.container_id
        container_hostname = config.container_hostname

        # Start the task
        start_task_response = start_task(gateway_stub, task_id, container_id)
        if not start_task_response.ok:
            raise RunnerException("Unable to start task")

        context = FunctionContext.new(config=config, task_id=task_id)

        # Start monitoring the task
        with ThreadPoolExecutor() as thread_pool:
            monitor_future = thread_pool.submit(
                _monitor_task,
                context=context,
                stub_id=config.stub_id,
                task_id=task_id,
                container_id=container_id,
                function_stub=function_stub,
                gateway_stub=gateway_stub,
            )

            # Invoke the function and handle its result
            try:
                result = invoke_function(function_stub, context, task_id)
            except BaseException as e:
                handle_task_failure(e, gateway_stub, task_id, container_id, container_hostname)
                raise
            finally:
                monitor_future.cancel()

            # End the task and send callback
            complete_task(
                gateway_stub,
                result,
                task_id,
                container_id,
                container_hostname,
                start_time,
            )


def start_task(gateway_stub, task_id, container_id):
    return gateway_stub.start_task(StartTaskRequest(task_id=task_id, container_id=container_id))


def invoke_function(function_stub, context, task_id):
    get_args_resp = function_stub.function_get_args(FunctionGetArgsRequest(task_id=task_id))
    if not get_args_resp.ok:
        raise InvalidFunctionArgumentsException("Invalid function arguments")

    payload = _load_args(get_args_resp.args)
    args = payload.get("args", [])
    kwargs = payload.get("kwargs", {})

    handler = FunctionHandler()
    result = handler(context, *args, **kwargs)

    pickled_result = cloudpickle.dumps(result)
    set_result_resp = function_stub.function_set_result(
        FunctionSetResultRequest(task_id=task_id, result=pickled_result)
    )
    if not set_result_resp.ok:
        raise RunnerException("Unable to set function result")

    return result


def complete_task(gateway_stub, result, task_id, container_id, container_hostname, start_time):
    task_status = TaskStatus.Complete
    task_duration = time.time() - start_time

    end_task_response = end_task_and_send_callback(
        gateway_stub=gateway_stub,
        payload=result,
        end_task_request=EndTaskRequest(
            task_id=task_id,
            task_duration=task_duration,
            task_status=task_status,
            container_id=container_id,
            container_hostname=container_hostname,
            keep_warm_seconds=0,
        ),
        override_callback_url=None,
    )

    if not end_task_response.ok:
        raise RunnerException("Unable to end task")


def handle_task_failure(exception, gateway_stub, task_id, container_id, container_hostname):
    result = str(exception)
    task_status = TaskStatus.Error
    task_duration = 0
    keep_warm_seconds = 0

    end_task_and_send_callback(
        gateway_stub=gateway_stub,
        payload=result,
        end_task_request=EndTaskRequest(
            task_id=task_id,
            task_duration=task_duration,
            task_status=task_status,
            container_id=container_id,
            container_hostname=container_hostname,
            keep_warm_seconds=keep_warm_seconds,
        ),
    )


if __name__ == "__main__":
    main()
