import asyncio
import json
import os
import time
import traceback
from concurrent.futures import ThreadPoolExecutor

import cloudpickle
import grpc
import grpclib
from grpclib.client import Channel
from grpclib.exceptions import StreamTerminatedError

from ..channel import with_runner_context
from ..clients.function import (
    FunctionGetArgsRequest,
    FunctionGetArgsResponse,
    FunctionMonitorRequest,
    FunctionMonitorResponse,
    FunctionServiceStub,
    FunctionSetResultRequest,
    FunctionSetResultResponse,
)
from ..clients.gateway import (
    EndTaskRequest,
    EndTaskResponse,
    GatewayServiceStub,
    StartTaskRequest,
    StartTaskResponse,
)
from ..exceptions import InvalidFunctionArgumentsException, RunnerException
from ..logging import StdoutJsonInterceptor
from ..runner.common import FunctionContext, FunctionHandler, config, send_callback
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


async def _monitor_task(
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
            async for response in function_stub.function_monitor(
                FunctionMonitorRequest(
                    task_id=task_id,
                    stub_id=stub_id,
                    container_id=container_id,
                )
            ):
                response: FunctionMonitorResponse
                if response.cancelled:
                    print(f"Task cancelled: {task_id}")

                    await send_callback(
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

                    await send_callback(
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
            grpc.aio.AioRpcError,
            grpclib.exceptions.GRPCError,
            StreamTerminatedError,
            ConnectionRefusedError,
        ):
            if retry == max_retries:
                print("Lost connection to task monitor, exiting")
                os._exit(0)

            await asyncio.sleep(backoff)
            backoff *= 2
            retry += 1

        except asyncio.exceptions.CancelledError:
            return

        except BaseException:
            print("Unexpected error occurred in task monitor")
            os._exit(0)


@with_runner_context
def main(channel: Channel):
    function_stub: FunctionServiceStub = FunctionServiceStub(channel)
    gateway_stub: GatewayServiceStub = GatewayServiceStub(channel)
    loop = asyncio.get_event_loop()
    executor = ThreadPoolExecutor()
    task_id = config.task_id

    async def _run_task():
        with StdoutJsonInterceptor(task_id=task_id):
            container_id = config.container_id
            container_hostname = config.container_hostname
            if not task_id:
                raise RunnerException("Invalid runner environment")

            # Start the task
            start_time = time.time()
            start_task_response: StartTaskResponse = await gateway_stub.start_task(
                StartTaskRequest(task_id=task_id, container_id=container_id)
            )

            if not start_task_response.ok:
                raise RunnerException("Unable to start task")

            # Kick off monitoring task
            context = FunctionContext.new(config=config, task_id=task_id)
            monitor_task = loop.create_task(
                _monitor_task(
                    context=context,
                    stub_id=config.stub_id,
                    task_id=task_id,
                    container_id=config.container_id,
                    function_stub=function_stub,
                    gateway_stub=gateway_stub,
                ),
            )

            task_status = TaskStatus.Complete
            error = None

            # Invoke function
            try:
                get_args_resp: FunctionGetArgsResponse = await function_stub.function_get_args(
                    FunctionGetArgsRequest(task_id=task_id)
                )

                if not get_args_resp.ok:
                    raise InvalidFunctionArgumentsException

                handler = FunctionHandler()
                payload: dict = _load_args(get_args_resp.args)
                args = payload.get("args") or []
                kwargs = payload.get("kwargs") or {}

                result = await loop.run_in_executor(
                    executor, lambda: handler(context, *args, **kwargs)
                )
            except BaseException as exc:
                print(traceback.format_exc())
                result = error = exc
                task_status = TaskStatus.Error
            finally:
                pickled_result = cloudpickle.dumps(result)
                set_result_resp: FunctionSetResultResponse = (
                    await function_stub.function_set_result(
                        FunctionSetResultRequest(task_id=task_id, result=pickled_result)
                    )
                )

                if not set_result_resp.ok:
                    raise RunnerException("Unable to set function result")

            task_duration = time.time() - start_time

            # End the task
            end_task_response: EndTaskResponse = await gateway_stub.end_task(
                EndTaskRequest(
                    task_id=task_id,
                    task_duration=task_duration,
                    task_status=task_status,
                    container_id=container_id,
                    container_hostname=container_hostname,
                    keep_warm_seconds=0,
                )
            )

            if not end_task_response.ok:
                raise RunnerException("Unable to end task")

            monitor_task.cancel()

            await send_callback(
                gateway_stub=gateway_stub,
                context=context,
                payload=result,
                task_status=task_status,
            )  # Send callback to callback_url, if defined

            if task_status == TaskStatus.Error:
                raise error.with_traceback(error.__traceback__)

    loop.run_until_complete(_run_task())


if __name__ == "__main__":
    main()
