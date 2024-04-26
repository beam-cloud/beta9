import asyncio
import json
import os
import time

import cloudpickle
import grpc
import grpclib
from grpclib.client import Channel
from grpclib.exceptions import StreamTerminatedError

from ..aio import run_sync
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
from ..config import with_runner_context
from ..exceptions import InvalidFunctionArgumentsException, RunnerException
from ..logging import StdoutJsonInterceptor
from ..runner.common import FunctionContext, FunctionHandler, config
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
    stub_id: str,
    task_id: str,
    container_id: str,
    function_stub: FunctionServiceStub,
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
                    os._exit(TaskExitCode.Cancelled)

                if response.complete:
                    return

                if response.timed_out:
                    print(f"Task timed out: {task_id}")
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
    task_id = config.task_id

    with StdoutJsonInterceptor(task_id=task_id):
        container_id = config.container_id
        container_hostname = config.container_hostname
        if not task_id:
            raise RunnerException("Invalid runner environment")

        # Start the task
        start_time = time.time()
        start_task_response: StartTaskResponse = run_sync(
            gateway_stub.start_task(StartTaskRequest(task_id=task_id, container_id=container_id))
        )
        if not start_task_response.ok:
            raise RunnerException("Unable to start task")

        # Kick off monitoring task
        monitor_task = loop.create_task(
            _monitor_task(
                stub_id=config.stub_id,
                task_id=task_id,
                container_id=config.container_id,
                function_stub=function_stub,
            ),
        )

        task_status = TaskStatus.Complete
        error = None

        # Invoke function
        try:
            get_args_resp: FunctionGetArgsResponse = run_sync(
                function_stub.function_get_args(FunctionGetArgsRequest(task_id=task_id)),
            )
            if not get_args_resp.ok:
                raise InvalidFunctionArgumentsException

            handler = FunctionHandler()
            context = FunctionContext.new(config=config, task_id=task_id)

            payload: dict = _load_args(get_args_resp.args)
            args = payload.get("args") or []
            kwargs = payload.get("kwargs") or {}

            result = handler(context, *args, **kwargs)
        except BaseException as exc:
            result = error = exc
            task_status = TaskStatus.Error
        finally:
            result = cloudpickle.dumps(result)
            set_result_resp: FunctionSetResultResponse = run_sync(
                function_stub.function_set_result(
                    FunctionSetResultRequest(task_id=task_id, result=result)
                ),
            )
            if not set_result_resp.ok:
                raise RunnerException("Unable to set function result")

        task_duration = time.time() - start_time

        # End the task
        end_task_response: EndTaskResponse = run_sync(
            gateway_stub.end_task(
                EndTaskRequest(
                    task_id=task_id,
                    task_duration=task_duration,
                    task_status=task_status,
                    container_id=container_id,
                    container_hostname=container_hostname,
                    keep_warm_seconds=0,
                )
            )
        )
        if not end_task_response.ok:
            raise RunnerException("Unable to end task")

        monitor_task.cancel()

        if task_status == TaskStatus.Error:
            raise error.with_traceback(error.__traceback__)


if __name__ == "__main__":
    main()
