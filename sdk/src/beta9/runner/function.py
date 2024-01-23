import json
import os
import time

import cloudpickle
from beta9.aio import run_sync
from beta9.clients.function import (
    FunctionGetArgsResponse,
    FunctionServiceStub,
    FunctionSetResultResponse,
)
from beta9.clients.gateway import EndTaskResponse, GatewayServiceStub, StartTaskResponse
from beta9.config import with_runner_context
from beta9.exceptions import InvalidFunctionArgumentsException, RunnerException
from beta9.runner.common import USER_CODE_VOLUME, config, load_handler
from beta9.type import TaskStatus
from grpclib.client import Channel


def _load_args(args: bytes) -> dict:
    try:
        return cloudpickle.loads(args)
    except BaseException:
        # If cloudpickle fails, fall back to JSON
        try:
            return json.loads(args.decode("utf-8"))
        except json.JSONDecodeError:
            raise InvalidFunctionArgumentsException


@with_runner_context
def main(channel: Channel):
    function_stub: FunctionServiceStub = FunctionServiceStub(channel)
    gateway_stub: GatewayServiceStub = GatewayServiceStub(channel)

    task_id = config.task_id
    container_id = config.container_id
    container_hostname = config.container_hostname
    if not task_id:
        raise RunnerException("Invalid runner environment")

    # Start the task
    start_time = time.time()
    start_task_response: StartTaskResponse = run_sync(
        gateway_stub.start_task(task_id=task_id, container_id=container_id)
    )
    if not start_task_response.ok:
        raise RunnerException("Unable to start task")

    task_status = TaskStatus.Complete
    current_wkdir = os.getcwd()
    error = None

    # Invoke function
    try:
        handler = load_handler()
        get_args_resp: FunctionGetArgsResponse = run_sync(
            function_stub.function_get_args(task_id=task_id),
        )
        if not get_args_resp.ok:
            raise InvalidFunctionArgumentsException

        payload: dict = _load_args(get_args_resp.args)
        args = payload.get("args") or []
        kwargs = payload.get("kwargs") or {}

        os.chdir(USER_CODE_VOLUME)
        result = handler(*args, **kwargs)
    except BaseException as exc:
        result = error = exc
        task_status = TaskStatus.Error
    finally:
        os.chdir(current_wkdir)
        result = cloudpickle.dumps(result)
        set_result_resp: FunctionSetResultResponse = run_sync(
            function_stub.function_set_result(task_id=task_id, result=result),
        )
        if not set_result_resp.ok:
            raise RunnerException("Unable to set function result")

    task_duration = time.time() - start_time

    # End the task
    end_task_response: EndTaskResponse = run_sync(
        gateway_stub.end_task(
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

    if task_status == TaskStatus.Error:
        raise error.with_traceback(error.__traceback__)


if __name__ == "__main__":
    main()
