import importlib
import os
import sys
import time
import traceback
from typing import Callable

import cloudpickle
from grpclib.client import Channel

from beam.aio import run_sync
from beam.clients.function import (
    FunctionGetArgsResponse,
    FunctionServiceStub,
    FunctionSetResultResponse,
)
from beam.clients.gateway import EndTaskResponse, GatewayServiceStub, StartTaskResponse
from beam.config import with_runner_context
from beam.exceptions import RunnerException
from beam.type import TaskStatus

USER_CODE_VOLUME = "/mnt/code"


def _load_handler() -> Callable:
    sys.path.insert(0, USER_CODE_VOLUME)

    handler = os.getenv("HANDLER")
    if not handler:
        raise RunnerException("Handler not specified")

    try:
        module, func = handler.split(":")
        target_module = importlib.import_module(module)
        method = getattr(target_module, func)
        return method
    except BaseException:
        raise RunnerException("Unable to load handler", traceback.format_exc())


@with_runner_context
def main(channel: Channel):
    function_stub: FunctionServiceStub = FunctionServiceStub(channel)
    gateway_stub: GatewayServiceStub = GatewayServiceStub(channel)

    task_id = os.getenv("TASK_ID")
    container_id = os.getenv("CONTAINER_ID")
    container_hostname = os.getenv("CONTAINER_HOSTNAME")
    if not task_id or not container_id:
        raise RunnerException("Invalid runner environment")

    # Load user function and arguments
    handler = _load_handler()
    get_args_resp: FunctionGetArgsResponse = run_sync(
        function_stub.function_get_args(task_id=task_id),
    )
    if not get_args_resp.ok:
        raise RunnerException("Unable to retrieve function arguments")

    args: dict = cloudpickle.loads(get_args_resp.args)

    # Start the task
    start_time = time.time()
    start_task_response: StartTaskResponse = run_sync(
        gateway_stub.start_task(task_id=task_id, container_id=container_id)
    )
    if not start_task_response.ok:
        raise RunnerException("Unable to start task")

    # Invoke function
    task_status = TaskStatus.Complete
    current_wkdir = os.getcwd()
    error = None

    try:
        os.chdir(USER_CODE_VOLUME)
        result = handler(*args.get("args", ()), **args.get("kwargs", {}))
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
            scale_down_delay=0,
        )
    )
    if not end_task_response.ok:
        raise RunnerException("Unable to end task")

    if task_status == TaskStatus.Error:
        raise error.with_traceback(error.__traceback__)


if __name__ == "__main__":
    main()
