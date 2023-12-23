import importlib
import os
import sys
from typing import Callable

import cloudpickle
from grpclib.client import Channel

from beam.aio import run_sync
from beam.clients.function import (
    FunctionGetArgsResponse,
    FunctionServiceStub,
    FunctionSetResultResponse,
)
from beam.config import with_runner_context
from beam.exceptions import RunnerException

USER_CODE_VOLUME = "/mnt/code"


def _load_handler() -> Callable:
    sys.path.insert(0, USER_CODE_VOLUME)

    handler = os.getenv("HANDLER")
    if not handler:
        raise RunnerException()

    try:
        module, func = handler.split(":")
        target_module = importlib.import_module(module)
        method = getattr(target_module, func)
        return method
    except BaseException:
        raise RunnerException()


@with_runner_context
def main(channel: Channel):
    function_stub: FunctionServiceStub = FunctionServiceStub(channel)

    invocation_id = os.getenv("INVOCATION_ID")
    if not invocation_id:
        raise RunnerException()

    handler = _load_handler()
    get_args_resp: FunctionGetArgsResponse = run_sync(
        function_stub.function_get_args(invocation_id=invocation_id),
    )
    if not get_args_resp.ok:
        raise RunnerException()

    args: dict = cloudpickle.loads(get_args_resp.args)
    result = handler(*args.get("args", ()), **args.get("kwargs", {}))

    result = cloudpickle.dumps(result)
    set_result_resp: FunctionSetResultResponse = run_sync(
        function_stub.function_set_result(invocation_id=invocation_id, result=result),
    )
    if not set_result_resp.ok:
        raise RunnerException()


if __name__ == "__main__":
    main()
