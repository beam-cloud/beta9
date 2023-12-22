import importlib
import os
import sys
import traceback
from typing import Callable

import cloudpickle
from grpclib.client import Channel

from beam.aio import run_sync
from beam.clients.function import (
    FunctionGetArgsResponse,
    FunctionServiceStub,
)
from beam.config import with_runner_context
from beam.exceptions import RunnerException


def _load_handler() -> Callable:
    sys.path.insert(0, "/code")

    handler = os.getenv("HANDLER")
    if not handler:
        raise RunnerException()

    try:
        module, func = handler.split(":")
        target_module = importlib.import_module(module)
        method = getattr(target_module, func)
        return method
    except BaseException:
        print(traceback.format_exc())
        raise RunnerException()


@with_runner_context
def main(channel: Channel):
    function_stub: FunctionServiceStub = FunctionServiceStub(channel)

    print(os.listdir("/code"))

    invocation_id = os.getenv("INVOCATION_ID")
    if not invocation_id:
        raise RunnerException()

    handler = _load_handler()
    r: FunctionGetArgsResponse = run_sync(
        function_stub.function_get_args(invocation_id=invocation_id),
    )
    if not r.ok:
        raise RunnerException()

    args: dict = cloudpickle.loads(r.args)
    handler(args.get("args", ()), args.get("kwargs", {}))


if __name__ == "__main__":
    main()
