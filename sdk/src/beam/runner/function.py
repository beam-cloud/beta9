import os

import cloudpickle
from grpclib.client import Channel

from beam.aio import run_sync
from beam.clients.function import (
    FunctionGetArgsResponse,
    FunctionServiceStub,
)
from beam.config import with_runner_context
from beam.exceptions import RunnerException


@with_runner_context
def main(channel: Channel):
    function_stub: FunctionServiceStub = FunctionServiceStub(channel)

    invocation_id = os.getenv("INVOCATION_ID")
    if invocation_id is None:
        raise RunnerException(code=1)

    r: FunctionGetArgsResponse = run_sync(
        function_stub.function_get_args(invocation_id=invocation_id),
    )
    if not r.ok:
        raise RunnerException(code=1)

    args: dict = cloudpickle.loads(r.args)
    print(args)

    # TODO: load the handler module and pass args
    # pass


if __name__ == "__main__":
    main()
