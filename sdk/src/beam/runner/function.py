import os
import sys

import cloudpickle

from beam.aio import run_sync
from beam.clients.function import (
    FunctionGetArgsResponse,
    FunctionServiceStub,
)
from beam.clients.gateway import GatewayServiceStub
from beam.config import runner_context

if __name__ == "__main__":
    with runner_context() as c:
        gateway_stub: GatewayServiceStub = GatewayServiceStub(c)
        function_stub: FunctionServiceStub = FunctionServiceStub(c)

        invocation_id = os.getenv("INVOCATION_ID")
        if invocation_id is None:
            sys.exit(1)

        r: FunctionGetArgsResponse = run_sync(
            function_stub.function_get_args(invocation_id=invocation_id),
        )
        if not r.ok:
            sys.exit(1)

        args: dict = cloudpickle.loads(r.args)
        print(args)

        # TODO: load the handler module and pass args
        # pass
