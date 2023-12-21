import os
import sys

import cloudpickle
from grpclib.client import Channel

from beam.aio import run_sync
from beam.clients.function import (
    FunctionGetArgsResponse,
    FunctionServiceStub,
)
from beam.clients.gateway import GatewayServiceStub
from beam.config import get_gateway_channel

if __name__ == "__main__":
    channel: Channel = get_gateway_channel()

    gateway_stub: GatewayServiceStub = GatewayServiceStub(channel)
    function_stub: FunctionServiceStub = FunctionServiceStub(channel)

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
