import asyncio
import os
import sys
from asyncio import AbstractEventLoop

import cloudpickle
from grpclib.client import Channel

from beam.abstractions.base import GatewayConfig, get_gateway_config
from beam.clients.function import (
    FunctionGetArgsResponse,
    FunctionServiceStub,
)
from beam.clients.gateway import GatewayServiceStub


def run_sync(loop: AbstractEventLoop, coroutine):
    return loop.run_until_complete(coroutine)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    config: GatewayConfig = get_gateway_config()
    channel: Channel = Channel(
        host=config.host,
        port=config.port,
        ssl=True if config.port == 443 else False,
    )
    gateway_stub: GatewayServiceStub = GatewayServiceStub(channel)
    function_stub: FunctionServiceStub = FunctionServiceStub(channel)

    invocation_id = os.getenv("INVOCATION_ID")
    if invocation_id is None:
        sys.exit(1)

    """
        Load any grpc classes we need
        use import lib to load their code from distributed storage
        run their function
        exit

    """

    r: FunctionGetArgsResponse = run_sync(
        loop, function_stub.function_get_args(invocation_id=invocation_id)
    )
    if not r.ok:
        sys.exit(1)

    args: dict = cloudpickle.loads(r.args)
    print(args)

    # TODO: load the handler module and pass args
    # pass
