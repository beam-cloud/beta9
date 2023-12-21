import asyncio
import os
import sys

if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    invocation_id = os.getenv("INVOCATION_ID")
    if invocation_id is None:
        sys.exit(1)

    """
        Load any grpc classes we need
        use import lib to load their code from distributed storage
        run their function
        exit

    """

    # import time

    # for i in range(100):
    #     print(i)
    #     time.sleep(1.0)

    # r: FunctionGetArgsResponse = self.run_sync(
    #     self.function_stub.function_get_args(invocation_id=invocation_id)
    # )

    # if not r.ok:
    #     sys.exit(1)

    # args: dict = cloudpickle.loads(r.args)
    # print(args)

    # # TODO: load the handler module and pass args
    # pass
