import asyncio
import inspect


def override_run_sync(f):
    if inspect.isawaitable(f):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(f)

    return f
