from typing import Callable


def override_run_sync(f: Callable):
    return f
