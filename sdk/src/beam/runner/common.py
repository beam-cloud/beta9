import importlib
import os
import sys
from typing import Callable

from beam.exceptions import RunnerException

USER_CODE_VOLUME = "/mnt/code"


def load_handler() -> Callable:
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
