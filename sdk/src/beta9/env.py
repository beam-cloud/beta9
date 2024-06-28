import os
from functools import wraps
from typing import Callable


def called_on_import() -> bool:
    """Check if we are currently in the process of importing the users code."""
    return os.getenv("BETA9_IMPORTING_USER_CODE", "false") == "true"


def is_local() -> bool:
    """Check if we are currently running in a remote container"""
    return os.getenv("CONTAINER_ID", "") == ""


def is_remote() -> bool:
    return not is_local()


def local_entrypoint(func: Callable) -> None:
    """Decorator that executes the decorated function if the environment is local (i.e. not a remote container)"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        if is_local():
            func(*args, **kwargs)

    wrapper()
