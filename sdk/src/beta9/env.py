import os
from functools import wraps


def is_local() -> bool:
    return os.getenv("CONTAINER_ID", "") == ""


def local_entrypoint(func):
    """Decorator that executes the decorated function if the environment is local (not a remote container)"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        if is_local():
            func(*args, **kwargs)

    wrapper()
