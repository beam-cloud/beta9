import os
from functools import wraps
from typing import Any, Callable


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


def try_env(env: str, default: Any) -> Any:
    """
    Tries to get an environment variable and returns the default value if it doesn't exist.

    Will also try to convert the value to the type of the default value.

    Args:
        env: The name of the environment variable.
        default: The default value to return if the environment variable doesn't exist.

    Returns:
        The value of the environment variable or the default value if it doesn't exist.
    """
    from .config import get_settings

    name = get_settings().name.upper()
    env_var = env.upper()
    env_val = os.getenv(f"{name}_{env_var}", "")
    target_type = type(default)

    try:
        if target_type is bool:
            return env_val.lower() in ["true", "1", "yes", "on"]

        return target_type(env_val)
    except (ValueError, TypeError):
        return default
