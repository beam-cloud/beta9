import os
import sys
from functools import wraps
from typing import Callable, TypeVar, Union, overload


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


EnvValue = TypeVar("EnvValue", str, int, float, bool)


@overload
def try_env(env: str, default: str) -> bool: ...


@overload
def try_env(env: str, default: EnvValue) -> EnvValue: ...


def try_env(env: str, default: EnvValue) -> Union[EnvValue, bool]:
    """
    Gets an environment variable and converts it to the correct type based on
    the default value.

    The environment variable name is prefixed with the name of the project if
    it is set in the settings.

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

        return target_type(env_val) or default
    except (ValueError, TypeError):
        return default


def is_notebook_env() -> bool:
    if "google.colab" in sys.modules or "marimo" in sys.modules:
        return True

    try:
        import hex

        return hex.context.is_notebook()
    except (ImportError, NameError, AttributeError):
        pass

    try:
        from ipykernel.zmqshell import ZMQInteractiveShell
        from IPython import get_ipython

        shell = get_ipython().__class__.__name__
        return shell == ZMQInteractiveShell.__name__
    except (NameError, ImportError):
        return False
