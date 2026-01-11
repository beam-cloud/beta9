import importlib
import inspect
import os
import sys
import time
from pathlib import Path

from . import terminal


def retry_on_transient_error(fn, max_retries: int = 3, delay: float = 0.5):
    """
    Retry a function on transient gRPC/connection errors.
    Returns the result or raises the last exception.

    Args:
        fn: The function to call.
        max_retries: Maximum number of retry attempts. Default is 3.
        delay: Base delay in seconds between retries (uses exponential backoff). Default is 0.5.

    Returns:
        The result of the function call.

    Raises:
        The last exception encountered if all retries fail.
    """
    last_exception = None
    for attempt in range(max_retries):
        try:
            return fn()
        except Exception as e:
            error_str = str(e).lower()
            # Retry on transient connection errors
            if any(
                keyword in error_str
                for keyword in ["connection", "unavailable", "timeout", "reset", "eof"]
            ):
                last_exception = e
                if attempt < max_retries - 1:
                    time.sleep(delay * (attempt + 1))  # Exponential backoff
                    continue
            # Non-transient error, raise immediately
            raise
    raise last_exception


class TempFile:
    """
    A temporary file that is automatically deleted when closed. This class exists
    because the `tempfile.NamedTemporaryFile` class does not allow for the filename
    to be explicitly set.
    """

    def __init__(self, name: str, mode: str = "wb", dir: str = "."):
        self.name = name
        self._file = open(os.path.join(dir, name), mode)

    def __getattr__(self, attr):
        return getattr(self._file, attr)

    def close(self):
        if not self._file.closed:
            self._file.close()
            os.remove(self.name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def get_init_args_kwargs(cls):
    sig = inspect.signature(cls.__init__)

    # Separate args and kwargs
    args = []
    kwargs = {}

    for k, v in sig.parameters.items():
        # Skip 'self' since it's implicit for instance methods
        if k == "self":
            continue

        # Check if the argument is a required positional argument
        if v.default is inspect.Parameter.empty and v.kind in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.POSITIONAL_ONLY,
        ):
            args.append(k)

        # Check if the argument is a keyword argument (with default)
        elif v.default is not inspect.Parameter.empty and v.kind in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        ):
            kwargs[k] = v.default

    all_args_set = args + list(kwargs.keys())
    return set(all_args_set)


def get_class_name(cls):
    return cls.__class__.__name__


def load_module_spec(specfile: str, command: str):
    current_dir = os.getcwd()
    if current_dir not in sys.path:
        sys.path.insert(0, current_dir)

    module_path, obj_name, *_ = specfile.split(":") if ":" in specfile else (specfile, "")
    module_name = module_path.replace(".py", "").replace(os.path.sep, ".")

    if not Path(module_path).exists():
        terminal.error(f"Unable to find file: '{module_path}'")

    if not obj_name:
        terminal.error(
            f"Invalid handler function specified. Expected format: beam {command} [file.py]:[function]"
        )

    module = importlib.import_module(module_name)

    module_spec = getattr(module, obj_name, None)
    if module_spec is None:
        terminal.error(
            f"Invalid handler function specified. Make sure '{module_path}' contains the function: '{obj_name}'"
        )

    return module_spec, module_name, obj_name
