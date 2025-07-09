import functools
import io
import json
import sys
from typing import Any


class StdoutJsonInterceptor(io.TextIOBase):
    def __init__(self, stream=sys.__stdout__, **ctx: Any):
        self.ctx = ctx
        self.stream = stream

    def __enter__(self):
        sys.stdout = self
        sys.stderr = self
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        return False

    def write(self, buf: str):
        try:
            for line in buf.splitlines():
                if line == "":
                    continue

                log_record = {
                    "message": f"{line}\n",
                    **self.ctx,
                }

                self.stream.write(json.dumps(log_record))
        except BaseException:
            self.stream.write(buf)

    def flush(self):
        return self.stream.flush()

    def fileno(self) -> int:
        return -1


def json_output_interceptor(**ctx: Any):
    """
    A class decorator that intercepts stdout and stderr and writes the output
    as JSON objects to the original stdout.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with StdoutJsonInterceptor(**ctx):
                return func(*args, **kwargs)

        return wrapper

    return decorator


class StoredStdoutInterceptor(io.TextIOBase):
    def __init__(self, capture_logs: bool = False):
        self.logs = []
        self.capture_logs = capture_logs

    def __enter__(self):
        self.logs = []
        sys.stdout = self
        return self

    def __exit__(self, *_, **__):
        sys.stdout = sys.__stdout__

    def write(self, data: str):
        self.logs.append(data)
        if not self.capture_logs:
            sys.__stdout__.write(data)
            sys.__stdout__.flush()

    def flush(self):
        if not self.capture_logs:
            sys.__stdout__.flush()

    def fileno(self) -> int:
        try:
            return sys.__stdout__.fileno()
        except (AttributeError, io.UnsupportedOperation):
            return -1

    def isatty(self) -> bool:
        return sys.__stdout__.isatty()

    def writable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False

    def readable(self) -> bool:
        return False
