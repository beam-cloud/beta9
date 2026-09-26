import contextvars
import functools
import io
import json
import re
import sys
from typing import Any, Callable, Dict

# Matches the token value in printed Authorization headers (the curl/websocat
# invocation examples). Restricted to the RFC 6750 token68 charset so
# surrounding quotes survive redaction.
_BEARER_TOKEN_PATTERN = re.compile(
    r"(Authorization:\s*Bearer\s+)[A-Za-z0-9\-._~+/=]+", re.IGNORECASE
)


def redact_bearer_tokens(text: str) -> str:
    """Replace bearer token values with [REDACTED], keeping the header shape."""
    return _BEARER_TOKEN_PATTERN.sub(r"\1[REDACTED]", text)


_stdout_json_context: contextvars.ContextVar[Dict[str, Any]] = contextvars.ContextVar(
    "stdout_json_context", default={}
)


def get_stdout_json_context() -> Dict[str, Any]:
    return _stdout_json_context.get()


class StdoutJsonContext:
    def __init__(self, **ctx: Any):
        self.ctx = ctx
        self.token = None

    def __enter__(self):
        self.token = _stdout_json_context.set(self.ctx)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.token is not None:
            _stdout_json_context.reset(self.token)
        return False


class ContextualStdoutJsonInterceptor(io.TextIOBase):
    def __init__(
        self,
        stream=sys.__stdout__,
        context_getter: Callable[[], Dict[str, Any]] = get_stdout_json_context,
        source: str = "stdout",
    ):
        self.context_getter = context_getter
        self.source = source
        self.stream = stream
        self.previous_stdout = None
        self.previous_stderr = None

    def __enter__(self):
        self.previous_stdout = sys.stdout
        self.previous_stderr = sys.stderr
        sys.stdout = self
        sys.stderr = ContextualStdoutJsonInterceptor(self.stream, self.context_getter, "stderr")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        sys.stdout = self.previous_stdout or sys.__stdout__
        sys.stderr = self.previous_stderr or sys.__stderr__
        return False

    def write(self, buf: str):
        try:
            for line in buf.splitlines():
                if line == "":
                    continue

                log_record = {
                    "message": f"{line}\n",
                    **self.context_getter(),
                    "beta9_log": True,
                    "stream": self.source,
                }

                self.stream.write(json.dumps(log_record))
        except BaseException:
            self.stream.write(buf)

    def flush(self):
        return self.stream.flush()

    def fileno(self) -> int:
        return -1


class StdoutJsonInterceptor(ContextualStdoutJsonInterceptor):
    def __init__(self, stream=sys.__stdout__, **ctx: Any):
        self.ctx = ctx
        super().__init__(stream, lambda: self.ctx)


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
    """Keep command logs, streaming them to stderr when stdout is reserved for JSON."""

    def __init__(self, capture_logs: bool = False):
        self.logs = []
        self.capture_logs = capture_logs

    def __enter__(self):
        self.logs = []
        self.previous_stdout = sys.stdout
        self.stream = sys.stderr if self.capture_logs else sys.stdout
        sys.stdout = self
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        sys.stdout = self.previous_stdout

    def write(self, data: str):
        if self.capture_logs:
            # Captured logs end up in machine-readable output (e.g. deploy
            # --format json) that gets piped into CI logs and files, so strip
            # credentials that are fine to show interactively.
            data = redact_bearer_tokens(data)
        self.logs.append(data)
        self.stream.write(data)
        self.stream.flush()
        return len(data)

    def flush(self):
        if hasattr(self, "stream"):
            self.stream.flush()

    def fileno(self) -> int:
        try:
            return self.stream.fileno()
        except (AttributeError, io.UnsupportedOperation):
            return -1

    def isatty(self) -> bool:
        return not self.capture_logs and self.stream.isatty()

    def writable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False

    def readable(self) -> bool:
        return False
