import io
import json
from contextvars import ContextVar


log_context = ContextVar("log_context", default={})


class StdoutJsonInterceptor(io.TextIOBase):
    def __init__(self, stream):
        self.stream = stream

    def write(self, buf: str):
        try:
            for line in buf.rstrip().splitlines():
                global_ctx = log_context.get()

                log_record = {
                    "message": line,
                    **global_ctx,
                }

                self.stream.write(json.dumps(log_record) + "\n")
        except BaseException:
            self.stream.write(buf)

    def flush(self):
        return self.stream.flush()

    def close(self):
        return self.stream.close()

    def fileno(self) -> int:
        return -1

    @staticmethod
    def set_context(**kwargs):
        log_context.set(kwargs)

    @staticmethod
    def get_context():
        return log_context.get()

    @staticmethod
    def reset_context():
        log_context.set({})

    @staticmethod
    def add_context_var(key, value):
        ctx = log_context.get()
        ctx[key] = value
        log_context.set(ctx)
