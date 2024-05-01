import io
import json
import sys


class StdoutJsonInterceptor(io.TextIOBase):
    def __init__(self, stream=sys.__stdout__, **ctx):
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
