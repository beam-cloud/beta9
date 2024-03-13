import io

class StdoutJsonInterceptor(io.TextIOWrapper):
    def __init__(self, stream, interceptor):
        super().__init__(stream)
        self.interceptor = interceptor

    def write(self, data):
        self.interceptor(data)
        return super().write(data)

    def flush(self):
        return super().flush()

    def close(self):
        return super().close()