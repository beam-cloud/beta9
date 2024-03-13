import io
from contextvars import ContextVar


log_context = ContextVar("log_context", default={})

class StdoutJsonInterceptor(io.TextIOBase):
    def __init__(self, stream):
        self.stream = stream

    def write(self, buf):
        try:
            for line in buf.rstrip().splitlines():
                ctx = log_context.get()
                
                log_record = {
                    "message": line,
                    **ctx
                }
                
                self.stream.write(json.dumps(log_record) + "\n")
                
        except BaseException:
            self.stream.write(buf)

    def flush(self):
        return self.stream.flush()

    def close(self):
        return self.stream.close()
    
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
    