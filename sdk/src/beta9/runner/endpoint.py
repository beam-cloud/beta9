import logging
import os
import signal

from fastapi import FastAPI, Request, Response
from uvicorn import Config, Server

from ..runner.common import load_handler

logger = logging.getLogger("uvicorn.access")


# Define the filter
class EndpointFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.args and len(record.args) >= 3 and record.args[2] != "/health"


logger.addFilter(EndpointFilter())


class EndpointManager:
    def __init__(self) -> None:
        # Manager attributes
        self.pid: int = os.getpid()
        self.exit_code: int = 0
        self.app = FastAPI()
        self.handler = load_handler().func  # The function exists under the decorator
        self.context = {"loader": "something"}  # TODO: implement context loader
        signal.signal(signal.SIGTERM, self.shutdown)

        # Attach context
        self.app.add_api_route("/", self.handler, methods=["POST", "GET"])

        @self.app.get("/health")
        def health():
            return Response(status_code=200)

        @self.app.middleware("http")
        def add_context(request: Request, call_next):
            request.state.context = self.context
            return call_next(request)

    def _watchdog(self):
        pass

    def shutdown(self, signum, frame):
        os._exit(self.exit_code)


if __name__ == "__main__":
    manager = EndpointManager()
    config = Config(
        app=manager.app,
        host="0.0.0.0",
        port=int(os.getenv("BIND_PORT")),
        workers=int(os.getenv("WORKERS", 1)),
    )
    server = Server(config)
    server.run()
