import atexit
import logging
import os
import signal
from typing import Callable

from fastapi import FastAPI, Response
from uvicorn import Config, Server

from ..runner.common import config as cfg
from ..runner.common import load_handler

logger = logging.getLogger("uvicorn.access")


class EndpointFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.args and len(record.args) >= 3 and record.args[2] != "/health"


logger.addFilter(EndpointFilter())


class EndpointManager:
    def __init__(self) -> None:
        self.pid: int = os.getpid()
        self.exit_code: int = 0
        self.app = FastAPI()
        self.handler: Callable = load_handler().func  # The function exists under the decorator

        # loader = load_loader()
        # self.loader: Callable =
        self.context = {"loader": "something"}  # TODO: implement context loader

        signal.signal(signal.SIGTERM, self.shutdown)

        @self.app.get("/health")
        def health():
            return Response(status_code=200)

        @self.app.route("/", methods=["POST", "GET"])
        async def function():
            return Response(status_code=200)

        # @self.app.middleware("http")
        # def add_context(request: Request, call_next):
        #     request.state.context = self.context
        #     return call_next(request)

    def _watchdog(self):
        pass

    def shutdown(self, signum=None, frame=None):
        os._exit(self.exit_code)


if __name__ == "__main__":
    mg = EndpointManager()
    atexit.register(mg.shutdown)

    config = Config(
        app=mg.app,
        host="0.0.0.0",
        port=cfg.bind_port,
        workers=cfg.concurrency,
    )

    server = Server(config)
    server.run()
