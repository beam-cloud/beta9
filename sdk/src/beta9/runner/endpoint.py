import atexit
import logging
import os
import signal
import traceback
from contextlib import asynccontextmanager
from typing import Any, Callable, Tuple

from fastapi import FastAPI, Request, Response
from grpclib.client import Channel
from uvicorn import Config, Server

from ..clients.gateway import GatewayServiceStub, StartTaskRequest, StartTaskResponse
from ..config import with_runner_context
from ..runner.common import config as cfg
from ..runner.common import load_handler


class EndpointFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.args and len(record.args) >= 3 and record.args[2] != "/health"


logger = logging.getLogger("uvicorn.access")
logger.addFilter(EndpointFilter())


@asynccontextmanager
@with_runner_context
async def lifespan(app: FastAPI, channel: Channel):
    app.state.gateway_stub = GatewayServiceStub(channel)
    yield


class EndpointManager:
    def __init__(self) -> None:
        self.pid: int = os.getpid()
        self.exit_code: int = 0
        self.app = FastAPI(lifespan=lifespan)
        self.handler: Callable = load_handler().func  # The function exists under the decorator
        self.context = {"loader": "something"}  # TODO: implement context loader

        signal.signal(signal.SIGTERM, self.shutdown)

        @self.app.get("/health")
        def health():
            return Response(status_code=200)

        @self.app.route("/", methods=["POST", "GET"])
        async def function(request: Request):
            r: StartTaskResponse = await request.app.state.gateway_stub.start_task(
                StartTaskRequest(task_id="", container_id=cfg.container_id)
            )
            print("R.ok?: ", r.ok)

            payload = await request.json()
            result = self._call_function(payload)
            print(result)

            await request.app.state.gateway_stub.end_task(
                StartTaskRequest(task_id="", container_id=cfg.container_id)
            )

            return Response(status_code=200)

    def _call_function(self, payload: dict) -> Tuple[Response, Any]:
        error = None
        response_body = {}

        try:
            response_body = self.handler(**payload)
        except BaseException:
            logger.exception("Unhandled exception")
            error = traceback.format_exc()
            response_body = {"errors": [traceback.format_exc()]}

        return response_body, error

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
