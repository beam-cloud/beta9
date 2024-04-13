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

from ..clients.gateway import (
    EndTaskRequest,
    EndTaskResponse,
    GatewayServiceStub,
    StartTaskRequest,
    StartTaskResponse,
)
from ..config import with_runner_context
from ..runner.common import config as cfg
from ..runner.common import load_handler
from ..type import TaskStatus


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
            # TODO: wait for loader to complete before returning a 200
            return Response(status_code=200)

        @self.app.route("/", methods=["POST", "GET"])
        async def function(request: Request):
            payload = await request.json()

            r: StartTaskResponse = await request.app.state.gateway_stub.start_task(
                StartTaskRequest(
                    task_id=payload["kwargs"]["task_id"], container_id=cfg.container_id
                )
            )

            print("R.ok?: ", r.ok)
            print(payload)
            result = self._call_function(payload)
            print(result)

            r: EndTaskResponse = await request.app.state.gateway_stub.end_task(
                EndTaskRequest(
                    task_id=payload["kwargs"]["task_id"],
                    container_id=cfg.container_id,
                    keep_warm_seconds=cfg.keep_warm_seconds,
                    task_status=TaskStatus.Complete,
                )
            )
            print("R.ok?: ", r.ok)

            return Response(status_code=200)

    def _call_function(self, payload: dict) -> Tuple[Response, Any]:
        error = None
        response_body = {}

        args = payload.get("args", [])
        if args is None:
            args = []

        kwargs = payload.get("kwargs", {})
        if kwargs is None:
            kwargs = {}

        try:
            response_body = self.handler(*args, **kwargs)
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
