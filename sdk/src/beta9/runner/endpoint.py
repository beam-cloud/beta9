import atexit
import logging
import os
import signal
import traceback
from contextlib import asynccontextmanager
from http import HTTPStatus
from typing import Any, Callable, Tuple

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from grpclib.client import Channel
from uvicorn import Config, Server

from ..clients.gateway import (
    EndTaskRequest,
    GatewayServiceStub,
    StartTaskRequest,
)
from ..config import with_runner_context
from ..logging import StdoutJsonInterceptor
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
            return Response(status_code=HTTPStatus.OK)

        @self.app.route("/", methods=["POST", "GET"])
        async def function(request: Request):
            payload = await request.json()
            task_id = payload["kwargs"]["task_id"]

            await request.app.state.gateway_stub.start_task(
                StartTaskRequest(task_id=task_id, container_id=cfg.container_id)
            )

            task_status = TaskStatus.Complete
            status_code = HTTPStatus.OK

            with StdoutJsonInterceptor(task_id=task_id):
                result, err = self._call_function(payload)
                if err:
                    task_status = TaskStatus.Error

            await request.app.state.gateway_stub.end_task(
                EndTaskRequest(
                    task_id=task_id,
                    container_id=cfg.container_id,
                    keep_warm_seconds=cfg.keep_warm_seconds,
                    task_status=task_status,
                )
            )

            if task_status == TaskStatus.Error:
                status_code = HTTPStatus.INTERNAL_SERVER_ERROR

            return self._create_response(body=result, status_code=status_code)

    def _create_response(self, *, body: Any, status_code: int = HTTPStatus.OK) -> Response:
        if isinstance(body, Response):
            return body

        try:
            return JSONResponse(body, status_code=status_code)
        except BaseException:
            logger.exception("Response serialization failed")

            return JSONResponse(
                {"errors": [traceback.format_exc()]},
                status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            )

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
