import logging
import os
import signal
import traceback
from contextlib import asynccontextmanager
from http import HTTPStatus
from typing import Any, Dict, Optional, Tuple

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from grpclib.client import Channel
from gunicorn.app.base import Arbiter, BaseApplication
from starlette.applications import Starlette
from starlette.types import ASGIApp
from uvicorn.workers import UvicornWorker

from ..abstractions.base.runner import (
    ENDPOINT_SERVE_STUB_TYPE,
)
from ..clients.gateway import (
    EndTaskRequest,
    GatewayServiceStub,
    StartTaskRequest,
)
from ..config import with_runner_context
from ..logging import StdoutJsonInterceptor
from ..runner.common import FunctionContext, FunctionHandler, execute_lifecycle_method
from ..runner.common import config as cfg
from ..type import LifeCycleMethod, TaskStatus


class EndpointFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if record.args:
            return record.args and len(record.args) >= 3 and record.args[2] != "/health"
        return True


class GunicornArbiter(Arbiter):
    def init_signals(self):
        super(GunicornArbiter, self).init_signals()

        # Add a custom signal handler to kill gunicorn master process with non-zero exit code.
        signal.signal(signal.SIGUSR1, self.handle_usr1)
        signal.siginterrupt(signal.SIGUSR1, True)

    # Override default usr1 handler to force shutdown server when forked processes crash
    # during startup
    def handle_usr1(self, sig, frame):
        os._exit(1)


class GunicornApplication(BaseApplication):
    def __init__(self, app: ASGIApp, options: Optional[Dict] = None) -> None:
        self.options = options or {}
        self.application = app
        super().__init__()

    def load_config(self) -> None:
        for key, value in self.options.items():
            if value is not None:
                self.cfg.set(key.lower(), value)

    def load(self) -> ASGIApp:
        return Starlette()  # Return a base Starlette app -- which will be replaced post-fork

    def run(self):
        GunicornArbiter(self).run()

    @staticmethod
    def post_fork_initialize(_, worker: UvicornWorker):
        logger = logging.getLogger("uvicorn.access")
        logger.addFilter(EndpointFilter())

        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        logger.addHandler(handler)
        logger.propagate = False

        try:
            mg = EndpointManager(logger=logger)
            asgi_app: ASGIApp = mg.app

            # Override the default starlette app
            worker.app.callable = asgi_app
        except EOFError:
            return
        except BaseException:
            logger.exception("Exiting worker due to startup error")
            if cfg.stub_type == ENDPOINT_SERVE_STUB_TYPE:
                return

            # We send SIGUSR1 to indicate to the gunicorn master that the server should shut down completely
            # since our asgi_app callable is erroring out.
            os.kill(os.getppid(), signal.SIGUSR1)
            os._exit(1)


async def task_lifecycle(request: Request):
    task_id = request.headers.get("X-TASK-ID")
    if not task_id:
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail="Task ID missing")

    print(f"Received task <{task_id}>")
    start_response = await request.app.state.gateway_stub.start_task(
        StartTaskRequest(task_id=task_id, container_id=cfg.container_id)
    )
    if not start_response.ok:
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail="Failed to start task"
        )

    try:
        task_status = TaskStatus.Complete
        yield task_status
        print(f"Task <{task_id}> finished")
    finally:
        await request.app.state.gateway_stub.end_task(
            EndTaskRequest(
                task_id=task_id,
                container_id=cfg.container_id,
                keep_warm_seconds=cfg.keep_warm_seconds,
                task_status=task_status,
            )
        )


class EndpointManager:
    @asynccontextmanager
    @with_runner_context
    async def lifespan(self, _: FastAPI, channel: Channel):
        self.app.state.gateway_stub = GatewayServiceStub(channel)

        yield

    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger
        self.pid: int = os.getpid()
        self.exit_code: int = 0
        self.app = FastAPI(lifespan=self.lifespan)

        # Load handler and execute on_start method
        self.handler: FunctionHandler = FunctionHandler()
        self.on_start_value = execute_lifecycle_method(name=LifeCycleMethod.OnStart)

        # Register signal handlers
        signal.signal(signal.SIGTERM, self.shutdown)

        @self.app.get("/health")
        async def health():
            return Response(status_code=HTTPStatus.OK)

        @self.app.post("/")
        async def function(request: Request, task_status: str = Depends(task_lifecycle)):
            task_id = request.headers.get("X-TASK-ID")
            payload = await request.json()

            status_code = HTTPStatus.OK
            with StdoutJsonInterceptor(task_id=task_id):
                result, err = self._call_function(task_id=task_id, payload=payload)
                if err:
                    task_status = TaskStatus.Error

            if task_status == TaskStatus.Error:
                status_code = HTTPStatus.INTERNAL_SERVER_ERROR

            return self._create_response(body=result, status_code=status_code)

    def _create_response(self, *, body: Any, status_code: int = HTTPStatus.OK) -> Response:
        if isinstance(body, Response):
            return body

        try:
            return JSONResponse(body, status_code=status_code)
        except BaseException:
            self.logger.exception("Response serialization failed")
            return JSONResponse(
                {"errors": [traceback.format_exc()]},
                status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    def _call_function(self, task_id: str, payload: dict) -> Tuple[Response, Any]:
        error = None
        response_body = {}

        args = payload.get("args", [])
        if args is None:
            args = []

        kwargs = payload.get("kwargs", {})
        if kwargs is None:
            kwargs = {}

        context = FunctionContext.new(
            config=cfg, task_id=task_id, on_start_value=self.on_start_value
        )

        try:
            response_body = self.handler(
                context,
                *args,
                **kwargs,
            )
        except BaseException:
            self.logger.exception("Unhandled exception")
            error = traceback.format_exc()
            response_body = {"error": traceback.format_exc()}

        return response_body, error

    def shutdown(self, signum=None, frame=None):
        os._exit(self.exit_code)


if __name__ == "__main__":
    options = {
        "bind": [f"[::]:{cfg.bind_port}"],
        "workers": cfg.concurrency,
        "worker_class": "uvicorn.workers.UvicornWorker",
        "loglevel": "info",
        "post_fork": GunicornApplication.post_fork_initialize,
        "timeout": cfg.timeout,
    }

    GunicornApplication(Starlette(), options).run()
