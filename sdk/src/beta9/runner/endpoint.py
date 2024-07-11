import asyncio
import logging
import os
import signal
import traceback
from contextlib import asynccontextmanager
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any, Dict, Optional, Tuple

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from gunicorn.app.base import Arbiter, BaseApplication
from starlette.applications import Starlette
from starlette.types import ASGIApp
from uvicorn.workers import UvicornWorker

from ..abstractions.base.runner import (
    ENDPOINT_SERVE_STUB_TYPE,
)
from ..channel import runner_context
from ..clients.gateway import (
    EndTaskRequest,
    GatewayServiceStub,
    StartTaskRequest,
)
from ..logging import StdoutJsonInterceptor
from ..runner.common import FunctionContext, FunctionHandler, execute_lifecycle_method
from ..runner.common import config as cfg
from ..type import LifeCycleMethod, TaskStatus
from .common import end_task_and_send_callback


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
            mg = EndpointManager(logger=logger, worker=worker)
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


@dataclass
class TaskLifecycleData:
    status: TaskStatus
    result: Any
    override_callback_url: Optional[str] = None


async def task_lifecycle(request: Request):
    task_id = request.headers.get("X-TASK-ID")
    if not task_id:
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail="Task ID missing")

    print(f"Received task <{task_id}>")
    start_response = request.app.state.gateway_stub.start_task(
        StartTaskRequest(task_id=task_id, container_id=cfg.container_id)
    )
    if not start_response.ok:
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail="Failed to start task"
        )

    task_lifecycle_data = TaskLifecycleData(
        status=TaskStatus.Complete, result=None, override_callback_url=None
    )
    try:
        yield task_lifecycle_data
        print(f"Task <{task_id}> finished")
    finally:
        end_task_and_send_callback(
            gateway_stub=request.app.state.gateway_stub,
            payload=task_lifecycle_data.result,
            end_task_request=EndTaskRequest(
                task_id=task_id,
                container_id=cfg.container_id,
                keep_warm_seconds=cfg.keep_warm_seconds,
                task_status=task_lifecycle_data.status,
            ),
            override_callback_url=task_lifecycle_data.override_callback_url,
        )


class OnStartMethodHandler:
    def __init__(self, worker):
        self._is_running = True
        self._worker = worker

    async def start(self):
        loop = asyncio.get_running_loop()
        task = loop.create_task(self._keep_worker_alive())
        result = await loop.run_in_executor(None, execute_lifecycle_method, LifeCycleMethod.OnStart)
        self._is_running = False
        await task
        return result

    async def _keep_worker_alive(self):
        while self._is_running:
            self._worker.notify()
            await asyncio.sleep(1)


class EndpointManager:
    @asynccontextmanager
    async def lifespan(self, _: FastAPI):
        with runner_context() as channel:
            self.app.state.gateway_stub = GatewayServiceStub(channel)
            yield

    def __init__(self, logger: logging.Logger, worker: UvicornWorker) -> None:
        self.logger = logger
        self.pid: int = os.getpid()
        self.exit_code: int = 0
        self.app = FastAPI(lifespan=self.lifespan)

        # Register signal handlers
        signal.signal(signal.SIGTERM, self.shutdown)

        # Load handler and execute on_start method
        self.handler: FunctionHandler = FunctionHandler()
        self.on_start_value = asyncio.run(OnStartMethodHandler(worker).start())

        @self.app.get("/health")
        async def health():
            return Response(status_code=HTTPStatus.OK)

        @self.app.get("/")
        @self.app.post("/")
        async def function(
            request: Request,
            task_lifecycle_data: TaskLifecycleData = Depends(task_lifecycle),
        ):
            task_id = request.headers.get("X-TASK-ID")
            payload = await request.json()

            status_code = HTTPStatus.OK
            with StdoutJsonInterceptor(task_id=task_id):
                task_lifecycle_data.result, err = await self._call_function(
                    task_id=task_id, payload=payload
                )
                if err:
                    task_lifecycle_data.status = TaskStatus.Error

            if task_lifecycle_data.status == TaskStatus.Error:
                status_code = HTTPStatus.INTERNAL_SERVER_ERROR

            task_lifecycle_data.override_callback_url = payload.get("kwargs", {}).get(
                "callback_url"
            )

            return self._create_response(body=task_lifecycle_data.result, status_code=status_code)

    def _create_response(self, *, body: Any, status_code: int = HTTPStatus.OK) -> Response:
        if isinstance(body, Response):
            return body

        try:
            return JSONResponse(body, status_code=status_code)
        except BaseException:
            self.logger.exception("Response serialization failed")
            return JSONResponse(
                {"error": traceback.format_exc()},
                status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    async def _call_function(self, task_id: str, payload: dict) -> Tuple[Response, Any]:
        error = None
        response_body = {}

        args = payload.get("args", [])
        if args is None:
            args = []

        kwargs = payload.get("kwargs", {})
        if kwargs is None:
            kwargs = {}

        context = FunctionContext.new(
            config=cfg,
            task_id=task_id,
            on_start_value=self.on_start_value,
        )

        try:
            if self.handler.is_async:
                response_body = await self.handler(
                    context,
                    *args,
                    **kwargs,
                )
            else:
                response_body = self.handler(
                    context,
                    *args,
                    **kwargs,
                )
        except BaseException:
            exception = traceback.format_exc()
            print(exception)
            response_body = {"error": exception}

        return response_body, error

    def shutdown(self, signum=None, frame=None):
        os._exit(self.exit_code)


if __name__ == "__main__":
    options = {
        "bind": [f"[::]:{cfg.bind_port}"],
        "workers": cfg.workers,
        "worker_class": "uvicorn.workers.UvicornWorker",
        "loglevel": "info",
        "post_fork": GunicornApplication.post_fork_initialize,
        "timeout": cfg.timeout,
    }

    GunicornApplication(Starlette(), options).run()
