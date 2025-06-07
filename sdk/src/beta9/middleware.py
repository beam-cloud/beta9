import os
import time
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any, Optional

from fastapi import HTTPException, Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import HTTPConnection
from starlette.types import ASGIApp, Receive, Scope, Send

from .clients.gateway import (
    EndTaskRequest,
    StartTaskRequest,
)
from .logging import StdoutJsonInterceptor
from .runner.common import config as cfg
from .runner.common import end_task_and_send_callback
from .type import TaskStatus


@dataclass
class TaskLifecycleData:
    status: TaskStatus
    result: Any
    override_callback_url: Optional[str] = None


async def run_task(request, func, func_args):
    start_time = time.time()
    task_id = request.headers.get("X-TASK-ID")
    if not task_id:
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail="Task ID missing")

    os.environ["TASK_ID"] = task_id
    with StdoutJsonInterceptor(task_id=task_id):
        print(f"Received task <{task_id}>")

        for attempt in range(3):
            try:
                start_response = request.app.state.gateway_stub.start_task(
                    StartTaskRequest(task_id=task_id, container_id=cfg.container_id)
                )
                if start_response.ok:
                    break
                else:
                    raise HTTPException(
                        status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail="Failed to start task"
                    )
            except BaseException:
                if attempt == 2:
                    raise

        task_lifecycle_data = TaskLifecycleData(
            status=TaskStatus.Complete, result=None, override_callback_url=None
        )

        try:
            request.state.task_lifecycle_data = task_lifecycle_data
            response = await func(*func_args)
            print(f"Task <{task_id}> finished")
            return response
        finally:
            if "TASK_ID" in os.environ:
                del os.environ["TASK_ID"]

            end_task_and_send_callback(
                gateway_stub=request.app.state.gateway_stub,
                payload=task_lifecycle_data.result,
                end_task_request=EndTaskRequest(
                    task_id=task_id,
                    container_id=cfg.container_id,
                    keep_warm_seconds=cfg.keep_warm_seconds,
                    task_status=task_lifecycle_data.status,
                    task_duration=time.time() - start_time,
                ),
                override_callback_url=task_lifecycle_data.override_callback_url,
            )


class WebsocketTaskLifecycleMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "websocket":
            await self.app(scope, receive, send)
            return

        request = HTTPConnection(scope, receive)
        return await run_task(request, self.app, (scope, receive, send))


class TaskLifecycleMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.url.path == "/health":
            return await call_next(request)

        return await run_task(request, call_next, (request,))
