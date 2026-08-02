import asyncio
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial
from http import HTTPStatus
from typing import Any, Dict, Optional

import grpc
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import HTTPConnection
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from .clients.gateway import (
    EndTaskRequest,
    ListTasksRequest,
    ListTasksResponse,
    StartTaskRequest,
    StartTaskResponse,
    StringList,
)
from .logging import StdoutJsonContext
from .runner.common import config as cfg
from .runner.common import end_task
from .runner.common import send_task_callback
from .type import TaskStatus


logger = logging.getLogger(__name__)

START_TASK_MAX_ATTEMPTS = 4
START_TASK_INITIAL_BACKOFF_SECONDS = 0.1
START_TASK_MAX_BACKOFF_SECONDS = 1.0
START_TASK_RPC_TIMEOUT_SECONDS = 5
START_TASK_EXECUTOR_WORKERS = 16
END_TASK_EXECUTOR_WORKERS = 8
CALLBACK_EXECUTOR_WORKERS = 16
CALLBACK_AWAIT_TIMEOUT_SECONDS = 10
LIFECYCLE_MAX_IN_FLIGHT = {
    "start": 64,
    "end": 32,
    "callback": CALLBACK_EXECUTOR_WORKERS,
}
TRANSIENT_GRPC_STATUS_CODES = {
    grpc.StatusCode.ABORTED,
    grpc.StatusCode.DEADLINE_EXCEEDED,
    grpc.StatusCode.INTERNAL,
    grpc.StatusCode.RESOURCE_EXHAUSTED,
    grpc.StatusCode.UNAVAILABLE,
}
_executor_lock = threading.Lock()
_executor_pid = 0
_executors: Dict[str, ThreadPoolExecutor] = {}
_executor_slots: Dict[str, threading.BoundedSemaphore] = {}


class TaskStartError(Exception):
    def __init__(self, status_code: HTTPStatus, detail: str, websocket_close_code: int) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail
        self.websocket_close_code = websocket_close_code


class LifecycleExecutorSaturated(RuntimeError):
    pass


def _get_lifecycle_executor(name: str) -> ThreadPoolExecutor:
    global _executor_pid, _executor_slots, _executors

    pid = os.getpid()
    with _executor_lock:
        # Executors created by a pre-fork parent have no usable worker threads
        # in the child. Recreate them lazily in each Gunicorn process.
        if _executor_pid != pid:
            _executor_pid = pid
            _executors = {}
            _executor_slots = {
                executor_name: threading.BoundedSemaphore(limit)
                for executor_name, limit in LIFECYCLE_MAX_IN_FLIGHT.items()
            }

        executor = _executors.get(name)
        if executor is not None:
            return executor

        worker_counts = {
            "start": START_TASK_EXECUTOR_WORKERS,
            "end": END_TASK_EXECUTOR_WORKERS,
            "callback": CALLBACK_EXECUTOR_WORKERS,
        }
        executor = ThreadPoolExecutor(
            max_workers=worker_counts[name], thread_name_prefix=f"beta9-{name}-task"
        )
        _executors[name] = executor
        return executor


def _acquire_lifecycle_slot(name: str) -> Optional[threading.BoundedSemaphore]:
    # Initializes the process-local executor and its bounded admission queue.
    # The slot remains held until the underlying thread actually exits.
    _get_lifecycle_executor(name)
    with _executor_lock:
        slots = _executor_slots.get(name)
    if slots is not None and slots.acquire(blocking=False):
        return slots
    return None


async def _run_in_thread(executor_name: str, func, *args, **kwargs):
    slot = _acquire_lifecycle_slot(executor_name)
    if slot is None:
        raise LifecycleExecutorSaturated(f"{executor_name} lifecycle executor is saturated")
    loop = asyncio.get_running_loop()
    call = partial(func, *args, **kwargs)
    future = loop.run_in_executor(_get_lifecycle_executor(executor_name), call)
    release_on_return = True
    try:
        return await asyncio.shield(future)
    except asyncio.CancelledError:
        # The thread cannot be cancelled. Keep its admission slot until it
        # really exits, otherwise disconnect storms can refill the executor's
        # unbounded internal queue behind still-running RPCs.
        release_on_return = False
        future.add_done_callback(lambda _: slot.release())
        raise
    finally:
        if release_on_return:
            slot.release()


async def _sleep(seconds: float) -> None:
    await asyncio.sleep(seconds)


def _call_start_task(gateway_stub, request: StartTaskRequest) -> StartTaskResponse:
    unary_unary = getattr(gateway_stub, "_unary_unary", None)
    if not callable(unary_unary):
        return gateway_stub.start_task(request)
    return unary_unary(
        "/gateway.GatewayService/StartTask",
        StartTaskRequest,
        StartTaskResponse,
    )(request, timeout=START_TASK_RPC_TIMEOUT_SECONDS)


def _call_list_tasks(gateway_stub, request: ListTasksRequest) -> ListTasksResponse:
    unary_unary = getattr(gateway_stub, "_unary_unary", None)
    if not callable(unary_unary):
        return gateway_stub.list_tasks(request)
    return unary_unary(
        "/gateway.GatewayService/ListTasks",
        ListTasksRequest,
        ListTasksResponse,
    )(request, timeout=START_TASK_RPC_TIMEOUT_SECONDS)


def _confirm_task_started(gateway_stub, task_id: str) -> bool:
    response = _call_list_tasks(
        gateway_stub,
        ListTasksRequest(filters={"id": StringList(values=[task_id])}, limit=1),
    )
    if not response.ok:
        return False
    return any(
        task.id == task_id
        and task.status == str(TaskStatus.Running)
        and task.container_id == cfg.container_id
        for task in response.tasks
    )


async def _confirm_ambiguous_task_start(request: HTTPConnection, task_id: str) -> bool:
    try:
        return await _run_in_thread(
            "start", _confirm_task_started, request.app.state.gateway_stub, task_id
        )
    except Exception:
        return False


@dataclass
class TaskLifecycleData:
    status: TaskStatus
    result: Any
    override_callback_url: Optional[str] = None


async def _start_task(request: HTTPConnection, task_id: str) -> None:
    start_request = StartTaskRequest(task_id=task_id, container_id=cfg.container_id)

    for attempt in range(START_TASK_MAX_ATTEMPTS):
        try:
            start_response = await _run_in_thread(
                "start", _call_start_task, request.app.state.gateway_stub, start_request
            )
        except LifecycleExecutorSaturated as exc:
            raise TaskStartError(
                status_code=HTTPStatus.SERVICE_UNAVAILABLE,
                detail="Container task lifecycle capacity exhausted",
                websocket_close_code=1013,
            ) from exc
        except grpc.RpcError as exc:
            code = exc.code()
            if code not in TRANSIENT_GRPC_STATUS_CODES:
                raise TaskStartError(
                    status_code=HTTPStatus.BAD_GATEWAY,
                    detail="Gateway failed to start task",
                    websocket_close_code=1011,
                ) from exc

            if await _confirm_ambiguous_task_start(request, task_id):
                return

            if attempt == START_TASK_MAX_ATTEMPTS - 1:
                raise TaskStartError(
                    status_code=HTTPStatus.SERVICE_UNAVAILABLE,
                    detail="Gateway temporarily unavailable while starting task",
                    websocket_close_code=1013,
                ) from exc
            retry_reason = code.name
        except Exception as exc:
            if await _confirm_ambiguous_task_start(request, task_id):
                return
            raise TaskStartError(
                status_code=HTTPStatus.BAD_GATEWAY,
                detail="Gateway failed to start task",
                websocket_close_code=1011,
            ) from exc

        else:
            if start_response.ok:
                return

            if attempt == START_TASK_MAX_ATTEMPTS - 1:
                raise TaskStartError(
                    status_code=HTTPStatus.CONFLICT,
                    detail="Gateway rejected task start",
                    websocket_close_code=1008,
                )
            retry_reason = "gateway returned ok=false"

        delay = min(
            START_TASK_INITIAL_BACKOFF_SECONDS * (2**attempt),
            START_TASK_MAX_BACKOFF_SECONDS,
        )
        print(
            f"Failed to start task <{task_id}> due to {retry_reason}; retrying in {delay:g} seconds"
        )
        await _sleep(delay)


async def _end_task(
    request: HTTPConnection,
    task_id: str,
    start_time: float,
    task_lifecycle_data: Optional[TaskLifecycleData] = None,
) -> None:
    if task_lifecycle_data is None:
        task_lifecycle_data = request.state.task_lifecycle_data
    end_request = EndTaskRequest(
        task_id=task_id,
        container_id=cfg.container_id,
        keep_warm_seconds=cfg.keep_warm_seconds,
        task_status=task_lifecycle_data.status,
        task_duration=time.time() - start_time,
    )
    try:
        await _run_in_thread(
            "end", end_task, request.app.state.gateway_stub, end_request
        )
    except Exception:
        logger.exception("Failed to finalize task <%s>", task_id)

    callback_task = asyncio.create_task(
        _run_in_thread(
            "callback",
            send_task_callback,
            gateway_stub=request.app.state.gateway_stub,
            payload=task_lifecycle_data.result,
            end_task_request=end_request,
            override_callback_url=task_lifecycle_data.override_callback_url,
        )
    )
    try:
        await asyncio.wait_for(
            asyncio.shield(callback_task), timeout=CALLBACK_AWAIT_TIMEOUT_SECONDS
        )
    except asyncio.TimeoutError:
        logger.warning(
            "Callback for task <%s> exceeded %ss; continuing in background",
            task_id,
            CALLBACK_AWAIT_TIMEOUT_SECONDS,
        )

        def _consume_callback_result(task) -> None:
            try:
                task.result()
            except Exception:
                logger.exception("Background callback failed for task <%s>", task_id)

        callback_task.add_done_callback(_consume_callback_result)
    except LifecycleExecutorSaturated:
        logger.warning("Callback capacity exhausted; dropping callback for task <%s>", task_id)
    except Exception:
        logger.exception("Failed to send callback for task <%s>", task_id)


async def run_task(request, func, func_args):
    start_time = time.time()
    task_id = request.headers.get("X-TASK-ID")
    if not task_id:
        raise TaskStartError(
            status_code=HTTPStatus.BAD_REQUEST,
            detail="Task ID missing",
            websocket_close_code=1008,
        )

    with StdoutJsonContext(task_id=task_id):
        print(f"Received task <{task_id}>")
        await _start_task(request, task_id)
        os.environ["TASK_ID"] = task_id

        request.state.task_id = task_id
        task_lifecycle_data = TaskLifecycleData(
            status=TaskStatus.Complete, result=None, override_callback_url=None
        )

        try:
            request.state.task_lifecycle_data = task_lifecycle_data
            response = await func(*func_args)
            print(f"Task <{task_id}> finished")
            return response
        except asyncio.CancelledError:
            task_lifecycle_data.status = TaskStatus.Cancelled
            raise
        except Exception:
            task_lifecycle_data.status = TaskStatus.Error
            raise
        finally:
            if "TASK_ID" in os.environ:
                del os.environ["TASK_ID"]
            await _end_task(request, task_id, start_time)


class WebsocketTaskLifecycleMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "websocket":
            await self.app(scope, receive, send)
            return

        request = HTTPConnection(scope, receive)
        try:
            await run_task(request, self.app, (scope, receive, send))
        except TaskStartError as exc:
            await send(
                {
                    "type": "websocket.close",
                    "code": exc.websocket_close_code,
                    "reason": exc.detail,
                }
            )


class TaskLifecycleMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.url.path == "/health":
            return await call_next(request)

        try:
            return await run_task(request, call_next, (request,))
        except TaskStartError as exc:
            return JSONResponse({"detail": exc.detail}, status_code=exc.status_code)
