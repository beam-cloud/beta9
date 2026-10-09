"""Reconnect transport failures without changing the operation's identity."""

import threading
import time
from collections import OrderedDict, deque
from contextvars import ContextVar
from uuid import uuid4

import grpc

from .clients.pod import PodServiceStub

request_metadata = ContextVar("beta9_request_metadata", default=())
_acks = OrderedDict()
_ack_lock = threading.Lock()
RECOVERY_TIMEOUT = 120.0


def transient_error(error):
    from .channel import GatewayHTTPError

    if isinstance(error, grpc.RpcError):
        return error.code() == grpc.StatusCode.UNAVAILABLE
    if isinstance(error, GatewayHTTPError):
        return error.status == 0 or error.status >= 500
    return isinstance(error, (ConnectionError, ConnectionRefusedError, ConnectionResetError))


def retry_operation(fn, *, owner=None, timeout=RECOVERY_TIMEOUT, delay=0.2):
    if request_metadata.get():
        return fn()
    # Imported lazily: channel also consumes the metadata below.
    from .channel import _deadline

    deadline = time.monotonic() + timeout
    if (scope_deadline := _deadline.get()) is not None:
        deadline = min(deadline, scope_deadline)
    request_id = str(uuid4())
    with _ack_lock:
        pending = _acks.pop(owner, deque(maxlen=512))
        acknowledgements = [pending.popleft() for _ in range(min(len(pending), 64))]
        _acks[owner] = pending
        while len(_acks) > 512:
            _acks.popitem(last=False)
    token = request_metadata.set(
        (("x-beta9-request-id", request_id),)
        + tuple(("x-beta9-request-ack", ack) for ack in acknowledgements)
    )
    try:
        while True:
            try:
                result = fn()
                with _ack_lock:
                    if owner in _acks:
                        _acks[owner].append(request_id)
                return result
            except Exception as error:
                remaining = deadline - time.monotonic()
                if not transient_error(error) or remaining <= 0:
                    raise
                time.sleep(min(delay, remaining))
                delay = min(delay * 1.5, 2.0)
    finally:
        request_metadata.reset(token)


class RecoveringPodStub(PodServiceStub):
    """Replay ordinary sandbox RPCs against the worker's request journal."""

    def _unary_unary(self, route, *args, **kwargs):
        call = super()._unary_unary(route, *args, **kwargs)
        method = route.rsplit("/", 1)[-1]
        if not method.startswith("Sandbox") or method in (
            "SandboxSnapshotMemory",
            "SandboxSnapshotDisks",
            "SandboxCreateImageFromFilesystem",
        ):
            return call

        def invoke(request, **options):
            def attempt():
                response = call(request, **options)
                if getattr(response, "error_msg", "") in (
                    "Failed to connect to sandbox",
                    "Failed to get sandbox stdout",
                    "Failed to get sandbox stderr",
                    "Failed to get sandbox status",
                ):
                    raise ConnectionError(response.error_msg)
                return response

            return retry_operation(attempt, owner=getattr(request, "container_id", None))

        return invoke


def resume_invocation(stub, request):
    from .channel import _deadline

    recovery_started = None
    delay = 0.2
    while True:
        try:
            for response in stub.function_invoke(request):
                request.task_id = response.task_id or request.task_id
                if response.output:
                    request.output_offset = response.output_offset
                recovery_started = None
                delay = 0.2
                yield response
                if response.done:
                    return
            raise ConnectionError("Function stream disconnected")
        except Exception as error:
            if not request.task_id or not transient_error(error):
                raise
            if recovery_started is None:
                recovery_started = time.monotonic()
            remaining = RECOVERY_TIMEOUT - (time.monotonic() - recovery_started)
            if remaining <= 0:
                raise
            if (deadline := _deadline.get()) is not None:
                remaining = min(remaining, deadline - time.monotonic())
                if remaining <= 0:
                    raise
            time.sleep(min(delay, remaining))
            delay = min(delay * 1.5, 2.0)
