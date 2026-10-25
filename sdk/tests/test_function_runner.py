import asyncio
import importlib
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import cloudpickle
import grpc
import pytest

from beta9.runner import common
from beta9.channel import Channel
from beta9.clients.function import FunctionMonitorRequest

original_config = common.config
common.config = SimpleNamespace(task_id="task-id")
try:
    function_runner = importlib.import_module("beta9.runner.function")
finally:
    common.config = original_config


def test_remote_function_import_does_not_require_ssh_client_packages():
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; sys.modules['websocket'] = None; "
            "sys.modules['paramiko'] = None; from beta9 import function",
        ],
        env=dict(os.environ, CONTAINER_ID="function-test"),
        check=True,
        capture_output=True,
    )


def test_handler_load_failure_is_captured_for_task_failure_reporting():
    load_error = ImportError("broken user import")

    with patch.object(function_runner, "FunctionHandler", side_effect=load_error):
        result = asyncio.run(
            function_runner.invoke_function(
                function_stub=MagicMock(),
                context=MagicMock(),
                task_id="task-id",
            )
        )

    assert result.exception is load_error


def test_runner_signals_ready_before_invoking_handler(tmp_path, monkeypatch):
    ready_path = tmp_path / "runner-ready"
    monkeypatch.setenv("BETA9_RUNNER_READY_PATH", str(ready_path))

    function_stub = MagicMock()
    function_stub.function_get_args.return_value = SimpleNamespace(
        ok=True,
        args=cloudpickle.dumps({"args": [], "kwargs": {}}),
    )
    function_stub.function_set_result.return_value = SimpleNamespace(ok=True)

    class Handler:
        is_async = False

        def __call__(self, _context):
            assert ready_path.exists()
            return "ok"

    result = asyncio.run(
        function_runner.invoke_function(
            function_stub=function_stub,
            context=MagicMock(),
            task_id="task-id",
            handler=Handler(),
        )
    )

    assert result.exception is None


def test_runner_ready_signal_failure_does_not_fail_user_code(monkeypatch):
    monkeypatch.setenv("BETA9_RUNNER_READY_PATH", "/runner-ready")

    function_stub = MagicMock()
    function_stub.function_get_args.return_value = SimpleNamespace(
        ok=True,
        args=cloudpickle.dumps({"args": [], "kwargs": {}}),
    )
    function_stub.function_set_result.return_value = SimpleNamespace(ok=True)

    handler = MagicMock(is_async=False, return_value="ok")
    with patch.object(function_runner.Path, "touch", side_effect=OSError("read-only")):
        result = asyncio.run(
            function_runner.invoke_function(
                function_stub=function_stub,
                context=MagicMock(),
                task_id="task-id",
                handler=handler,
            )
        )

    assert result.exception is None
    handler.assert_called_once()


def test_monitor_reconnects_without_killing_running_function(monkeypatch):
    import grpc

    class Unavailable(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.UNAVAILABLE

    channel = MagicMock()
    channel.__enter__.return_value = channel
    stub = SimpleNamespace(
        function_monitor=MagicMock(
            side_effect=[Unavailable()] * 7
            + [[SimpleNamespace(cancelled=False, complete=True, timed_out=False)]]
        )
    )
    monkeypatch.setattr(function_runner, "get_channel", lambda _: channel)
    monkeypatch.setattr(function_runner, "FunctionServiceStub", lambda _: stub)
    monkeypatch.setattr(function_runner.time, "sleep", lambda _: None)
    kill = MagicMock()
    monkeypatch.setattr(function_runner.os, "kill", kill)

    function_runner._monitor_task(
        function_context=SimpleNamespace(task_id="task", stub_id="stub", container_id="container"),
        runner_pid=42,
    )
    assert stub.function_monitor.call_count == 8
    kill.assert_not_called()


@pytest.mark.parametrize(
    "completion_code", [None, grpc.StatusCode.CANCELLED, grpc.StatusCode.FAILED_PRECONDITION]
)
def test_function_stops_monitor_before_finalization(monkeypatch, completion_code):
    entered, cancelled = Event(), Event()
    errors = []

    def stream(request, context):
        context.add_callback(cancelled.set)
        entered.set()
        cancelled.wait(10)
        return iter(())

    server = grpc.server(ThreadPoolExecutor(max_workers=1))
    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                "function.FunctionService",
                {
                    "FunctionMonitor": grpc.unary_stream_rpc_method_handler(
                        stream,
                        request_deserializer=FunctionMonitorRequest().parse,
                        response_serializer=bytes,
                    )
                },
            ),
        )
    )
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    channel = Channel(f"127.0.0.1:{port}")
    context = SimpleNamespace(
        task_id="task", stub_id="stub", container_id="container", callback_url=None
    )
    monkeypatch.setattr(
        function_runner,
        "config",
        SimpleNamespace(
            task_id="task",
            container_id="container",
            container_hostname="host",
        ),
    )
    monkeypatch.setattr(function_runner.FunctionContext, "new", lambda **_: context)
    monkeypatch.setattr(function_runner, "get_channel", lambda _: channel)
    monkeypatch.setattr(function_runner.signal, "signal", lambda *_: None)
    monkeypatch.setattr(function_runner.os, "kill", MagicMock())
    monkeypatch.setattr(function_runner, "start_task", lambda *_: SimpleNamespace(ok=True))

    async def invoke(*_):
        assert entered.wait(5)
        return function_runner.InvokeResult(result="ok")

    def complete(*_):
        assert cancelled.wait(5), "the monitor RPC must end before finalization"
        if completion_code is not None:
            raise RpcFailure(completion_code)

    original_monitor = function_runner._monitor_task

    def monitor(**kwargs):
        try:
            original_monitor(**kwargs)
        except Exception as error:
            errors.append(error)

    monkeypatch.setattr(function_runner, "_monitor_task", monitor)
    monkeypatch.setattr(function_runner, "invoke_function", invoke)
    monkeypatch.setattr(function_runner, "complete_task", complete)
    try:
        if completion_code == grpc.StatusCode.FAILED_PRECONDITION:
            with pytest.raises(RpcFailure):
                function_runner.run(channel)
        else:
            function_runner.run(channel)
        assert not errors
    finally:
        channel.close()
        server.stop(0).wait()


class RpcFailure(grpc.RpcError):
    def __init__(self, code=grpc.StatusCode.CANCELLED):
        self._code = code

    def code(self):
        return self._code


def test_result_upload_cancellation_is_not_reported_as_handler_failure():
    stub = MagicMock()
    stub.function_get_args.return_value = SimpleNamespace(
        ok=True,
        args=cloudpickle.dumps(
            {"args": [], "kwargs": {"callback_url": "https://example.com/cancel"}}
        ),
    )
    stub.function_set_result.side_effect = RpcFailure()
    result = asyncio.run(
        function_runner.invoke_function(
            stub,
            SimpleNamespace(container_id="container"),
            "task",
            handler=MagicMock(is_async=False, return_value="ok"),
        )
    )
    assert result.cancelled
    assert result.exception is None
    assert result.callback_url == "https://example.com/cancel"


def test_handler_cancellation_error_is_still_a_handler_failure():
    stub = MagicMock()
    stub.function_get_args.return_value = SimpleNamespace(
        ok=True, args=cloudpickle.dumps({"args": [], "kwargs": {}})
    )
    error = RpcFailure()
    result = asyncio.run(
        function_runner.invoke_function(
            stub,
            SimpleNamespace(container_id="container"),
            "task",
            handler=MagicMock(is_async=False, side_effect=error),
        )
    )
    assert not result.cancelled
    assert result.exception is error
    stub.function_set_result.assert_not_called()


def test_cancelled_completion_does_not_send_success_callback():
    gateway = MagicMock()
    gateway.end_task.side_effect = RpcFailure()
    with patch.object(common, "send_task_callback") as callback:
        with pytest.raises(RpcFailure):
            common.end_task_and_send_callback(
                gateway_stub=gateway,
                payload="result",
                end_task_request=function_runner.EndTaskRequest(
                    task_id="task",
                    container_id="container",
                    task_status="COMPLETE",
                ),
            )
    callback.assert_called_once()
    assert callback.call_args.kwargs["end_task_request"].task_status == "CANCELLED"
    assert callback.call_args.kwargs["payload"] == {}
