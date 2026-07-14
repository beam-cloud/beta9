import asyncio
import importlib
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import cloudpickle

from beta9.runner import common

original_config = common.config
common.config = SimpleNamespace(task_id="task-id")
try:
    function_runner = importlib.import_module("beta9.runner.function")
finally:
    common.config = original_config


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
