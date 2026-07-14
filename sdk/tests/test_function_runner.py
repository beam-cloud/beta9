import asyncio
import importlib
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from beta9.runner import common

common.config = SimpleNamespace(task_id="task-id")
function_runner = importlib.import_module("beta9.runner.function")


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
