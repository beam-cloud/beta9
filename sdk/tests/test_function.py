import inspect
import io
from unittest import TestCase, mock
from unittest.mock import MagicMock, PropertyMock

import cloudpickle

from beta9 import Image, terminal
from beta9.abstractions.function import Function, Schedule
from beta9.abstractions.image import ImageBuildResult
from beta9.cli.extraclick import handle_config_override
from beta9.clients.function import FunctionInvokeResponse
from beta9.clients.gateway import GetOrCreateStubResponse
from beta9.sync import FileSyncResult

NO_CREDITS = (
    "insufficient_credits: This workspace has no credits yet. Add credits at "
    "https://platform.beam.cloud/settings/credits to run workloads."
)


def refuse_stub(wrapper, reason=NO_CREDITS):
    """Let the image and file sync succeed, then have the gateway refuse the stub."""
    parent = wrapper.parent
    parent.image.build = MagicMock(
        return_value=ImageBuildResult(success=True, image_id="image-id", python_version="python3.8")
    )
    parent.syncer.sync = MagicMock(return_value=FileSyncResult(success=True, object_id="object-id"))
    parent.gateway_stub.get_or_create_stub = MagicMock(
        return_value=GetOrCreateStubResponse(ok=False, err_msg=reason)
    )


class TestTaskQueue(TestCase):
    def test_init(self):
        mock_stub = MagicMock()

        queue = Function(
            cpu=1,
            memory=128,
            image=Image(python_version="python3.8"),
        )
        queue.function_stub = mock_stub

        self.assertEqual(queue.image.python_version, "python3.8")
        self.assertEqual(queue.cpu, 1000)
        self.assertEqual(queue.memory, 128)
        self.assertEqual(queue.keep_warm_seconds, 0)

    def test_keep_warm_is_not_function_config(self):
        self.assertNotIn("keep_warm_seconds", inspect.signature(Function.__init__).parameters)

        with self.assertRaises(TypeError):
            Function(
                cpu=1,
                memory=128,
                image=Image(python_version="python3.8"),
                keep_warm_seconds=1000,
            )

    def test_keep_warm_override_is_ignored_for_functions(self):
        @Function(cpu=1, memory=128, image=Image(python_version="python3.8"))
        def test_func():
            return 1

        self.assertTrue(
            handle_config_override(
                test_func,
                {
                    "keep_warm_seconds": 120,
                    "min_replicas": 1,
                    "max_replicas": 2,
                    "always_on": True,
                },
            )
        )

        self.assertEqual(test_func.parent.keep_warm_seconds, 0)
        self.assertFalse(hasattr(test_func.parent, "min_replicas"))
        self.assertFalse(hasattr(test_func.parent, "max_replicas"))

    def test_run_local(self):
        @Function(cpu=1, memory=128, image=Image(python_version="python3.8"))
        def test_func():
            return 1

        resp = test_func.local()

        self.assertEqual(resp, 1)

    def test_function_invoke(self):
        def test_func():
            return 1998

        with mock.patch(
            "beta9.abstractions.function.Function.function_stub",
            new_callable=PropertyMock,
            return_value=MagicMock(),
        ):
            func = Function(cpu=1, memory=128, image=Image(python_version="python3.8"))
            func = func(test_func)

            func.parent.syncer = MagicMock()
            func.parent.prepare_runtime = MagicMock(return_value=True)

            func.parent.function_stub.function_invoke.return_value = [
                FunctionInvokeResponse(done=True, exit_code=0, result=cloudpickle.dumps(1998))
            ]

            self.assertEqual(func(), 1998)

            func.parent.function_stub.function_invoke.return_value = [
                FunctionInvokeResponse(done=False, exit_code=1, result=b"")
            ]

    def test_map(self):
        with mock.patch(
            "beta9.abstractions.function.Function.function_stub",
            new_callable=PropertyMock,
            return_value=MagicMock(),
        ):

            @Function(cpu=1, memory=128, image=Image(python_version="python3.8"))
            def test_func(*args, **kwargs):
                return 1998

            pickled_value = cloudpickle.dumps(1998)

            test_func.parent.syncer = MagicMock()

            # Since the return value is a reference to this same aysnc iterator, everytime it
            # it will iterate to the next value. This iterator in testing is persisted across
            # multiple calls to the function, so we can simulate multiple responses.
            # (ONLY HAPPENS DURING TESTING)
            test_func.parent.function_stub.function_invoke.return_value = [
                FunctionInvokeResponse(done=True, exit_code=0, result=pickled_value),
                FunctionInvokeResponse(done=True, exit_code=0, result=pickled_value),
                FunctionInvokeResponse(done=True, exit_code=0, result=pickled_value),
            ]

            test_func.parent.prepare_runtime = MagicMock(return_value=True)

            for val in test_func.map([1, 2, 3]):
                self.assertEqual(val, 1998)

    def test_refused_runtime_exits_on_the_reported_reason(self):
        # A traceback after the reason would bury it, so the call exits instead of raising.
        function = Function(cpu=1, memory=128, image=Image(python_version="python3.8"))
        schedule = Schedule(when="@daily", image=Image(python_version="python3.8"))

        for decorator in (function, schedule):
            wrapper = decorator(lambda *args: 1)
            refuse_stub(wrapper)

            for call in (wrapper, wrapper.remote, lambda: list(wrapper.map([1]))):
                output = io.StringIO()
                with terminal.redirect_terminal_to_buffer(output):
                    with self.assertRaises(SystemExit) as exited:
                        call()

                self.assertEqual(exited.exception.code, 1)
                self.assertEqual(output.getvalue().count("insufficient_credits"), 1)

    def test_map_while_importing_user_code_still_raises(self):
        # Nothing is reported on this path, so exiting would leave no explanation.
        @Function(cpu=1, memory=128, image=Image(python_version="python3.8"))
        def test_func(*args):
            return 1

        with mock.patch.dict("os.environ", {"BETA9_IMPORTING_USER_CODE": "true"}):
            with self.assertRaisesRegex(RuntimeError, "Unable to prepare function runtime"):
                list(test_func.map([1]))
