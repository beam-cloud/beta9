from unittest import TestCase, mock
from unittest.mock import MagicMock, PropertyMock

import cloudpickle

from beta9 import Image
from beta9.abstractions.function import Function
from beta9.clients.function import FunctionInvokeResponse


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
