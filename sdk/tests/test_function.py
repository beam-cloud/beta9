from unittest import TestCase
from unittest.mock import MagicMock

import cloudpickle

from beam import Image
from beam.abstractions.function import Function
from beam.clients.function import FunctionInvokeResponse


class AsyncIterator:
    def __init__(self, seq):
        self.iter = iter(seq)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.iter)
        except StopIteration:
            raise StopAsyncIteration


class TestTaskQueue(TestCase):
    def test_init(self):
        mock_stub = MagicMock()

        queue = Function(
            cpu=1,
            memory=128,
            image=Image(python_version="python3.8"),
        )
        queue.stub = mock_stub

        self.assertEqual(queue.image.python_version, "python3.8")
        self.assertEqual(queue.cpu, 100)
        self.assertEqual(queue.memory, 128)

    def test_run_local(self):
        @Function(cpu=1, memory=128, image=Image(python_version="python3.8"))
        def test_func():
            return 1

        resp = test_func.local()

        self.assertEqual(resp, 1)

    def test_function_invoke(self):
        @Function(cpu=1, memory=128, image=Image(python_version="python3.8"))
        def test_func(*args, **kwargs):
            return 1998

        pickled_value = cloudpickle.dumps(1998)

        test_func.parent.function_stub = MagicMock()
        test_func.parent.syncer = MagicMock()

        test_func.parent.function_stub.function_invoke.return_value = AsyncIterator(
            [FunctionInvokeResponse(done=True, exit_code=0, result=pickled_value)]
        )

        test_func.parent.prepare_runtime = MagicMock(return_value=True)

        self.assertEqual(test_func(), 1998)

        test_func.parent.function_stub.function_invoke.return_value = AsyncIterator(
            [FunctionInvokeResponse(done=False, exit_code=1, result=b"")]
        )

        self.assertRaises(SystemExit, test_func)

    def test_map(self):
        @Function(cpu=1, memory=128, image=Image(python_version="python3.8"))
        def test_func(*args, **kwargs):
            return 1998

        pickled_value = cloudpickle.dumps(1998)

        test_func.parent.function_stub = MagicMock()
        test_func.parent.syncer = MagicMock()

        # Since the return value is a reference to this same aysnc iterator, everytime it
        # it will iterate to the next value. This iterator in testing is persisted across
        # multiple calls to the function, so we can simulate multiple responses.
        # (ONLY HAPPENS DURING TESTING)
        test_func.parent.function_stub.function_invoke.return_value = AsyncIterator(
            [
                FunctionInvokeResponse(done=True, exit_code=0, result=pickled_value),
                FunctionInvokeResponse(done=True, exit_code=0, result=pickled_value),
                FunctionInvokeResponse(done=True, exit_code=0, result=pickled_value),
            ]
        )

        test_func.parent.prepare_runtime = MagicMock(return_value=True)

        for val in test_func.map([1, 2, 3]):
            self.assertEqual(val, 1998)
