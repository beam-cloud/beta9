from unittest import TestCase
from unittest.mock import MagicMock

import cloudpickle
from beta9.abstractions.queue import SimpleQueue, SimpleQueueInternalServerError
from beta9.clients.simplequeue import (
    SimpleQueueEmptyResponse,
    SimpleQueuePeekResponse,
    SimpleQueuePopResponse,
    SimpleQueuePutResponse,
    SimpleQueueSizeResponse,
)

from .utils import mock_coroutine_with_result


class TestSimpleQueue(TestCase):
    def setUp(self):
        pass

    def test_put(self):
        mock_stub = MagicMock()

        mock_stub.simple_queue_put = mock_coroutine_with_result(SimpleQueuePutResponse(ok=True))

        queue = SimpleQueue(name="test")
        queue.stub = mock_stub

        self.assertTrue(queue.put("test"))

        mock_stub.simple_queue_put = mock_coroutine_with_result(SimpleQueuePutResponse(ok=False))

        queue = SimpleQueue(name="test")
        queue.stub = mock_stub

        self.assertRaises(SimpleQueueInternalServerError, queue.put, "test")

    def test_pop(self):
        mock_stub = MagicMock()

        pickled_value = cloudpickle.dumps("test")

        mock_stub.simple_queue_pop = mock_coroutine_with_result(
            SimpleQueuePopResponse(ok=True, value=pickled_value)
        )

        queue = SimpleQueue(name="test")
        queue.stub = mock_stub

        self.assertEqual(queue.pop(), "test")

        mock_stub.simple_queue_pop = mock_coroutine_with_result(
            SimpleQueuePopResponse(ok=False, value=b"")
        )

        queue = SimpleQueue(name="test")
        queue.stub = mock_stub

        self.assertRaises(SimpleQueueInternalServerError, queue.pop)

    def test_peek(self):
        mock_stub = MagicMock()

        pickled_value = cloudpickle.dumps("test")

        mock_stub.simple_queue_peek = mock_coroutine_with_result(
            SimpleQueuePeekResponse(ok=True, value=pickled_value)
        )

        queue = SimpleQueue(name="test")
        queue.stub = mock_stub

        self.assertEqual(queue.peek(), "test")

        mock_stub.simple_queue_peek = mock_coroutine_with_result(
            SimpleQueuePeekResponse(ok=False, value=b"")
        )

        queue = SimpleQueue(name="test")
        queue.stub = mock_stub

        self.assertRaises(SimpleQueueInternalServerError, queue.peek)

    def test_empty(self):
        mock_stub = MagicMock()

        mock_stub.simple_queue_empty = mock_coroutine_with_result(
            SimpleQueueEmptyResponse(ok=True, empty=True)
        )

        queue = SimpleQueue(name="test")
        queue.stub = mock_stub

        self.assertTrue(queue.empty())

        mock_stub.simple_queue_empty = mock_coroutine_with_result(
            SimpleQueueEmptyResponse(ok=False, empty=True)
        )

        queue = SimpleQueue(name="test")
        queue.stub = mock_stub

        self.assertRaises(SimpleQueueInternalServerError, queue.empty)

    def test_size(self):
        mock_stub = MagicMock()

        mock_stub.simple_queue_size = mock_coroutine_with_result(
            SimpleQueueSizeResponse(ok=True, size=1)
        )

        queue = SimpleQueue(name="test")
        queue.stub = mock_stub

        self.assertEqual(len(queue), 1)

        mock_stub.simple_queue_size = mock_coroutine_with_result(
            SimpleQueueSizeResponse(ok=False, size=1)
        )

        queue = SimpleQueue(name="test")
        queue.stub = mock_stub

        self.assertEqual(len(queue), 0)
