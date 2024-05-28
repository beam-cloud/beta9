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


class TestSimpleQueue(TestCase):
    def setUp(self):
        pass

    def test_put(self):
        mock_stub = MagicMock()

        mock_stub.simple_queue_put = MagicMock(return_value=(SimpleQueuePutResponse(ok=True)))

        queue = SimpleQueue(name="test")
        queue._stub = mock_stub

        self.assertTrue(queue.put("test"))

        mock_stub.simple_queue_put = MagicMock(return_value=(SimpleQueuePutResponse(ok=False)))

        queue = SimpleQueue(name="test")
        queue._stub = mock_stub

        self.assertRaises(SimpleQueueInternalServerError, queue.put, "test")

    def test_pop(self):
        mock_stub = MagicMock()

        pickled_value = cloudpickle.dumps("test")

        mock_stub.simple_queue_pop = MagicMock(
            return_value=(SimpleQueuePopResponse(ok=True, value=pickled_value))
        )

        queue = SimpleQueue(name="test")
        queue._stub = mock_stub

        self.assertEqual(queue.pop(), "test")

        mock_stub.simple_queue_pop = MagicMock(
            return_value=(SimpleQueuePopResponse(ok=False, value=b""))
        )

        queue = SimpleQueue(name="test")
        queue._stub = mock_stub

        self.assertRaises(SimpleQueueInternalServerError, queue.pop)

    def test_peek(self):
        mock_stub = MagicMock()

        pickled_value = cloudpickle.dumps("test")

        mock_stub.simple_queue_peek = MagicMock(
            return_value=(SimpleQueuePeekResponse(ok=True, value=pickled_value))
        )

        queue = SimpleQueue(name="test")
        queue._stub = mock_stub

        self.assertEqual(queue.peek(), "test")

        mock_stub.simple_queue_peek = MagicMock(
            return_value=(SimpleQueuePeekResponse(ok=False, value=b""))
        )

        queue = SimpleQueue(name="test")
        queue._stub = mock_stub

        self.assertRaises(SimpleQueueInternalServerError, queue.peek)

    def test_empty(self):
        mock_stub = MagicMock()

        mock_stub.simple_queue_empty = MagicMock(
            return_value=(SimpleQueueEmptyResponse(ok=True, empty=True))
        )

        queue = SimpleQueue(name="test")
        queue._stub = mock_stub

        self.assertTrue(queue.empty())

        mock_stub.simple_queue_empty = MagicMock(
            return_value=(SimpleQueueEmptyResponse(ok=False, empty=True))
        )

        queue = SimpleQueue(name="test")
        queue._stub = mock_stub

        self.assertRaises(SimpleQueueInternalServerError, queue.empty)

    def test_size(self):
        mock_stub = MagicMock()

        mock_stub.simple_queue_size = MagicMock(
            return_value=(SimpleQueueSizeResponse(ok=True, size=1))
        )

        queue = SimpleQueue(name="test")
        queue._stub = mock_stub

        self.assertEqual(len(queue), 1)

        mock_stub.simple_queue_size = MagicMock(
            return_value=(SimpleQueueSizeResponse(ok=False, size=1))
        )

        queue = SimpleQueue(name="test")
        queue._stub = mock_stub

        self.assertEqual(len(queue), 0)
