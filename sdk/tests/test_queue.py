from unittest import TestCase
from unittest.mock import MagicMock

import cloudpickle

from beam.abstractions.queue import SimpleQueue, SimpleQueueInternalServerError
from beam.clients.simplequeue import (
    SimpleQueueEmptyResponse,
    SimpleQueuePeekResponse,
    SimpleQueuePopResponse,
    SimpleQueuePutResponse,
    SimpleQueueSizeResponse,
)

from .utils import override_run_sync


class TestSimpleQueue(TestCase):
    def setUp(self):
        pass

    def test_put(self):
        mock_stub = MagicMock()

        mock_stub.put.return_value = SimpleQueuePutResponse(ok=True)

        queue = SimpleQueue(name="test")
        queue.stub = mock_stub
        queue.run_sync = override_run_sync

        self.assertTrue(queue.put("test"))

        mock_stub.put.return_value = SimpleQueuePutResponse(ok=False)

        queue = SimpleQueue(name="test")
        queue.stub = mock_stub
        queue.run_sync = override_run_sync

        self.assertRaises(SimpleQueueInternalServerError, queue.put, "test")

    def test_pop(self):
        mock_stub = MagicMock()

        pickled_value = cloudpickle.dumps("test")

        mock_stub.pop.return_value = SimpleQueuePopResponse(ok=True, value=pickled_value)

        queue = SimpleQueue(name="test")
        queue.stub = mock_stub
        queue.run_sync = override_run_sync

        self.assertEqual(queue.pop(), "test")

        mock_stub.pop.return_value = SimpleQueuePopResponse(ok=False, value=b"")

        queue = SimpleQueue(name="test")
        queue.stub = mock_stub
        queue.run_sync = override_run_sync

        self.assertRaises(SimpleQueueInternalServerError, queue.pop)

    def test_peek(self):
        mock_stub = MagicMock()

        pickled_value = cloudpickle.dumps("test")

        mock_stub.peek.return_value = SimpleQueuePeekResponse(ok=True, value=pickled_value)

        queue = SimpleQueue(name="test")
        queue.stub = mock_stub
        queue.run_sync = override_run_sync

        self.assertEqual(queue.peek(), "test")

        mock_stub.peek.return_value = SimpleQueuePeekResponse(ok=False, value=b"")

        queue = SimpleQueue(name="test")
        queue.stub = mock_stub
        queue.run_sync = override_run_sync

        self.assertRaises(SimpleQueueInternalServerError, queue.peek)

    def test_empty(self):
        mock_stub = MagicMock()

        mock_stub.empty.return_value = SimpleQueueEmptyResponse(ok=True, empty=True)

        queue = SimpleQueue(name="test")
        queue.stub = mock_stub
        queue.run_sync = override_run_sync

        self.assertTrue(queue.empty())

        mock_stub.empty.return_value = SimpleQueueEmptyResponse(ok=False, empty=True)

        queue = SimpleQueue(name="test")
        queue.stub = mock_stub
        queue.run_sync = override_run_sync

        self.assertRaises(SimpleQueueInternalServerError, queue.empty)

    def test_size(self):
        mock_stub = MagicMock()

        mock_stub.size.return_value = SimpleQueueSizeResponse(ok=True, size=1)

        queue = SimpleQueue(name="test")
        queue.stub = mock_stub
        queue.run_sync = override_run_sync

        self.assertEqual(len(queue), 1)

        mock_stub.size.return_value = SimpleQueueSizeResponse(ok=False, size=1)

        queue = SimpleQueue(name="test")
        queue.stub = mock_stub
        queue.run_sync = override_run_sync

        self.assertEqual(len(queue), 0)
