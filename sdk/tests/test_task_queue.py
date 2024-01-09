from unittest import TestCase
from unittest.mock import MagicMock

from beam import Image
from beam.abstractions.taskqueue import TaskQueue
from beam.clients.taskqueue import TaskQueuePutResponse

from .utils import override_run_sync


class TestTaskQueue(TestCase):
    def test_init(self):
        mock_stub = MagicMock()

        queue = TaskQueue(Image(python_version="python3.7"), cpu=100, memory=128)
        queue.stub = mock_stub

        self.assertEqual(queue.image.python_version, "python3.7")
        self.assertEqual(queue.cpu, 100)
        self.assertEqual(queue.memory, 128)

    def test_run_local(self):
        @TaskQueue(Image(python_version="python3.7"), cpu=100, memory=128)
        def test_func():
            return 1

        resp = test_func.local()

        self.assertEqual(resp, 1)

    def test_put(self):
        @TaskQueue(Image(python_version="python3.7"), cpu=100, memory=128)
        def test_func():
            return 1

        test_func.parent.taskqueue_stub = MagicMock()
        test_func.parent.taskqueue_stub.task_queue_put.return_value = TaskQueuePutResponse(
            ok=True, task_id="1234"
        )
        test_func.parent.prepare_runtime = MagicMock(return_value=True)
        test_func.parent.run_sync = override_run_sync

        test_func.put()

        test_func.parent.taskqueue_stub.task_queue_put.return_value = TaskQueuePutResponse(
            ok=False, task_id=""
        )

        self.assertRaises(SystemExit, test_func.put)

    def test__call__(self):
        @TaskQueue(Image(python_version="python3.7"), cpu=100, memory=128)
        def test_func():
            return 1

        test_func.parent.taskqueue_stub = MagicMock()
        test_func.parent.taskqueue_stub.task_queue_put.return_value = TaskQueuePutResponse(
            ok=True, task_id="1234"
        )
        test_func.parent.prepare_runtime = MagicMock(return_value=True)
        test_func.parent.run_sync = override_run_sync

        self.assertRaises(
            NotImplementedError,
            test_func,
        )

        # Test calling in container
        import os

        os.environ["CONTAINER_ID"] = "1234"
        self.assertEqual(test_func(), 1)
