import os
from unittest import TestCase, mock
from unittest.mock import MagicMock, PropertyMock

from beta9 import Image
from beta9.abstractions.taskqueue import TaskQueue
from beta9.clients.taskqueue import TaskQueuePutResponse


class TestTaskQueue(TestCase):
    def test_init(self):
        mock_stub = MagicMock()

        queue = TaskQueue(cpu=1, memory=128, image=Image(python_version="python3.8"))
        queue.taskqueue_stub = mock_stub

        self.assertEqual(queue.image.python_version, "python3.8")
        self.assertEqual(queue.cpu, 1000)
        self.assertEqual(queue.memory, 128)

    def test_run_local(self):
        @TaskQueue(cpu=1, memory=128, image=Image(python_version="python3.8"))
        def test_func():
            return 1

        resp = test_func.local()

        self.assertEqual(resp, 1)

    def test_put(self):
        with mock.patch(
            "beta9.abstractions.taskqueue.TaskQueue.taskqueue_stub",
            new_callable=PropertyMock,
            return_value=MagicMock(),
        ):

            @TaskQueue(cpu=1, memory=128, image=Image(python_version="python3.8"))
            def test_func():
                return 1

            test_func.parent.taskqueue_stub.task_queue_put = MagicMock(
                return_value=(TaskQueuePutResponse(ok=True, task_id="1234"))
            )
            test_func.parent.prepare_runtime = MagicMock(return_value=True)

            test_func.put()

            test_func.parent.taskqueue_stub.task_queue_put = MagicMock(
                return_value=(TaskQueuePutResponse(ok=False, task_id=""))
            )

            self.assertRaises(SystemExit, test_func.put)

    def test__call__(self):
        with mock.patch(
            "beta9.abstractions.taskqueue.TaskQueue.taskqueue_stub",
            new_callable=PropertyMock,
            return_value=MagicMock(),
        ):

            @TaskQueue(cpu=1, memory=128, image=Image(python_version="python3.8"))
            def test_func():
                return 1

            test_func.parent.taskqueue_stub.task_queue_put = MagicMock(
                return_value=(TaskQueuePutResponse(ok=True, task_id="1234"))
            )

            test_func.parent.prepare_runtime = MagicMock(return_value=True)

            self.assertRaises(
                NotImplementedError,
                test_func,
            )

            # Test calling in container
            os.environ["CONTAINER_ID"] = "1234"
            self.assertEqual(test_func(), 1)
