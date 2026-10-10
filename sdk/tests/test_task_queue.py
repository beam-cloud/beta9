import os
from types import SimpleNamespace
from unittest import TestCase, mock
from unittest.mock import MagicMock, PropertyMock

import pytest

from beta9 import Image
from beta9.abstractions.taskqueue import TaskQueue
from beta9.clients.taskqueue import TaskQueueCompleteResponse, TaskQueuePutResponse
from beta9.runner.taskqueue import Task, TaskQueueWorker


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
            test_func.parent.get_client = MagicMock(
                return_value=MagicMock().assign_attr(
                    "get_task_by_id",
                    MagicMock(return_value=MagicMock()),
                )
            )

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
            test_func.parent.get_client = MagicMock(
                return_value=MagicMock().assign_attr(
                    "get_task_by_id",
                    MagicMock(return_value=MagicMock()),
                )
            )

            self.assertRaises(
                NotImplementedError,
                test_func,
            )

            # Test calling in container
            os.environ["CONTAINER_ID"] = "1234"
            self.assertEqual(test_func(), 1)


@pytest.mark.parametrize(
    "return_value,expected_result,expected_callback_payload",
    [
        (False, b"false", False),
        (0, b"0", 0),
        ("", b'""', ""),
        ([], b"[]", []),
        ({}, b"{}", {}),
        (None, None, {}),
        (True, b"true", True),
        ({"status": "ok"}, b'{"status": "ok"}', {"status": "ok"}),
    ],
)
def test_taskqueue_worker_preserves_falsy_and_truthy_results(
    return_value, expected_result, expected_callback_payload
):
    worker = TaskQueueWorker(
        worker_index=0,
        parent_pid=123,
        worker_startup_event=mock.MagicMock(),
        workers_ready=mock.MagicMock(),
    )
    mock_taskqueue_stub = mock.MagicMock()
    mock_gateway_stub = mock.MagicMock()
    mock_send_callback = mock.MagicMock()

    worker._get_next_task = mock.MagicMock(
        side_effect=[
            Task(id="task-1", args=[], kwargs={}),
            None,
        ]
    )
    worker._monitor_task = mock.MagicMock()

    mock_handler = mock.MagicMock(return_value=return_value)
    mock_handler.parent_abstraction = mock.MagicMock(retry_for=[])

    def side_effect_complete(req):
        worker.should_exit = True
        return TaskQueueCompleteResponse(ok=True)

    mock_taskqueue_stub.task_queue_complete.side_effect = side_effect_complete

    mock_cfg = SimpleNamespace(
        checkpoint_enabled=False,
        stub_id="stub-1",
        stub_type="task_queue",
        python_version="python3.13",
        container_id="c-1",
        container_hostname="host-1",
        keep_warm_seconds=10,
        task_id="task-1",
        on_start_value=None,
        callback_url=None,
        bind_port=8080,
        timeout=300,
    )

    with (
        mock.patch("beta9.runner.taskqueue.TaskQueueServiceStub", return_value=mock_taskqueue_stub),
        mock.patch("beta9.runner.taskqueue.GatewayServiceStub", return_value=mock_gateway_stub),
        mock.patch("beta9.runner.taskqueue.FunctionHandler", return_value=mock_handler),
        mock.patch("beta9.runner.taskqueue.execute_lifecycle_method", return_value=None),
        mock.patch("beta9.runner.taskqueue.signal.signal"),
        mock.patch("beta9.runner.taskqueue.time.sleep"),
        mock.patch("beta9.runner.taskqueue.send_callback", mock_send_callback),
        mock.patch("beta9.runner.taskqueue.cfg", mock_cfg),
        mock.patch("beta9.runner.taskqueue.config", mock_cfg),
    ):
        worker.process_tasks.__wrapped__(worker, channel=mock.MagicMock())

    mock_taskqueue_stub.task_queue_complete.assert_called_once()
    req = mock_taskqueue_stub.task_queue_complete.call_args[0][0]
    assert req.result == expected_result

    mock_send_callback.assert_called_once()
    assert mock_send_callback.call_args.kwargs["payload"] == expected_callback_payload


@pytest.mark.parametrize(
    "payload,expected_payload",
    [
        (False, False),
        (0, 0),
        ("", ""),
        ([], []),
        ({}, {}),
        (None, None),
        (True, True),
        ({"status": "ok"}, {"status": "ok"}),
    ],
)
def test_send_callback_delivers_falsy_and_truthy_payloads(payload, expected_payload):
    mock_gateway_stub = mock.MagicMock()
    mock_sign_resp = mock.MagicMock(signature="test-sig", timestamp=1234567890)
    mock_context = SimpleNamespace(
        task_id="task-1",
        callback_url="https://callback.example.com",
    )
    mock_post = mock.MagicMock()
    mock_post.return_value.raise_for_status = mock.MagicMock()

    with (
        mock.patch("beta9.runner.common._call_gateway_unary", return_value=mock_sign_resp),
        mock.patch("requests.post", mock_post),
    ):
        from beta9.runner.common import send_callback

        send_callback(
            gateway_stub=mock_gateway_stub,
            context=mock_context,
            payload=payload,
            task_status="COMPLETE",
        )

    mock_post.assert_called_once()
    assert mock_post.call_args.kwargs["json"]["data"] == expected_payload
    assert mock_post.call_args.kwargs["headers"]["X-Task-ID"] == "task-1"
    assert mock_post.call_args.kwargs["headers"]["X-Task-Status"] == "COMPLETE"
    assert mock_post.call_args.kwargs["headers"]["X-Task-Signature"] == "test-sig"
