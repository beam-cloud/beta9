from concurrent.futures import ThreadPoolExecutor
from threading import Event
from types import SimpleNamespace
from unittest.mock import Mock, patch

import grpc
import pytest

from beta9.channel import Channel
from beta9.clients.taskqueue import (
    TaskQueueCompleteRequest,
    TaskQueueCompleteResponse,
    TaskQueueMonitorResponse,
    TaskQueueServiceStub,
)
from beta9.runner.taskqueue import Task, TaskQueueWorker, complete_task
from beta9.type import TaskExitCode, TaskStatus


class Unavailable(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.UNAVAILABLE


def completion_request(status=TaskStatus.Complete):
    return TaskQueueCompleteRequest(
        task_id="task",
        stub_id="stub",
        container_id="container",
        task_status=status,
        task_duration=96.3,
        result=b"original serialized result",
    )


def test_completion_keeps_result_until_persistence_is_acknowledged(monkeypatch):
    now = [0.0]
    monkeypatch.setattr("beta9.channel.time.monotonic", lambda: now[0])
    monkeypatch.setattr("beta9.channel.time.sleep", lambda delay: None)
    requests = []

    def complete(request):
        requests.append(request)
        now[0] += 10
        if len(requests) < 4:
            raise Unavailable()
        return TaskQueueCompleteResponse(ok=True)

    request = completion_request()
    assert complete_task(SimpleNamespace(task_queue_complete=complete), request).ok
    assert now[0] < 120
    assert len(requests) == 4
    assert all(attempt is request for attempt in requests)
    assert request.result == b"original serialized result"


@pytest.mark.parametrize("persisted_before_disconnect", [False, True])
def test_completion_replays_same_result_after_gateway_replacement(persisted_before_disconnect):
    started, disconnected = Event(), Event()
    requests = []
    persisted = []

    def old_gateway(request, context):
        requests.append(request)
        if persisted_before_disconnect:
            persisted.append(bytes(request))
        context.add_callback(disconnected.set)
        started.set()
        disconnected.wait(10)
        return TaskQueueCompleteResponse(ok=True)

    def new_gateway(request, context):
        requests.append(request)
        # Model the server's terminal-task replay guard; Go tests verify it
        # against the real completion handler and Redis repository.
        if not persisted:
            persisted.append(bytes(request))
        return TaskQueueCompleteResponse(ok=True)

    def gateway(handler, address):
        server = grpc.server(ThreadPoolExecutor(max_workers=2))
        server.add_generic_rpc_handlers(
            (
                grpc.method_handlers_generic_handler(
                    "taskqueue.TaskQueueService",
                    {
                        "TaskQueueComplete": grpc.unary_unary_rpc_method_handler(
                            handler,
                            request_deserializer=TaskQueueCompleteRequest().parse,
                            response_serializer=bytes,
                        ),
                    },
                ),
            )
        )
        port = server.add_insecure_port(address)
        assert port
        server.start()
        return server, port

    old_server, port = gateway(old_gateway, "127.0.0.1:0")
    channel = Channel(f"127.0.0.1:{port}", retry=(lambda _: None, False))
    new_server = None
    executor = ThreadPoolExecutor(max_workers=1)
    try:
        request = completion_request()
        finished = executor.submit(complete_task, TaskQueueServiceStub(channel), request)
        assert started.wait(5)
        # Drop the in-flight RPC and replace the gateway behind the same address.
        old_server.stop(0).wait()
        assert disconnected.wait(5)
        new_server, _ = gateway(new_gateway, f"127.0.0.1:{port}")
        assert finished.result(timeout=15).ok
        assert len(requests) == 2
        assert all(bytes(attempt) == bytes(request) for attempt in requests)
        assert persisted == [bytes(request)]
    finally:
        channel.close()
        old_server.stop(0).wait()
        if new_server is not None:
            new_server.stop(0).wait()
        disconnected.set()
        executor.shutdown(wait=True)


def test_completion_does_not_replay_a_request_to_execute_user_code_again():
    stub = SimpleNamespace(task_queue_complete=Mock(side_effect=Unavailable()))
    with pytest.raises(Unavailable):
        complete_task(stub, completion_request(TaskStatus.Retry))
    stub.task_queue_complete.assert_called_once()


def test_monitor_reconnects_without_exiting_the_task(monkeypatch):
    stub = SimpleNamespace(
        task_queue_monitor=Mock(
            side_effect=[Unavailable()] * 8
            + [
                [TaskQueueMonitorResponse(ok=True, complete=True)],
            ]
        )
    )
    exit_task = Mock(side_effect=AssertionError("monitor disconnect must not exit user code"))
    monkeypatch.setattr("beta9.runner.taskqueue.os._exit", exit_task)
    monkeypatch.setattr("beta9.runner.taskqueue.time.sleep", lambda _: None)
    worker = TaskQueueWorker.__new__(TaskQueueWorker)
    worker._monitor_task(
        context=SimpleNamespace(),
        stub_id="stub",
        container_id="container",
        task=Task(id="task"),
        taskqueue_stub=stub,
        gateway_stub=Mock(),
    )
    assert stub.task_queue_monitor.call_count == 9
    exit_task.assert_not_called()


@pytest.mark.parametrize(
    "flag,task_status,exit_code",
    [
        ("cancelled", TaskStatus.Cancelled, TaskExitCode.Cancelled),
        ("timed_out", TaskStatus.Timeout, TaskExitCode.Timeout),
    ],
)
def test_shared_monitor_preserves_cancellation_and_timeout(
    monkeypatch, flag, task_status, exit_code
):
    from beta9.runner import common

    callback, exit_task = Mock(), Mock()
    monkeypatch.setattr(common, "send_callback", callback)
    context = SimpleNamespace(task_id="task")
    gateway = Mock()
    common.monitor_task(
        context,
        lambda: [TaskQueueMonitorResponse(**{flag: True})],
        gateway,
        exit_task,
    )
    callback.assert_called_once_with(
        gateway_stub=gateway, context=context, payload={}, task_status=task_status
    )
    exit_task.assert_called_once_with(exit_code)


def test_shared_monitor_reopens_a_cleanly_closed_stream(monkeypatch):
    from beta9.runner import common

    monkeypatch.setattr("beta9.channel.time.sleep", lambda _: None)
    streams = Mock(side_effect=[[], [TaskQueueMonitorResponse(complete=True)]])
    exit_task = Mock()
    common.monitor_task(SimpleNamespace(task_id="task"), streams, Mock(), exit_task)
    assert streams.call_count == 2
    exit_task.assert_not_called()


def test_completion_recovery_has_a_total_budget(monkeypatch):
    now = [0.0]
    monkeypatch.setattr("beta9.channel.time.monotonic", lambda: now[0])
    monkeypatch.setattr("beta9.channel.time.sleep", lambda _: None)
    calls = []

    def unavailable(request):
        calls.append(request)
        now[0] += 180
        raise Unavailable()

    with pytest.raises(Unavailable):
        complete_task(SimpleNamespace(task_queue_complete=unavailable), completion_request())
    assert len(calls) == 1


def test_negative_completion_acknowledgment_is_not_retried():
    operation = Mock(return_value=TaskQueueCompleteResponse(ok=False))
    with pytest.raises(RuntimeError):
        complete_task(SimpleNamespace(task_queue_complete=operation), completion_request())
    operation.assert_called_once()


@pytest.mark.parametrize(
    "code",
    [
        grpc.StatusCode.NOT_FOUND,
        grpc.StatusCode.FAILED_PRECONDITION,
        grpc.StatusCode.PERMISSION_DENIED,
    ],
)
def test_permanent_completion_rejection_is_not_retried(code):
    class Rejected(grpc.RpcError):
        def code(self):
            return code

    operation = Mock(side_effect=Rejected())
    with pytest.raises(Rejected):
        complete_task(SimpleNamespace(task_queue_complete=operation), completion_request())
    operation.assert_called_once()


def test_permanent_monitor_rejection_stops_the_runner():
    from beta9.runner import common

    class Deleted(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.NOT_FOUND

    stream, exit_task = Mock(side_effect=Deleted()), Mock()
    with pytest.raises(Deleted):
        common.monitor_task(SimpleNamespace(task_id="task"), stream, Mock(), exit_task)
    stream.assert_called_once()
    exit_task.assert_called_once_with(TaskExitCode.Error)


def test_hung_completion_rpc_consumes_the_total_recovery_budget(monkeypatch):
    from beta9.runner import common

    monkeypatch.setattr(common, "RECOVERY_TIMEOUT", 0.1)
    disconnected = Event()
    attempts = []

    def complete(request, context):
        attempts.append(request)
        context.add_callback(disconnected.set)
        disconnected.wait(5)
        return TaskQueueCompleteResponse(ok=True)

    server = grpc.server(ThreadPoolExecutor(max_workers=2))
    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                "taskqueue.TaskQueueService",
                {
                    "TaskQueueComplete": grpc.unary_unary_rpc_method_handler(
                        complete,
                        request_deserializer=TaskQueueCompleteRequest().parse,
                        response_serializer=bytes,
                    )
                },
            ),
        )
    )
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    channel = Channel(f"127.0.0.1:{port}", retry=(lambda _: None, False))
    try:
        with pytest.raises(grpc.RpcError) as error:
            complete_task(TaskQueueServiceStub(channel), completion_request())
        assert error.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
        assert disconnected.wait(2)
        assert len(attempts) == 1
    finally:
        disconnected.set()
        channel.close()
        server.stop(0).wait()


def test_completed_attempt_cancels_actual_monitor_rpc_without_exiting_container():
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    from beta9.channel import Channel
    from beta9.clients.taskqueue import TaskQueueMonitorRequest, TaskQueueServiceStub
    from beta9.runner.common import TaskMonitor

    entered, cancelled = Event(), Event()

    def server_monitor(request, context):
        context.add_callback(cancelled.set)
        entered.set()
        while context.is_active():
            yield TaskQueueMonitorResponse(ok=True)
            cancelled.wait(0.01)

    executor = ThreadPoolExecutor(max_workers=2)
    server = grpc.server(executor)
    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                "taskqueue.TaskQueueService",
                {
                    "TaskQueueMonitor": grpc.unary_stream_rpc_method_handler(
                        server_monitor,
                        request_deserializer=TaskQueueMonitorRequest().parse,
                        response_serializer=bytes,
                    )
                },
            ),
        )
    )
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    channel = Channel(f"127.0.0.1:{port}")
    monitor = TaskMonitor()
    exit_task = Mock()
    worker = TaskQueueWorker.__new__(TaskQueueWorker)
    try:
        with patch("beta9.runner.taskqueue.os._exit", exit_task):
            future = executor.submit(
                worker._monitor_task,
                context=SimpleNamespace(),
                stub_id="stub",
                container_id="container",
                task=Task(id="task"),
                taskqueue_stub=TaskQueueServiceStub(channel),
                gateway_stub=Mock(),
                monitor=monitor,
            )
            assert entered.wait(5)
            monitor.stop()
            future.result(timeout=5)
            assert cancelled.wait(5)
            exit_task.assert_not_called()
    finally:
        monitor.stop()
        channel.close()
        server.stop(0).wait()
        executor.shutdown(wait=True)
