"""A call whose container the scheduler gives up on ends by saying why."""

import json
from concurrent.futures import ThreadPoolExecutor

import grpc
import pytest

from beta9 import terminal
from beta9.abstractions.function import _Invocation
from beta9.channel import (
    SCHEDULING_FAILURE_TRAILER,
    Channel,
    handle_grpc_error,
    scheduling_failure,
)
from beta9.clients.function import (
    FunctionInvokePending,
    FunctionInvokeRequest,
    FunctionInvokeResponse,
    FunctionServiceStub,
)


def invocation_that_ends(code, message, trailer=None):
    """Invoke a function on a gateway that accepts it, says it waits, and ends the stream."""
    attempts = []

    def invoke(request, context):
        attempts.append(dict(context.invocation_metadata()).get("x-beta9-task-id"))
        yield FunctionInvokeResponse(task_id="task-123", container_id="function-1")
        yield FunctionInvokeResponse(
            task_id="task-123",
            pending=FunctionInvokePending(
                phase="waiting_for_capacity", reason="no_capacity", waited_seconds=3
            ),
        )
        if trailer:
            context.set_trailing_metadata(((SCHEDULING_FAILURE_TRAILER, trailer),))
        context.abort(code, message)

    server = grpc.server(ThreadPoolExecutor(max_workers=2))
    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                "function.FunctionService",
                {
                    "FunctionInvoke": grpc.unary_stream_rpc_method_handler(
                        invoke,
                        request_deserializer=FunctionInvokeRequest().parse,
                        response_serializer=bytes,
                    )
                },
            ),
        )
    )
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    channel = Channel(f"127.0.0.1:{port}", retry=(lambda _: None, False))
    responses = []
    try:
        with pytest.raises(grpc.RpcError) as raised:
            for response in _Invocation(
                FunctionServiceStub(channel), FunctionInvokeRequest(stub_id="stub")
            ):
                responses.append(response)
    finally:
        channel.close()
        server.stop(0).wait()
    return raised.value, responses, attempts


NO_A10G = "No A10G capacity became available in 20 minutes."


def test_a_call_the_scheduler_gave_up_on_is_not_tried_again():
    error, responses, attempts = invocation_that_ends(
        grpc.StatusCode.RESOURCE_EXHAUSTED, NO_A10G, "worker_capacity_timeout"
    )

    assert error.code() == grpc.StatusCode.RESOURCE_EXHAUSTED
    assert error.details() == NO_A10G
    assert scheduling_failure(error) == "worker_capacity_timeout"
    # It was told what it waited for, and the gateway was asked once.
    assert [r.pending.phase for r in responses] == ["", "waiting_for_capacity"]
    assert attempts == [""]


def reported(monkeypatch, capsys, error, json_mode):
    monkeypatch.setattr(terminal, "json_output", lambda: json_mode)
    with pytest.raises(SystemExit) as exit:
        handle_grpc_error(error)
    assert exit.value.code == 1
    return capsys.readouterr()


def test_no_capacity_is_reported_as_what_the_gateway_said(monkeypatch, capsys):
    error, _, _ = invocation_that_ends(
        grpc.StatusCode.RESOURCE_EXHAUSTED, NO_A10G, "worker_capacity_timeout"
    )

    printed = reported(monkeypatch, capsys, error, json_mode=False)
    lines = printed.err.splitlines()
    assert lines[0] == f"✗ {NO_A10G}"
    assert "Resource limit exceeded" not in printed.err
    hint = " ".join(line.strip() for line in lines[1:])
    assert hint.startswith("hint: Try again in a few minutes.")
    assert "machine list" in hint


def test_no_capacity_has_a_code_a_script_can_branch_on(monkeypatch, capsys):
    error, _, _ = invocation_that_ends(
        grpc.StatusCode.RESOURCE_EXHAUSTED, NO_A10G, "worker_capacity_timeout"
    )

    payload = json.loads(reported(monkeypatch, capsys, error, json_mode=True).out)
    assert payload["error"] == NO_A10G
    assert payload["code"] == "CAPACITY"
    assert "gpu=[" in payload["hint"]


def test_another_scheduling_failure_is_reported_as_what_the_gateway_said(monkeypatch, capsys):
    message = 'The selected compute pool "gone" is unavailable or has no active controller.'
    error, _, _ = invocation_that_ends(grpc.StatusCode.UNKNOWN, message, "no_controller")

    payload = json.loads(reported(monkeypatch, capsys, error, json_mode=True).out)
    assert payload == {"error": message, "code": "NOT_SCHEDULED"}


def test_a_limit_that_is_not_the_schedulers_is_reported_as_before(monkeypatch, capsys):
    error, _, _ = invocation_that_ends(grpc.StatusCode.RESOURCE_EXHAUSTED, "too many tunnels")

    assert scheduling_failure(error) == ""
    payload = json.loads(reported(monkeypatch, capsys, error, json_mode=True).out)
    assert payload == {"error": "Resource limit exceeded: too many tunnels", "code": "CAPACITY"}
