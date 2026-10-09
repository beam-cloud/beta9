from unittest.mock import Mock

import grpc
import pytest

from beta9.channel import rpc_timeout
from beta9.clients.function import FunctionInvokeRequest, FunctionInvokeResponse
from beta9.recovery import request_metadata, resume_invocation, retry_operation


class Unavailable(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.UNAVAILABLE


def test_retry_keeps_request_identity_and_releases_metadata(monkeypatch):
    monkeypatch.setattr("beta9.recovery.time.sleep", lambda _: None)
    identities = []

    def operation():
        identities.append(request_metadata.get()[0])
        if len(identities) == 1:
            raise Unavailable()
        return "accepted"

    assert retry_operation(operation) == "accepted"
    assert identities[0] == identities[1]
    assert request_metadata.get() == ()


def test_retry_respects_explicit_deadline():
    operation = Mock(side_effect=Unavailable())
    with rpc_timeout(0), pytest.raises(Unavailable):
        retry_operation(operation)
    operation.assert_called_once()


def test_function_reattaches_to_same_task_and_output_cursor(monkeypatch):
    monkeypatch.setattr("beta9.recovery.time.sleep", lambda _: None)
    attempts = []

    class Stub:
        def function_invoke(self, request):
            attempts.append((request.task_id, request.output_offset))
            if len(attempts) == 1:
                yield FunctionInvokeResponse(task_id="task-123")
                yield FunctionInvokeResponse(task_id="task-123", output="before\n", output_offset=7)
                raise Unavailable()
            yield FunctionInvokeResponse(task_id="task-123", output="after\n", output_offset=13)
            yield FunctionInvokeResponse(task_id="task-123", done=True, result=b"result")

    responses = list(resume_invocation(Stub(), FunctionInvokeRequest(stub_id="stub")))
    assert attempts == [("", 0), ("task-123", 7)]
    assert "".join(response.output for response in responses) == "before\nafter\n"
    assert responses[-1].result == b"result"


def test_function_does_not_start_another_task_when_identity_is_unknown():
    stub = Mock()
    stub.function_invoke.side_effect = Unavailable()
    with pytest.raises(Unavailable):
        list(resume_invocation(stub, FunctionInvokeRequest(stub_id="stub")))
    stub.function_invoke.assert_called_once()


def test_acknowledgements_stay_with_their_container():
    observed = []

    def operation():
        observed.append(dict(request_metadata.get()))
        return "ok"

    retry_operation(operation, owner="sandbox-a")
    retry_operation(operation, owner="sandbox-b")
    retry_operation(operation, owner="sandbox-a")
    assert "x-beta9-request-ack" not in observed[1]
    assert observed[2]["x-beta9-request-ack"] == observed[0]["x-beta9-request-id"]
