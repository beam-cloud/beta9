import hashlib
import io
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock

import grpc
import pytest

from beta9.abstractions.function import _Invocation
from beta9.abstractions.sandbox import SandboxFileSystem
from beta9.channel import (
    Channel,
    GatewayHTTP,
    ServiceClient,
    request_metadata,
    retry_operation,
    rpc_timeout,
)
from beta9.clients.function import FunctionInvokeRequest, FunctionInvokeResponse


class TestChannelIdentity(TestCase):
    def test_cache_key_identifies_gateway_and_token(self):
        # Per-channel caches (image existence, synced objects) are shared by
        # channels to the same gateway and token, and by nothing else.
        channels = [
            Channel("localhost:1993", token="t1"),
            Channel("localhost:1993", token="t1"),
            Channel("localhost:1993", token="t2"),
            Channel("other:1993", token="t1"),
        ]
        try:
            a, b, c, d = (ch.cache_key for ch in channels)
            self.assertEqual(a, b)
            self.assertNotEqual(a, c)
            self.assertNotEqual(a, d)
        finally:
            for ch in channels:
                ch.close()


@pytest.mark.parametrize(
    "address, secure", [("localhost:443", True), ("[::1]:443", True), ("127.0.0.1:46443", False)]
)
def test_channel_infers_tls_only_for_port_443(monkeypatch, address, secure):
    tls = Mock(return_value=Mock(spec=grpc.Channel))
    plaintext = Mock(return_value=Mock(spec=grpc.Channel))
    monkeypatch.setattr(grpc, "secure_channel", tls)
    monkeypatch.setattr(grpc, "insecure_channel", plaintext)
    channel = Channel(address)
    try:
        assert tls.called is secure
        assert plaintext.called is not secure
    finally:
        channel.close()


class TestGatewayHTTP(TestCase):
    def test_sequential_requests_reuse_connection_and_preserve_identity(self):
        requests = []

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self):
                requests.append((self.client_address, self.path, self.headers["Authorization"]))
                self.send_response(200)
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"{}")

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        client = GatewayHTTP(f"http://127.0.0.1:{server.server_port}", "workspace", "test-token")
        try:
            self.assertEqual(client.json("GET", "/{ws}/first"), {})
            self.assertEqual(client.json("GET", "/{ws}/second"), {})
            self.assertEqual(requests[0][0], requests[1][0])
            self.assertEqual([r[1] for r in requests], ["/workspace/first", "/workspace/second"])
            self.assertEqual([r[2] for r in requests], ["Bearer test-token"] * 2)
        finally:
            client.close()
            server.shutdown()
            server.server_close()
            thread.join()


@pytest.mark.parametrize("supported", [False, True])
def test_http_transport_preserves_vm_capability_from_gateway(supported):
    from beta9.clients.gateway import ExportWorkspaceConfigResponse
    from beta9.config import ConfigContext

    with ServiceClient(ConfigContext(token="test-token")) as service:
        service._gateway = Mock()
        service._gateway.export_workspace_config.return_value = ExportWorkspaceConfigResponse(
            workspace_id="workspace",
            gateway_http_host="gateway.example",
            gateway_http_port=443,
            gateway_http_tls=True,
            persistent_vms=supported,
        )
        assert service.http.persistent_vms is supported
        assert service.http is service.http
        service._gateway.export_workspace_config.assert_called_once()


def test_direct_vm_transport_requires_api_for_another_gateway():
    from beta9.config import ConfigContext

    with ServiceClient(ConfigContext(token="test-token", gateway_host="custom.example")) as service:
        assert service.vm_http is None
    with ServiceClient(
        ConfigContext(
            token="test-token", gateway_host="custom.example", api_url="https://custom.example/"
        )
    ) as service:
        assert service.vm_http.base_url == "https://custom.example"
        assert service.vm_http.workspace_id == "self"


class Unavailable(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.UNAVAILABLE


def test_retry_keeps_request_identity_and_releases_metadata(monkeypatch):
    monkeypatch.setattr("beta9.channel.time.sleep", lambda _: None)
    identities = []

    def operation():
        identities.append(request_metadata.get()[0])
        if len(identities) == 1:
            raise Unavailable()
        return "accepted"

    assert retry_operation(operation) == "accepted"
    assert identities[0] == identities[1]
    assert request_metadata.get() == ()


@pytest.mark.parametrize("recovery_timeout", [120, None])
def test_retry_respects_explicit_deadline(recovery_timeout):
    operation = Mock(side_effect=Unavailable())
    with rpc_timeout(0), pytest.raises(Unavailable):
        retry_operation(operation, timeout=recovery_timeout)
    operation.assert_called_once()


@pytest.mark.parametrize("caller_timeout", [None, 10])
def test_filesystem_upload_recovers_after_thirty_seconds(monkeypatch, caller_timeout):
    from contextlib import nullcontext

    now = [0.0]
    monkeypatch.setattr("beta9.channel.time.monotonic", lambda: now[0])
    monkeypatch.setattr("beta9.channel.time.sleep", lambda _: None)
    attempts = []

    def upload(request):
        def attempt():
            attempts.append(request)
            if len(attempts) == 1:
                now[0] = 35
                raise Unavailable()
            return SimpleNamespace(ok=True)

        return retry_operation(attempt, owner="sandbox")

    process = Mock()
    process.exec.return_value.wait.return_value = 0
    process.exec.return_value.stdout = io.StringIO(hashlib.sha256(b"payload").hexdigest())
    instance = SimpleNamespace(
        container_id="sandbox",
        process=process,
        stub=SimpleNamespace(sandbox_upload_file=upload, sandbox_delete_file=Mock()),
    )
    scope = rpc_timeout(caller_timeout) if caller_timeout else nullcontext()
    with scope:
        if caller_timeout:
            with pytest.raises(Unavailable):
                SandboxFileSystem(instance).write_bytes("/file", b"payload")
            assert len(attempts) == 1
            process.exec.assert_not_called()
        else:
            SandboxFileSystem(instance).write_bytes("/file", b"payload")
            assert len(attempts) == 2
            assert attempts[0] is attempts[1]


@pytest.mark.parametrize("supports_resume", [True, False])
@pytest.mark.parametrize(
    "failure", [grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.INTERNAL, grpc.StatusCode.UNKNOWN]
)
def test_function_reattaches_to_same_task_and_output_cursor(monkeypatch, supports_resume, failure):
    from concurrent.futures import ThreadPoolExecutor
    from beta9.clients.function import FunctionServiceStub

    monkeypatch.setattr("beta9.channel.time.sleep", lambda _: None)
    attempts = []

    def invoke(request, context):
        headers = dict(context.invocation_metadata())
        attempts.append((headers["x-beta9-task-id"], int(headers["x-beta9-log-offset"])))
        if len(attempts) == 1:
            if supports_resume:
                yield FunctionInvokeResponse(task_id="task-123")
            yield FunctionInvokeResponse(task_id="task-123", output="before\n")
            context.abort(
                failure,
                "Stream removed"
                if failure == grpc.StatusCode.UNKNOWN
                else "Received RST_STREAM with error code 2",
            )
        yield FunctionInvokeResponse(task_id="task-123", output="after\n")
        yield FunctionInvokeResponse(task_id="task-123", done=True, result=b"result")

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
    try:
        invocation = _Invocation(
            FunctionServiceStub(channel), FunctionInvokeRequest(stub_id="stub")
        )
        if not supports_resume:
            with pytest.raises(grpc.RpcError):
                list(invocation)
            assert attempts == [("", 0)]
            return
        responses = []
        for response in invocation:
            assert request_metadata.get() == ()
            responses.append(response)
        assert attempts == [("", 0), ("task-123", 7)]
        assert "".join(response.output for response in responses) == "before\nafter\n"
        assert responses[-1].result == b"result"
    finally:
        channel.close()
        server.stop(0).wait()


def test_function_does_not_start_another_task_when_identity_is_unknown():
    stub = Mock()
    stub.function_invoke.side_effect = Unavailable()
    with pytest.raises(Unavailable):
        list(_Invocation(stub, FunctionInvokeRequest(stub_id="stub")))
    stub.function_invoke.assert_called_once()


@pytest.mark.parametrize("status", [grpc.StatusCode.INTERNAL, grpc.StatusCode.UNKNOWN])
def test_recovery_does_not_retry_application_internal_errors(status):
    class Internal(grpc.RpcError):
        def code(self):
            return status

        def details(self):
            return "invalid application response"

    operation = Mock(side_effect=Internal())
    with pytest.raises(Internal):
        retry_operation(operation)
    operation.assert_called_once()


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


@pytest.mark.parametrize(
    "failure",
    [grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNKNOWN],
)
def test_generated_sandbox_stub_retries_in_the_shared_channel(monkeypatch, failure):
    from concurrent.futures import ThreadPoolExecutor
    from beta9.clients.pod import PodServiceStub, PodSandboxExecRequest, PodSandboxExecResponse

    monkeypatch.setattr("beta9.channel.time.sleep", lambda _: None)
    calls = []

    def execute(request, context):
        calls.append(dict(context.invocation_metadata()))
        if len(calls) == 1:
            context.abort(failure, "Stream removed")
        return PodSandboxExecResponse(ok=True, pid=42)

    server = grpc.server(ThreadPoolExecutor(max_workers=2))
    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                "pod.PodService",
                {
                    "SandboxExec": grpc.unary_unary_rpc_method_handler(
                        execute,
                        request_deserializer=PodSandboxExecRequest().parse,
                        response_serializer=bytes,
                    )
                },
            ),
        )
    )
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    channel = Channel(f"127.0.0.1:{port}", token="token", retry=(lambda _: None, False))
    try:
        result = PodServiceStub(channel).sandbox_exec(
            PodSandboxExecRequest(container_id="sandbox", command="once")
        )
        assert result.pid == 42
        assert len(calls) == 2
        assert calls[0]["x-beta9-request-id"] == calls[1]["x-beta9-request-id"]
        assert calls[1]["authorization"] == "Bearer token"
    finally:
        channel.close()
        server.stop(0).wait()


def test_recovery_does_not_retry_cancellation(monkeypatch):
    monkeypatch.setattr("beta9.channel.time.sleep", Mock())

    class Cancelled(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.CANCELLED

    operation = Mock(side_effect=Cancelled())
    with pytest.raises(Cancelled):
        retry_operation(operation)
    operation.assert_called_once()
    from beta9.channel import time

    time.sleep.assert_not_called()


def test_runner_finalization_recovers_from_removed_stream(monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from beta9.clients.gateway import (
        EndTaskRequest,
        EndTaskResponse,
        GatewayServiceStub,
    )
    from beta9.runner.common import end_task

    monkeypatch.setattr("beta9.channel.time.sleep", lambda _: None)
    calls = []

    def complete(request, context):
        calls.append((request, dict(context.invocation_metadata())))
        if len(calls) == 1:
            context.abort(grpc.StatusCode.UNKNOWN, "Stream removed")
        return EndTaskResponse(ok=True)

    server = grpc.server(ThreadPoolExecutor(max_workers=2))
    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                "gateway.GatewayService",
                {
                    "EndTask": grpc.unary_unary_rpc_method_handler(
                        complete,
                        request_deserializer=EndTaskRequest().parse,
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
        request = EndTaskRequest(task_id="task", container_id="function")
        assert end_task(GatewayServiceStub(channel), request).ok
        assert len(calls) == 2
        assert calls[0][0] == calls[1][0] == request
        assert calls[0][1]["x-beta9-request-id"] == calls[1][1]["x-beta9-request-id"]
    finally:
        channel.close()
        server.stop(0).wait()


@pytest.mark.parametrize("method", ["ImageExists", "ExportWorkspaceConfig"])
def test_preparation_reads_reconnect_after_gateway_restart(monkeypatch, method):
    from beta9.channel import AuthTokenInterceptor

    monkeypatch.setattr("beta9.channel.time.sleep", lambda _: None)
    details = Mock(method=f"/gateway.GatewayService/{method}", timeout=None, metadata=None)
    details._replace.return_value = details
    unavailable = Mock()
    unavailable.result.side_effect = Unavailable()
    completed = Mock()
    continuation = Mock(side_effect=[unavailable, completed])
    result = AuthTokenInterceptor().intercept_unary_unary(continuation, details, Mock())
    assert result is completed
    assert continuation.call_count == 2


def test_function_retry_changes_container_and_resets_log_cursor(monkeypatch):
    monkeypatch.setattr("beta9.channel.time.sleep", lambda _: None)
    headers = []

    def invoke(_request):
        headers.append(dict(request_metadata.get()))
        attempt = len(headers)
        if attempt == 3:
            yield FunctionInvokeResponse(task_id="task", done=True, result=b"result")
            return
        yield FunctionInvokeResponse(task_id="task", container_id=f"container-{attempt}")
        yield FunctionInvokeResponse(task_id="task", output="first" if attempt == 1 else "second")
        raise ConnectionError("gateway restarted")

    responses = list(_Invocation(SimpleNamespace(function_invoke=invoke), FunctionInvokeRequest()))
    assert headers[1]["x-beta9-container-id"] == "container-1"
    assert headers[1]["x-beta9-log-offset"] == "5"
    assert headers[2]["x-beta9-container-id"] == "container-2"
    assert headers[2]["x-beta9-log-offset"] == "6"
    assert "".join(response.output for response in responses) == "firstsecond"
    assert responses[-1].result == b"result"


def test_function_result_cache_replays_identical_payload_with_deadline(monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from beta9.clients.function import (
        FunctionServiceStub,
        FunctionSetResultRequest,
        FunctionSetResultResponse,
    )

    monkeypatch.setattr("beta9.channel.time.sleep", lambda _: None)
    calls = []

    def set_result(request, context):
        calls.append((bytes(request), context.time_remaining()))
        if len(calls) == 1:
            context.abort(grpc.StatusCode.UNAVAILABLE, "gateway restarting")
        return FunctionSetResultResponse(ok=True)

    server = grpc.server(ThreadPoolExecutor(max_workers=2))
    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                "function.FunctionService",
                {
                    "FunctionSetResult": grpc.unary_unary_rpc_method_handler(
                        set_result,
                        request_deserializer=FunctionSetResultRequest().parse,
                        response_serializer=bytes,
                    )
                },
            ),
        )
    )
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    channel = Channel(f"127.0.0.1:{port}")
    try:
        request = FunctionSetResultRequest(
            task_id="task", container_id="container", result=b"result"
        )
        with rpc_timeout(10):
            assert FunctionServiceStub(channel).function_set_result(request).ok
        assert len(calls) == 2
        assert calls[0][0] == calls[1][0] == bytes(request)
        assert all(0 < remaining <= 11 for _, remaining in calls)
    finally:
        channel.close()
        server.stop(0).wait()
