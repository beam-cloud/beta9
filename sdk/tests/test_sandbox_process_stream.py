from types import SimpleNamespace

import pytest

from beta9.abstractions.sandbox import SandboxProcess, SandboxProcessStream
from beta9.exceptions import SandboxProcessError


class FakeProcess:
    def status(self):
        return 0, "exited"


class FakeStub:
    def __init__(self, response):
        self.response = response

    def _unary_unary(self, *_args, **_kwargs):
        def call(_request, **_call_kwargs):
            return self.response

        return call


class SequencedStub:
    def __init__(self):
        self.stdout_chunks = ["line one\npartial"]
        self.stderr_chunks = [""]

    def _unary_unary(self, path, *_args, **_kwargs):
        def call(_request, **_call_kwargs):
            if path.endswith("SandboxStdout"):
                value = self.stdout_chunks.pop(0) if self.stdout_chunks else ""
                return SimpleNamespace(ok=True, stdout=value)
            if path.endswith("SandboxStderr"):
                value = self.stderr_chunks.pop(0) if self.stderr_chunks else ""
                return SimpleNamespace(ok=True, stderr=value)
            if path.endswith("SandboxStatus"):
                return SimpleNamespace(ok=True, exit_code=0, status="exited")
            raise AssertionError(f"unexpected RPC path: {path}")

        return call


def test_process_stream_read_propagates_fetch_errors():
    def fetch():
        raise SandboxProcessError("stdout unavailable")

    stream = SandboxProcessStream(FakeProcess(), fetch)

    with pytest.raises(SandboxProcessError, match="stdout unavailable"):
        stream.read()


def test_process_stdout_checks_rpc_ok():
    sandbox = SimpleNamespace(
        container_id="sandbox-123",
        stub=FakeStub(SimpleNamespace(ok=False, error_msg="stdout failed", stdout="")),
    )
    process = SandboxProcess(sandbox, pid=42, cwd="/workspace", args=[], env={})

    with pytest.raises(SandboxProcessError, match="stdout failed"):
        process._stdout()


def test_process_stderr_checks_rpc_ok():
    sandbox = SimpleNamespace(
        container_id="sandbox-123",
        stub=FakeStub(SimpleNamespace(ok=False, error_msg="stderr failed", stderr="")),
    )
    process = SandboxProcess(sandbox, pid=42, cwd="/workspace", args=[], env={})

    with pytest.raises(SandboxProcessError, match="stderr failed"):
        process._stderr()


def test_combined_logs_read_preserves_buffered_partial_line():
    sandbox = SimpleNamespace(container_id="sandbox-123", stub=SequencedStub())
    process = SandboxProcess(sandbox, pid=42, cwd="/workspace", args=[], env={})
    logs = process.logs

    assert next(logs) == "line one\n"
    assert logs.read() == "partial"
