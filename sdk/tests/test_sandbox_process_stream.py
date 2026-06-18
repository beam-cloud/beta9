from types import SimpleNamespace

import pytest

from beta9.abstractions import sandbox as sandbox_module
from beta9.abstractions.sandbox import (
    SandboxProcess,
    SandboxProcessManager,
    SandboxProcessStream,
)
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


class LateStderrStub:
    def __init__(self):
        self.stdout_chunks = ["combined-stdout\n", ""]
        self.stderr_chunks = ["", "combined-stderr\n", ""]

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


class ExecReadyRetryStub:
    def __init__(self, error_msg="Process manager not ready within timeout"):
        self.calls = 0
        self.error_msg = error_msg

    def _unary_unary(self, path, *_args, **_kwargs):
        def call(_request, **_call_kwargs):
            if path.endswith("SandboxExec"):
                self.calls += 1
                if self.calls == 1:
                    return SimpleNamespace(
                        ok=False,
                        pid=-1,
                        error_msg=self.error_msg,
                    )
                return SimpleNamespace(ok=True, pid=123, error_msg="")
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


def test_combined_logs_iterator_reads_late_stderr_after_exit():
    sandbox = SimpleNamespace(container_id="sandbox-123", stub=LateStderrStub())
    process = SandboxProcess(sandbox, pid=42, cwd="/workspace", args=[], env={})

    assert "".join(process.logs) == "combined-stdout\ncombined-stderr\n"


@pytest.mark.parametrize(
    "error_msg",
    [
        "Process manager not ready within timeout",
        "Failed to connect to sandbox",
    ],
)
def test_exec_retries_process_manager_not_ready(monkeypatch, error_msg):
    stub = ExecReadyRetryStub(error_msg)
    sandbox = SimpleNamespace(container_id="sandbox-123", stub=stub)
    manager = SandboxProcessManager(sandbox)
    monkeypatch.setattr(sandbox_module, "SANDBOX_EXEC_READY_RETRY_DELAY_SECONDS", 0)

    process = manager.exec("echo", "ok")

    assert process.pid == 123
    assert stub.calls == 2
