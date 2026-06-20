import asyncio
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


class StreamStub:
    def __init__(self, stdout=None, stderr=None):
        self.stdout = list(stdout or [])
        self.stderr = list(stderr or [])

    def _unary_unary(self, path, *_args, **_kwargs):
        def call(_request, **_call_kwargs):
            if path.endswith("SandboxStdout"):
                return SimpleNamespace(ok=True, stdout=self._pop(self.stdout))
            if path.endswith("SandboxStderr"):
                return SimpleNamespace(ok=True, stderr=self._pop(self.stderr))
            if path.endswith("SandboxStatus"):
                return SimpleNamespace(ok=True, exit_code=0, status="exited")
            raise AssertionError(f"unexpected RPC path: {path}")

        return call

    @staticmethod
    def _pop(chunks):
        return chunks.pop(0) if chunks else ""


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
    sandbox = SimpleNamespace(
        container_id="sandbox-123", stub=StreamStub(["line one\npartial"], [""])
    )
    process = SandboxProcess(sandbox, pid=42, cwd="/workspace", args=[], env={})
    logs = process.logs

    assert next(logs) == "line one\n"
    assert logs.read() == "partial"


def test_combined_logs_iterator_reads_late_stderr_after_exit():
    sandbox = SimpleNamespace(
        container_id="sandbox-123",
        stub=StreamStub(["combined-stdout\n", ""], ["", "combined-stderr\n", ""]),
    )
    process = SandboxProcess(sandbox, pid=42, cwd="/workspace", args=[], env={})

    assert "".join(process.logs) == "combined-stdout\ncombined-stderr\n"


def stream_process(container_id="sandbox-123"):
    sandbox = SimpleNamespace(
        container_id=container_id,
        stub=StreamStub(["stdout-line\nstdout-tail", ""], ["stderr-line\nstderr-tail", ""]),
    )
    return SandboxProcess(sandbox, pid=42, cwd="/workspace", args=[], env={})


def test_split_stdout_stderr_streams_iterate_read_and_consume_independently():
    process = stream_process()

    assert next(process.stdout) == "stdout-line\n"
    assert process.stdout.read() == "stdout-tail"
    assert process.stdout.read() == ""

    assert next(process.stderr) == "stderr-line\n"
    assert process.stderr.read() == "stderr-tail"
    assert process.stderr.read() == ""


def test_combined_logs_read_returns_buffered_line_then_consumes_streams():
    process = stream_process()

    assert next(process.logs) == "stdout-line\n"
    assert process.logs.read() == "stderr-line\nstdout-tailstderr-tail"
    assert process.logs.read() == ""


def test_async_process_streams_wrap_sync_stdout_stderr_and_logs():
    async def run():
        process = stream_process()

        stdout_line = await process.aio.stdout.__anext__()
        stdout_tail = await process.aio.stdout.read()
        stderr = await process.aio.stderr.read()

        process = stream_process("sandbox-456")
        first_log = await process.aio.logs.__anext__()
        remaining_logs = await process.aio.logs.read()

        return stdout_line, stdout_tail, stderr, first_log, remaining_logs

    assert asyncio.run(run()) == (
        "stdout-line\n",
        "stdout-tail",
        "stderr-line\nstderr-tail",
        "stdout-line\n",
        "stderr-line\nstdout-tailstderr-tail",
    )


@pytest.mark.parametrize(
    "error_msg",
    [
        "Process manager not ready within timeout",
        "Failed to connect to sandbox",
    ],
)
def test_exec_retries_transient_readiness_errors(monkeypatch, error_msg):
    stub = ExecReadyRetryStub(error_msg)
    sandbox = SimpleNamespace(container_id="sandbox-123", stub=stub)
    manager = SandboxProcessManager(sandbox)
    monkeypatch.setattr(sandbox_module, "SANDBOX_EXEC_READY_RETRY_DELAY_SECONDS", 0)

    process = manager.exec("echo", "ok")

    assert process.pid == 123
    assert stub.calls == 2
