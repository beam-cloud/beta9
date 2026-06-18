import os
import queue
import threading
import time

import pytest

from beta9 import Image, Sandbox


def _configure_local_gateway(monkeypatch):
    if os.environ.get("BETA9_SANDBOX_INTEGRATION") != "1":
        pytest.skip("BETA9_SANDBOX_INTEGRATION=1 is required")

    host = (
        os.environ.get("BETA9_INTEGRATION_GATEWAY_HOST")
        or os.environ.get("BETA9_GATEWAY_HOST")
        or "127.0.0.1"
    )
    port = (
        os.environ.get("BETA9_INTEGRATION_GATEWAY_PORT")
        or os.environ.get("BETA9_GATEWAY_PORT")
        or "1993"
    )
    token = os.environ.get("BETA9_INTEGRATION_TOKEN") or os.environ.get("BETA9_TOKEN")

    monkeypatch.setenv("BETA9_GATEWAY_HOST", host)
    monkeypatch.setenv("BETA9_GATEWAY_PORT", port)
    if token:
        monkeypatch.setenv("BETA9_TOKEN", token)
        return

    monkeypatch.delenv("BETA9_TOKEN", raising=False)


def test_python_sdk_sandbox_log_streaming_live(monkeypatch):
    _configure_local_gateway(monkeypatch)

    script = (
        "printf 'py-stdout-start\\n'; "
        "sleep 3; "
        "printf 'py-stderr-middle\\n' >&2; "
        "sleep 1; "
        "printf 'py-stdout-end\\n'; "
        "printf 'py-stderr-end\\n' >&2"
    )

    sandbox = Sandbox(
        name="python-sdk-log-streaming",
        image=Image(python_version="python3.11"),
        keep_warm_seconds=300,
        pool=os.environ.get("BETA9_INTEGRATION_POOL", "gvisor"),
    )
    instance = sandbox.create()
    try:
        process = instance.process.exec("sh", "-lc", script)
        started = time.monotonic()
        events = queue.Queue()

        def read_stream(name, stream):
            try:
                for chunk in stream:
                    events.put((name, chunk, time.monotonic() - started))
            except BaseException as exc:
                events.put((name, f"ERROR:{exc}", time.monotonic() - started))

        threads = [
            threading.Thread(target=read_stream, args=("stdout", process.stdout)),
            threading.Thread(target=read_stream, args=("stderr", process.stderr)),
        ]
        for thread in threads:
            thread.start()

        exit_code = process.wait(timeout=30)
        for thread in threads:
            thread.join(timeout=10)

        rows = []
        while not events.empty():
            rows.append(events.get())

        stdout = "".join(chunk for stream, chunk, _ in rows if stream == "stdout")
        stderr = "".join(chunk for stream, chunk, _ in rows if stream == "stderr")
        first_stdout = min(
            (at for stream, chunk, at in rows if stream == "stdout" and "py-stdout-start" in chunk),
            default=None,
        )

        assert exit_code == 0
        assert first_stdout is not None and first_stdout < 2, rows
        assert "py-stdout-start" in stdout and "py-stdout-end" in stdout
        assert "py-stderr-middle" in stderr and "py-stderr-end" in stderr

        logs_process = instance.process.exec(
            "sh", "-lc", "printf 'py-log-out\\n'; printf 'py-log-err\\n' >&2"
        )
        combined = "".join(logs_process.logs)
        assert logs_process.wait(timeout=30) == 0
        assert "py-log-out" in combined and "py-log-err" in combined
    finally:
        instance.terminate()
