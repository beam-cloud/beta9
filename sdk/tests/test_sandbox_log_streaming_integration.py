import os
import queue
import threading
import time
import urllib.request

import pytest

from beta9 import Image, Sandbox
from beta9.abstractions import base as base_module
from beta9.abstractions.base import unset_channel
from beta9.config import set_settings


def _close_global_channel():
    channel = getattr(base_module, "_channel", None)
    if channel is not None:
        channel.close()
    unset_channel()


def _configure_local_gateway(monkeypatch, tmp_path):
    if os.environ.get("BETA9_SANDBOX_INTEGRATION") != "1":
        pytest.skip("BETA9_SANDBOX_INTEGRATION=1 is required")

    host = os.environ.get("BETA9_INTEGRATION_GATEWAY_HOST") or "127.0.0.1"
    port = os.environ.get("BETA9_INTEGRATION_GATEWAY_PORT") or "1993"
    token = os.environ.get("BETA9_INTEGRATION_TOKEN") or os.environ.get("BETA9_TOKEN")
    if token == "test-token":
        token = None

    monkeypatch.setenv("CONFIG_PATH", str(tmp_path / "config.ini"))
    monkeypatch.setenv("BETA9_GATEWAY_HOST", host)
    monkeypatch.setenv("BETA9_GATEWAY_PORT", port)
    if token:
        monkeypatch.setenv("BETA9_TOKEN", token)
    else:
        monkeypatch.delenv("BETA9_TOKEN", raising=False)

    set_settings()
    _close_global_channel()


def _wait_public_url_contains(url, *values):
    token = os.environ.get("BETA9_TOKEN")
    deadline = time.monotonic() + 90
    last_error = None
    last_body = ""

    while time.monotonic() < deadline:
        try:
            request = urllib.request.Request(url)
            if token:
                request.add_header("Authorization", f"Bearer {token}")
            with urllib.request.urlopen(request, timeout=5) as response:
                last_body = response.read().decode()
            if all(value in last_body for value in values):
                return last_body
        except Exception as exc:
            last_error = exc
        time.sleep(0.5)

    raise AssertionError(
        f"timed out waiting for {url} to contain {values}: body={last_body!r} error={last_error!r}"
    )


def test_python_sdk_sandbox_log_streaming_live(monkeypatch, tmp_path):
    _configure_local_gateway(monkeypatch, tmp_path)

    script = (
        "printf 'py-stdout-start\\n'; "
        "sleep 3; "
        "printf 'py-stderr-middle\\n' >&2; "
        "sleep 1; "
        "printf 'py-stdout-end\\n'; "
        "printf 'py-stderr-end\\n' >&2"
    )

    pool = os.environ.get("BETA9_INTEGRATION_POOL")
    kwargs = {"pool": pool} if pool else {}
    sandbox = Sandbox(
        name="python-sdk-log-streaming",
        image=Image(python_version="python3.11"),
        keep_warm_seconds=300,
        **kwargs,
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
        _close_global_channel()


def test_python_sdk_sandbox_expose_port_listener_families(monkeypatch, tmp_path):
    _configure_local_gateway(monkeypatch, tmp_path)

    pool = os.environ.get("BETA9_INTEGRATION_POOL")
    kwargs = {"pool": pool} if pool else {}
    sandbox = Sandbox(
        name="python-sdk-listener-families",
        image=Image(python_version="python3.11"),
        keep_warm_seconds=300,
        ports=[8092, 8093],
        **kwargs,
    )
    instance = sandbox.create()
    servers = []
    try:
        instance.fs.create_directory("/workspace/py-sdk")
        local_file = tmp_path / "file.txt"
        local_file.write_text("python-sdk-listener-ok\n")
        instance.fs.upload_file(str(local_file), "/workspace/py-sdk/file.txt")

        servers.append(
            instance.process.exec(
                "python3",
                "-m",
                "http.server",
                "8092",
                "--bind",
                "0.0.0.0",
                "--directory",
                "/workspace/py-sdk",
            )
        )
        servers.append(
            instance.process.exec(
                "python3",
                "-m",
                "http.server",
                "8093",
                "--bind",
                "::",
                "--directory",
                "/workspace/py-sdk",
            )
        )

        ipv4_url = instance.expose_port(8092)
        _wait_public_url_contains(f"{ipv4_url}/file.txt", "python-sdk-listener-ok")

        ipv6_url = instance.expose_port(8093)
        _wait_public_url_contains(f"{ipv6_url}/file.txt", "python-sdk-listener-ok")
    finally:
        for server in servers:
            try:
                server.kill()
            except Exception:
                pass
        instance.terminate()
        _close_global_channel()
