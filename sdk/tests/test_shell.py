import socket

import pytest

from beta9.abstractions.shell import SSHShell, create_socket, wait_for_ok


def test_wait_for_ok_preserves_coalesced_ssh_banner():
    client, server = socket.socketpair()
    try:
        server.sendall(b"OKSSH-2.0-dropbear\r\n")

        wait_for_ok(client)

        assert client.recv(22) == b"SSH-2.0-dropbear\r\n"
    finally:
        client.close()
        server.close()


def test_wait_for_ok_reports_http_rejection():
    client, server = socket.socketpair()
    try:
        server.sendall(
            b"HTTP/1.1 410 Gone\r\n"
            b"Content-Length: 24\r\n"
            b"\r\n"
            b"Container is not running"
        )
        server.shutdown(socket.SHUT_WR)

        with pytest.raises(ConnectionError, match="410 Gone"):
            wait_for_ok(client)
    finally:
        client.close()
        server.close()


def test_create_socket_clears_connect_timeout(monkeypatch: pytest.MonkeyPatch):
    client, server = socket.socketpair()
    client.settimeout(10)
    server.sendall(b"OK")
    monkeypatch.setattr(socket, "create_connection", lambda *_args, **_kwargs: client)

    connected = create_socket("gateway.test", 80, "/shell/id/stub", "pod-123", "token")
    try:
        assert connected.gettimeout() is None
        assert server.recv(4096).startswith(b"GET /shell/id/stub/pod-123 HTTP/1.1")
    finally:
        connected.close()
        server.close()


def test_shell_reconnects_iteratively(monkeypatch: pytest.MonkeyPatch):
    class Channel:
        def __init__(self, exit_status: int):
            self.exit_status = exit_status
            self.closed = False

        def recv_exit_status(self):
            return self.exit_status

        def close(self):
            self.closed = True

    first = Channel(-1)
    second = Channel(0)
    shell = SSHShell(
        host="gateway.test",
        port=443,
        path="/shell/id/stub",
        container_id="pod-123",
        stub_id="stub",
        auth_token="token",
        username="root",
        password="password",
        channel=first,
    )
    open_calls = 0

    def reopen():
        nonlocal open_calls
        open_calls += 1
        shell.channel = second

    monkeypatch.setattr("beta9.abstractions.shell.interactive_shell", lambda _channel: None)
    monkeypatch.setattr("beta9.abstractions.shell.time.sleep", lambda _delay: None)
    monkeypatch.setattr(shell, "_open", reopen)

    shell.start()

    assert first.closed is True
    assert second.closed is False
    assert open_calls == 1
