import io
import os
import socket
import ssl
import threading

import paramiko
import pytest

from beta9.abstractions.shell import (
    SSHShell,
    ShellProxyError,
    create_socket,
    parse_shell_connection_url,
    posix_shell,
    wait_for_ok,
    windows_shell,
)


class ScriptedSocket:
    def __init__(self, chunks):
        self.chunks = [bytearray(chunk) for chunk in chunks]
        self.timeouts = []

    def recv(self, size):
        if not self.chunks:
            return b""
        chunk = self.chunks[0]
        data = bytes(chunk[:size])
        del chunk[:size]
        if not chunk:
            self.chunks.pop(0)
        return data

    def settimeout(self, timeout):
        self.timeouts.append(timeout)


class Channel:
    def __init__(self, exit_status: int = 0, recv_chunks=None):
        self.exit_status = exit_status
        self.recv_chunks = list(recv_chunks or [])
        self.closed = False
        self.sent = []
        self.write_shutdown = False
        self.timeout = None
        self.pty = None
        self.shell_invoked = False

    def close(self):
        self.closed = True

    def get_pty(self, **kwargs):
        self.pty = kwargs

    def invoke_shell(self):
        self.shell_invoked = True

    def recv(self, _size):
        return self.recv_chunks.pop(0)

    def recv_exit_status(self):
        return self.exit_status

    def sendall(self, data):
        self.sent.append(data)

    def settimeout(self, timeout):
        self.timeout = timeout

    def shutdown_write(self):
        self.write_shutdown = True


def make_shell(channel=None, **kwargs):
    return SSHShell(
        host="gateway.test",
        port=443,
        path="/shell/id/stub",
        container_id="pod-123",
        stub_id="stub",
        auth_token="token",
        username="root",
        password="password",
        channel=channel,
        **kwargs,
    )


def test_shell_preserves_legacy_positional_optional_fields():
    sock = object()
    channel = object()
    transport = object()

    shell = SSHShell(
        "gateway.test",
        443,
        "/shell/id/stub",
        "pod-123",
        "stub",
        "token",
        "root",
        "password",
        sock,
        channel,
        transport,
        7,
    )

    assert shell.socket is sock
    assert shell.channel is channel
    assert shell.transport is transport
    assert shell.max_reconnect_attempts == 7


@pytest.mark.parametrize(
    ("url", "host", "port", "path", "host_header", "use_tls"),
    [
        (
            "https://gateway.test:8443/shell/id/stub/",
            "gateway.test",
            8443,
            "/shell/id/stub",
            "gateway.test:8443",
            True,
        ),
        (
            "http://[::1]:8080/shell/id/stub",
            "::1",
            8080,
            "/shell/id/stub",
            "[::1]:8080",
            False,
        ),
        ("http://gateway.test", "gateway.test", 80, "", "gateway.test", False),
    ],
)
def test_parse_shell_connection_url(url, host, port, path, host_header, use_tls):
    connection = parse_shell_connection_url(url)

    assert connection.host == host
    assert connection.port == port
    assert connection.path == path
    assert connection.host_header == host_header
    assert connection.use_tls is use_tls


@pytest.mark.parametrize(
    "url",
    [
        "gateway.test/shell/id/stub",
        "ssh://gateway.test/shell/id/stub",
        "https:///shell/id/stub",
        "https://user@gateway.test/shell/id/stub",
        "https://gateway.test:0/shell/id/stub",
        "https://gateway.test/shell/id/stub?token=secret",
    ],
)
def test_parse_shell_connection_url_rejects_invalid_urls(url):
    with pytest.raises(ValueError):
        parse_shell_connection_url(url)


def test_wait_for_ok_preserves_coalesced_ssh_banner():
    client, server = socket.socketpair()
    try:
        server.sendall(b"OKSSH-2.0-dropbear\r\n")

        wait_for_ok(client)

        assert client.recv(22) == b"SSH-2.0-dropbear\r\n"
    finally:
        client.close()
        server.close()


def test_wait_for_ok_reads_separately_packetized_http_body():
    sock = ScriptedSocket(
        [
            b"HTTP/1.1 410 Gone\r\nContent-Length: 24\r\n\r\n",
            b"Container is not running",
        ]
    )

    with pytest.raises(ShellProxyError, match="Container is not running") as exc_info:
        wait_for_ok(sock)

    assert exc_info.value.status_code == 410
    assert exc_info.value.retryable is False


def test_wait_for_ok_marks_server_errors_retryable():
    sock = ScriptedSocket([b"HTTP/1.1 503 Unavailable\r\nContent-Length: 4\r\n\r\n", b"busy"])

    with pytest.raises(ShellProxyError) as exc_info:
        wait_for_ok(sock)

    assert exc_info.value.status_code == 503
    assert exc_info.value.retryable is True


def test_create_socket_clears_setup_timeout_before_return(monkeypatch: pytest.MonkeyPatch):
    client, server = socket.socketpair()
    client.settimeout(10)
    server.sendall(b"OK")
    monkeypatch.setattr(socket, "create_connection", lambda *_args, **_kwargs: client)

    connected = create_socket(
        "gateway.test",
        80,
        "/shell/id/stub",
        "pod-123",
        "token",
        host_header="gateway.test:80",
        use_tls=False,
    )
    try:
        assert connected.gettimeout() is None
        request = server.recv(4096)
        assert request.startswith(b"GET /shell/id/stub/pod-123 HTTP/1.1")
        assert b"Host: gateway.test:80\r\n" in request
    finally:
        connected.close()
        server.close()


def test_create_socket_uses_explicit_tls_on_nonstandard_port(
    monkeypatch: pytest.MonkeyPatch,
):
    client, server = socket.socketpair()
    server.sendall(b"OK")
    wrapped = []

    class Context:
        def wrap_socket(self, *, sock, server_hostname):
            wrapped.append((sock, server_hostname))
            return sock

    monkeypatch.setattr(socket, "create_connection", lambda *_args, **_kwargs: client)
    monkeypatch.setattr("ssl.create_default_context", Context)

    connected = create_socket(
        "gateway.test",
        8443,
        "/shell/id/stub",
        "pod-123",
        "token",
        use_tls=True,
    )
    try:
        assert wrapped == [(client, "gateway.test")]
    finally:
        connected.close()
        server.close()


def test_create_socket_preserves_legacy_port_443_tls_default(
    monkeypatch: pytest.MonkeyPatch,
):
    client, server = socket.socketpair()
    server.sendall(b"OK")
    wrapped = []

    class Context:
        def wrap_socket(self, *, sock, server_hostname):
            wrapped.append((sock, server_hostname))
            return sock

    monkeypatch.setattr(socket, "create_connection", lambda *_args, **_kwargs: client)
    monkeypatch.setattr("ssl.create_default_context", Context)

    connected = create_socket(
        "gateway.test",
        443,
        "/shell/id/stub",
        "pod-123",
        "token",
    )
    try:
        assert wrapped == [(client, "gateway.test")]
    finally:
        connected.close()
        server.close()


def test_create_socket_does_not_infer_tls_from_port(monkeypatch: pytest.MonkeyPatch):
    client, server = socket.socketpair()
    server.sendall(b"OK")
    monkeypatch.setattr(socket, "create_connection", lambda *_args, **_kwargs: client)

    def unexpected_tls():
        raise AssertionError("TLS should be controlled by the URL scheme")

    monkeypatch.setattr("ssl.create_default_context", unexpected_tls)

    connected = create_socket(
        "gateway.test",
        443,
        "/shell/id/stub",
        "pod-123",
        "token",
        use_tls=False,
    )
    try:
        assert connected.fileno() >= 0
    finally:
        connected.close()
        server.close()


def test_create_socket_closes_socket_when_tls_setup_fails(
    monkeypatch: pytest.MonkeyPatch,
):
    client, server = socket.socketpair()

    class Context:
        def wrap_socket(self, **_kwargs):
            raise ssl.SSLError("certificate rejected")

    monkeypatch.setattr(socket, "create_connection", lambda *_args, **_kwargs: client)
    monkeypatch.setattr("ssl.create_default_context", Context)

    with pytest.raises(ssl.SSLError):
        create_socket(
            "gateway.test",
            8443,
            "/shell/id/stub",
            "pod-123",
            "token",
            use_tls=True,
        )

    assert client.fileno() == -1
    server.close()


def test_close_releases_every_resource_when_channel_close_fails():
    closed = []

    class BrokenChannel:
        def close(self):
            closed.append("channel")
            raise paramiko.SSHException("transport is gone")

    class Resource:
        def __init__(self, name):
            self.name = name

        def close(self):
            closed.append(self.name)

    shell = make_shell(
        channel=BrokenChannel(),
        transport=Resource("transport"),
        socket=Resource("socket"),
    )

    shell._close()

    assert closed == ["channel", "transport", "socket"]
    assert shell.channel is None
    assert shell.transport is None
    assert shell.socket is None


def test_open_bounds_paramiko_setup_and_clears_interactive_timeout(
    monkeypatch: pytest.MonkeyPatch,
):
    class Socket:
        def __init__(self):
            self.timeouts = []
            self.closed = False

        def settimeout(self, timeout):
            self.timeouts.append(timeout)

        def close(self):
            self.closed = True

    sock = Socket()
    channel = Channel()
    transports = []

    class Transport:
        def __init__(self, received_socket):
            assert received_socket is sock
            self.closed = False
            self.start_timeout = None
            self.open_timeout = None
            transports.append(self)

        def set_keepalive(self, _interval):
            pass

        def start_client(self, *, timeout):
            self.start_timeout = timeout

        def auth_password(self, *, username, password):
            assert (username, password) == ("root", "password")

        def open_session(self, *, timeout):
            self.open_timeout = timeout
            return channel

        def close(self):
            self.closed = True

    monkeypatch.setattr("beta9.abstractions.shell.create_socket", lambda *_args, **_kwargs: sock)
    monkeypatch.setattr("beta9.abstractions.shell.paramiko.Transport", Transport)
    monkeypatch.setattr(
        "beta9.abstractions.shell.shutil.get_terminal_size",
        lambda **_kwargs: os.terminal_size((80, 24)),
    )
    shell = make_shell(setup_timeout=2)

    shell._open()

    assert transports[0].start_timeout <= 2
    assert transports[0].open_timeout <= 2
    assert sock.timeouts[-1] is None
    assert channel.pty["width"] == 80
    assert channel.pty["height"] == 24
    assert channel.shell_invoked is True


def test_open_deadline_covers_pty_request(monkeypatch: pytest.MonkeyPatch):
    release_pty = threading.Event()

    class Socket:
        def __init__(self):
            self.closed = False

        def settimeout(self, _timeout):
            pass

        def close(self):
            self.closed = True

    class BlockingChannel(Channel):
        def get_pty(self, **kwargs):
            self.pty = kwargs
            release_pty.wait()

        def close(self):
            super().close()
            release_pty.set()

    sock = Socket()
    channel = BlockingChannel()

    class Transport:
        def __init__(self, _socket):
            self.closed = False

        def set_keepalive(self, _interval):
            pass

        def start_client(self, *, timeout):
            assert timeout > 0

        def auth_password(self, **_kwargs):
            pass

        def open_session(self, *, timeout):
            assert timeout > 0
            return channel

        def close(self):
            self.closed = True

    monkeypatch.setattr("beta9.abstractions.shell.create_socket", lambda *_args, **_kwargs: sock)
    monkeypatch.setattr("beta9.abstractions.shell.paramiko.Transport", Transport)
    shell = make_shell(setup_timeout=0.05)

    with pytest.raises(TimeoutError, match="remote terminal"):
        shell._open()

    assert channel.closed is True
    assert sock.closed is True


def test_nonnegative_remote_exit_is_final(monkeypatch: pytest.MonkeyPatch):
    channel = Channel(exit_status=42)
    shell = make_shell(channel=channel)
    monkeypatch.setattr("beta9.abstractions.shell.interactive_shell", lambda _channel: None)
    monkeypatch.setattr(
        shell,
        "_open",
        lambda: pytest.fail("A remote exit status must not trigger a reconnect"),
    )

    assert shell.start() == 42


def test_shell_reconnects_iteratively(monkeypatch: pytest.MonkeyPatch):
    first = Channel(exit_status=-1)
    second = Channel(exit_status=0)
    shell = make_shell(channel=first)
    open_calls = 0

    def reopen():
        nonlocal open_calls
        open_calls += 1
        shell.channel = second

    monkeypatch.setattr("beta9.abstractions.shell.interactive_shell", lambda _channel: None)
    monkeypatch.setattr("beta9.abstractions.shell.time.sleep", lambda _delay: None)
    monkeypatch.setattr(shell, "_open", reopen)

    assert shell.start() == 0
    assert first.closed is True
    assert second.closed is False
    assert open_calls == 1


def test_shell_stops_immediately_on_permanent_proxy_error(
    monkeypatch: pytest.MonkeyPatch,
):
    shell = make_shell(channel=Channel(exit_status=-1), max_reconnect_attempts=5)
    open_calls = 0

    def reopen():
        nonlocal open_calls
        open_calls += 1
        raise ShellProxyError(
            "HTTP/1.1 410 Gone",
            status_code=410,
            retryable=False,
        )

    monkeypatch.setattr("beta9.abstractions.shell.interactive_shell", lambda _channel: None)
    monkeypatch.setattr("beta9.abstractions.shell.time.sleep", lambda _delay: None)
    monkeypatch.setattr(shell, "_open", reopen)

    with pytest.raises(SystemExit):
        shell.start()

    assert open_calls == 1


def test_immediate_drops_exhaust_reconnect_budget(monkeypatch: pytest.MonkeyPatch):
    shell = make_shell(channel=Channel(exit_status=-1), max_reconnect_attempts=3)
    open_calls = 0

    def reopen():
        nonlocal open_calls
        open_calls += 1
        shell.channel = Channel(exit_status=-1)

    monkeypatch.setattr("beta9.abstractions.shell.interactive_shell", lambda _channel: None)
    monkeypatch.setattr("beta9.abstractions.shell.time.sleep", lambda _delay: None)
    monkeypatch.setattr(shell, "_open", reopen)

    with pytest.raises(SystemExit):
        shell.start()

    assert open_calls == 3


def _patch_posix_terminal(monkeypatch, stdin, stdout, readable):
    monkeypatch.setattr("sys.stdin", stdin)
    monkeypatch.setattr("sys.stdout", stdout)
    monkeypatch.setattr("termios.tcgetattr", lambda _stdin: object())
    monkeypatch.setattr("termios.tcsetattr", lambda *_args: None)
    monkeypatch.setattr("tty.setraw", lambda _fd: None)
    monkeypatch.setattr("select.select", lambda *_args: (next(readable), [], []))


def test_posix_shell_forwards_escape_without_waiting_for_another_key(
    monkeypatch: pytest.MonkeyPatch,
):
    class Stdin:
        def fileno(self):
            return 123

    stdin = Stdin()
    stdout = io.StringIO()
    channel = Channel(recv_chunks=[b""])
    readable = iter([[stdin], [stdin], [channel]])
    input_chunks = iter([b"\x1b", b""])
    _patch_posix_terminal(monkeypatch, stdin, stdout, readable)
    monkeypatch.setattr("os.read", lambda *_args: next(input_chunks))

    posix_shell(channel)

    assert channel.sent == [b"\x1b"]
    assert channel.write_shutdown is True


def test_posix_shell_preserves_split_utf8_output(monkeypatch: pytest.MonkeyPatch):
    class Stdin:
        def fileno(self):
            return 123

    stdin = Stdin()
    stdout = io.StringIO()
    channel = Channel(recv_chunks=[b"\xe2", b"\x82\xac", b""])
    readable = iter([[channel], [channel], [channel]])
    _patch_posix_terminal(monkeypatch, stdin, stdout, readable)

    posix_shell(channel)

    assert stdout.getvalue() == "€\r\n"


def test_windows_shell_decodes_bytes_and_joins_reader(monkeypatch: pytest.MonkeyPatch):
    channel = Channel(recv_chunks=[b"\xe2", b"\x82\xac", b""])
    stdout = io.StringIO()
    monkeypatch.setattr("sys.stdout", stdout)
    monkeypatch.setattr("beta9.abstractions.shell.windows_readkey", lambda: "")

    windows_shell(channel)

    assert "€" in stdout.getvalue()
    assert "*** EOF ***" in stdout.getvalue()
