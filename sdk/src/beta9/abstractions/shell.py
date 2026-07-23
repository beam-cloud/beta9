from __future__ import annotations

import codecs
import os
import shutil
import socket
import ssl
import sys
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional
from urllib.parse import quote, urlsplit

from .. import terminal
from ..env import is_local

if is_local():
    import paramiko


MAX_PROXY_RESPONSE_BYTES = 64 * 1024
RETRYABLE_HTTP_STATUSES = {408, 425, 429}


class ShellProxyError(ConnectionError):
    """A shell proxy rejection that callers can classify for reconnects."""

    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        retryable: bool = True,
    ) -> None:
        super().__init__(message)
        self.status_code: Optional[int] = status_code
        self.retryable: bool = retryable


@dataclass(frozen=True)
class ShellConnectionInfo:
    host: str
    port: int
    path: str
    host_header: str
    use_tls: bool


def parse_shell_connection_url(url: str) -> ShellConnectionInfo:
    """Validate and normalize a gateway-provided shell URL."""

    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError as exc:
        raise ValueError(f"Invalid shell connection URL: {exc}") from exc

    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"}:
        raise ValueError("Shell connection URL must use http or https")
    if not parsed.hostname:
        raise ValueError("Shell connection URL is missing a hostname")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("Shell connection URL must not contain user information")
    if parsed.query or parsed.fragment:
        raise ValueError("Shell connection URL must not contain a query or fragment")
    if "\r" in parsed.path or "\n" in parsed.path:
        raise ValueError("Shell connection URL contains an invalid path")
    if port is not None and port <= 0:
        raise ValueError("Shell connection URL contains an invalid port")

    return ShellConnectionInfo(
        host=parsed.hostname,
        port=port if port is not None else (443 if scheme == "https" else 80),
        path=parsed.path.rstrip("/"),
        host_header=parsed.netloc,
        use_tls=scheme == "https",
    )


def _remaining_time(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("Timed out while establishing the shell connection")
    return remaining


def _recv_before_deadline(sock: socket.socket, size: int, deadline: float) -> bytes:
    sock.settimeout(_remaining_time(deadline))
    return sock.recv(size)


def _run_before_deadline(
    operation: Callable[[], None],
    deadline: float,
    description: str,
) -> None:
    errors: List[BaseException] = []

    def run() -> None:
        try:
            operation()
        except BaseException as exc:
            errors.append(exc)

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    worker.join(timeout=_remaining_time(deadline))
    if worker.is_alive():
        raise TimeoutError(f"Timed out while {description}")
    if errors:
        raise errors[0]


def create_socket(
    proxy_host: str,
    proxy_port: int,
    path: str,
    container_id: str,
    auth_token: str,
    timeout: float = 10,
    *,
    host_header: Optional[str] = None,
    use_tls: Optional[bool] = None,
) -> socket.socket:
    """
    Create a socket connection to the server and authenticate with the given token.
    """
    deadline = time.monotonic() + timeout
    sock: Optional[socket.socket] = None
    try:
        sock = socket.create_connection((proxy_host, proxy_port), timeout=_remaining_time(deadline))
        sock.settimeout(_remaining_time(deadline))
        # Explicit scheme information is preferred. Preserve the historical
        # port-443 behavior for callers using create_socket directly.
        if use_tls is None:
            use_tls = proxy_port == 443
        if use_tls:
            sock = ssl.create_default_context().wrap_socket(
                sock=sock,
                server_hostname=proxy_host,
            )
            sock.settimeout(_remaining_time(deadline))

        request_path = f"{path.rstrip('/')}/{quote(container_id, safe='')}"
        if not request_path.startswith("/"):
            request_path = f"/{request_path}"
        request_host = host_header or proxy_host
        if any(
            "\r" in value or "\n" in value for value in (request_path, request_host, auth_token)
        ):
            raise ValueError("Invalid shell proxy request metadata")

        request = (
            f"GET {request_path} HTTP/1.1\r\n"
            f"Host: {request_host}\r\n"
            f"Authorization: Bearer {auth_token}\r\n"
            "\r\n"
        )
        sock.sendall(request.encode())
        wait_for_ok(sock, deadline=deadline)
        # create_socket is also a public helper. Preserve its historical
        # lifetime semantics; SSHShell reapplies its remaining setup deadline
        # immediately before Paramiko negotiation.
        sock.settimeout(None)
    except BaseException:
        if sock is not None:
            try:
                sock.close()
            except BaseException:
                pass
        raise

    return sock


def wait_for_ok(
    sock: socket.socket,
    timeout: float = 10,
    *,
    deadline: Optional[float] = None,
) -> None:
    """Read the two-byte tunnel preamble without consuming the SSH banner."""

    deadline = deadline or time.monotonic() + timeout
    preamble = bytearray()
    while len(preamble) < 2:
        chunk = _recv_before_deadline(sock, 2 - len(preamble), deadline)
        if not chunk:
            raise ShellProxyError("Shell proxy closed the connection during setup")
        preamble.extend(chunk)
    if bytes(preamble) == b"OK":
        return

    response = bytearray(preamble)
    if bytes(response) == b"HT":
        while len(response) < MAX_PROXY_RESPONSE_BYTES and b"\r\n\r\n" not in response:
            chunk = _recv_before_deadline(
                sock,
                min(MAX_PROXY_RESPONSE_BYTES - len(response), 4096),
                deadline,
            )
            if not chunk:
                break
            response.extend(chunk)

        header_bytes, separator, body = bytes(response).partition(b"\r\n\r\n")
        header_lines = header_bytes.decode("iso-8859-1", errors="replace").split("\r\n")
        status_line = header_lines[0].strip()
        headers: Dict[str, str] = {}
        for line in header_lines[1:]:
            name, colon, value = line.partition(":")
            if colon:
                headers[name.strip().lower()] = value.strip()

        content_length = 0
        try:
            content_length = max(0, int(headers.get("content-length", "0")))
        except ValueError:
            pass
        available_body_bytes = max(0, MAX_PROXY_RESPONSE_BYTES - len(header_bytes) - len(separator))
        body_limit = min(content_length, available_body_bytes)
        body_bytes = bytearray(body[:body_limit])
        while separator and len(body_bytes) < body_limit:
            chunk = _recv_before_deadline(
                sock,
                min(body_limit - len(body_bytes), 4096),
                deadline,
            )
            if not chunk:
                break
            body_bytes.extend(chunk)

        status_code: Optional[int] = None
        status_parts = status_line.split(" ", 2)
        if len(status_parts) >= 2:
            try:
                status_code = int(status_parts[1])
            except ValueError:
                pass
        retryable = (
            status_code is None or status_code >= 500 or status_code in RETRYABLE_HTTP_STATUSES
        )
        detail = bytes(body_bytes).decode("utf-8", errors="replace").strip()
        message = status_line or "Invalid HTTP response"
        if detail:
            message = f"{message}: {detail}"
        raise ShellProxyError(
            f"Shell proxy rejected the connection: {message}",
            status_code=status_code,
            retryable=retryable,
        )

    while len(response) < MAX_PROXY_RESPONSE_BYTES:
        try:
            chunk = _recv_before_deadline(
                sock,
                min(MAX_PROXY_RESPONSE_BYTES - len(response), 4096),
                deadline,
            )
        except (TimeoutError, socket.timeout):
            break
        if not chunk:
            break
        response.extend(chunk)
    message = bytes(response).decode("utf-8", errors="replace").strip()
    if message.startswith("ERROR:"):
        message = message[len("ERROR:") :].strip()
    raise ShellProxyError(f"Shell proxy rejected the connection: {message}")


@dataclass
class SSHShell:
    """Interactive ssh shell that can be used as a context manager - for use with 'shell' command"""

    host: str
    port: int
    path: str
    container_id: str
    stub_id: str
    auth_token: str
    username: str
    password: str
    socket: Optional[socket.socket] = None
    channel: Optional["paramiko.Channel"] = None
    transport: Optional["paramiko.Transport"] = None
    max_reconnect_attempts: int = 5
    use_tls: Optional[bool] = None
    host_header: Optional[str] = None
    setup_timeout: float = 10

    def _open(self):
        self._close()
        deadline = time.monotonic() + self.setup_timeout
        try:
            self.socket = create_socket(
                self.host,
                self.port,
                self.path,
                self.container_id,
                self.auth_token,
                timeout=_remaining_time(deadline),
                host_header=self.host_header,
                use_tls=self.use_tls,
            )
            self.socket.settimeout(_remaining_time(deadline))
            self.transport = paramiko.Transport(self.socket)
            self.transport.set_keepalive(15)
            self.transport.banner_timeout = _remaining_time(deadline)
            self.transport.start_client(timeout=_remaining_time(deadline))
            self.transport.auth_timeout = _remaining_time(deadline)
            self.transport.auth_password(username=self.username, password=self.password)
            self.transport.channel_timeout = _remaining_time(deadline)
            channel = self.transport.open_session(timeout=_remaining_time(deadline))
            self.channel = channel

            terminal_size = shutil.get_terminal_size(fallback=(80, 24))

            _run_before_deadline(
                lambda: channel.get_pty(
                    term=os.getenv("TERM", "xterm-256color"),
                    width=max(1, terminal_size.columns),
                    height=max(1, terminal_size.lines),
                ),
                deadline,
                "requesting a remote terminal",
            )
            _run_before_deadline(
                channel.invoke_shell,
                deadline,
                "starting the remote shell",
            )
            # Setup operations are deadline-bound. Once the interactive channel
            # is ready, Paramiko keepalives and the SSH protocol own its lifetime.
            self.socket.settimeout(None)
        except BaseException:
            self._close()
            raise

    def _close(self):
        channel, self.channel = self.channel, None
        transport, self.transport = self.transport, None
        sock, self.socket = self.socket, None
        for resource in (channel, transport, sock):
            if resource is None:
                continue
            try:
                resource.close()
            except BaseException:
                pass

    def __enter__(self):
        try:
            with terminal.progress("Connecting..."):
                self._open()
        except paramiko.SSHException as exc:
            self._close()
            terminal.error(f"SSH connection failed: {exc}")
        except Exception as exc:
            self._close()
            terminal.error(f"Failed to establish shell: {exc}")

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self._close()

    def _run_session(self) -> int:
        if self.channel is None:
            raise ConnectionError("Shell channel is not open")
        interactive_shell(self.channel)
        return self.channel.recv_exit_status()

    @staticmethod
    def _is_permanent_connection_error(exc: BaseException) -> bool:
        return (isinstance(exc, ShellProxyError) and not exc.retryable) or isinstance(
            exc, paramiko.AuthenticationException
        )

    def start(self) -> int:
        """Start the interactive shell session."""
        last_error: BaseException
        try:
            exit_status = self._run_session()
            if exit_status >= 0:
                return exit_status
            last_error = ConnectionError("Remote shell closed without an exit status")
        except (EOFError, OSError, paramiko.SSHException) as exc:
            last_error = exc
        self._close()

        for reconnect_attempt in range(1, self.max_reconnect_attempts + 1):
            delay = min(2 ** (reconnect_attempt - 1), 5)
            terminal.warn(
                "Lost connection to shell, attempting to reconnect "
                f"in {delay} second{'s' if delay != 1 else ''} "
                f"({reconnect_attempt}/{self.max_reconnect_attempts})..."
            )
            time.sleep(delay)
            try:
                with terminal.progress("Connecting..."):
                    self._open()
                exit_status = self._run_session()
                if exit_status >= 0:
                    return exit_status
                last_error = ConnectionError("Remote shell closed without an exit status")
            except (ConnectionError, OSError, paramiko.SSHException) as exc:
                last_error = exc
                if self._is_permanent_connection_error(exc):
                    self._close()
                    terminal.error(f"Shell is no longer available: {exc}")
            self._close()

        terminal.error(f"Failed to reconnect to shell: {last_error}")
        return -1


"""
   NOTE: much of the interactive shell code below is pulled from paramiko's examples, with a few slight modifications for use here.
   Original license / source information is as follows:
"""
# Source: https://github.com/paramiko/paramiko/blob/main/demos/interactive.py

# Copyright (C) 2003-2007  Robey Pointer <robeypointer@gmail.com>
#
# This file is part of paramiko.
#
# Paramiko is free software; you can redistribute it and/or modify it under the
# terms of the GNU Lesser General Public License as published by the Free
# Software Foundation; either version 2.1 of the License, or (at your option)
# any later version.
#
# Paramiko is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with Paramiko; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301 USA.


# windows does not have termios...
try:
    import termios
    import tty

    has_termios = True
except ImportError:
    has_termios = False


def interactive_shell(chan: "paramiko.Channel"):
    if has_termios:
        posix_shell(chan)
    else:
        windows_shell(chan)


def windows_readkey() -> str:
    """Reads the next keypress. If an escaped key is pressed, the full
    sequence is read and returned.

        Copied from readchar:
        https://github.com/magmax/python-readchar/blob/master/readchar/_win_read.py#LL14C1-L30C24
    """

    if os.name == "nt":
        import msvcrt

        ch = msvcrt.getwch()
        if ch not in "\x00\xe0":
            return ch
        return "\x00" + msvcrt.getwch()
    else:
        ch = sys.stdin.read(1)

    # if it is a normal character:
    if ch not in "\x00\xe0":
        return ch

    # if it is a scpeal key, read second half:
    ch2 = sys.stdin.read(1)

    return "\x00" + ch2


def _write_output(data: bytes, decoder) -> None:
    output_buffer = getattr(sys.stdout, "buffer", None)
    if output_buffer is not None:
        output_buffer.write(data)
    else:
        sys.stdout.write(decoder.decode(data))
    sys.stdout.flush()


def _flush_output_decoder(decoder) -> None:
    if getattr(sys.stdout, "buffer", None) is not None:
        return
    remaining = decoder.decode(b"", final=True)
    if remaining:
        sys.stdout.write(remaining)
        sys.stdout.flush()


def posix_shell(chan: "paramiko.Channel"):
    import select

    oldtty = termios.tcgetattr(sys.stdin)
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    stdin_open = True

    try:
        tty.setraw(sys.stdin.fileno())
        chan.settimeout(0.0)
        while True:
            readers = [chan]
            if stdin_open:
                readers.append(sys.stdin)
            readable, _, _ = select.select(readers, [], [])
            if chan in readable:
                try:
                    data = chan.recv(4096)
                    if not data:
                        _write_output(b"\r\n", decoder)
                        break
                    _write_output(data, decoder)
                except socket.timeout:
                    pass
            if stdin_open and sys.stdin in readable:
                data = os.read(sys.stdin.fileno(), 4096)
                if not data:
                    stdin_open = False
                    try:
                        chan.shutdown_write()
                    except (EOFError, OSError, paramiko.SSHException):
                        pass
                    continue
                chan.sendall(data)

    finally:
        _flush_output_decoder(decoder)
        try:
            termios.tcsetattr(sys.stdin, termios.TCSADRAIN, oldtty)
        except BaseException:
            pass


# thanks to Mike Looijmans for this code
def windows_shell(chan: "paramiko.Channel"):
    sys.stdout.write("Line-buffered terminal emulation. Press F6 or ^Z to send EOF.\r\n\r\n")
    sys.stdout.flush()
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    stopped = threading.Event()
    read_errors = []

    def writeall(sock):
        try:
            while True:
                data = sock.recv(4096)
                if not data:
                    _write_output(b"\r\n*** EOF ***\r\n\r\n", decoder)
                    break
                _write_output(data, decoder)
        except (EOFError, OSError, paramiko.SSHException) as exc:
            read_errors.append(exc)
        finally:
            _flush_output_decoder(decoder)
            stopped.set()

    writer = threading.Thread(target=writeall, args=(chan,), daemon=True)
    writer.start()

    try:
        while not stopped.is_set():
            if os.name == "nt":
                import msvcrt

                if not msvcrt.kbhit():
                    stopped.wait(0.05)
                    continue
            d = windows_readkey()
            if not d:
                try:
                    chan.shutdown_write()
                except (EOFError, OSError, paramiko.SSHException):
                    pass
                break
            chan.sendall(d.encode())
    except EOFError:
        # user hit ^Z or F6
        try:
            chan.shutdown_write()
        except (EOFError, OSError, paramiko.SSHException):
            pass
    finally:
        writer.join(timeout=1)

    if read_errors:
        raise read_errors[0]
