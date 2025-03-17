import os
import socket
import ssl
import struct
import sys
import time
from dataclasses import dataclass
from typing import Optional

from .. import terminal
from ..env import is_local

if is_local():
    import paramiko


def create_socket(
    proxy_host: str, proxy_port: int, path: str, container_id: str, auth_token: str
) -> socket.socket:
    """
    Create a socket connection to the server and authenticate with the given token.
    """
    sock = socket.create_connection((proxy_host, proxy_port), timeout=60)
    if proxy_port == 443:
        sock = ssl.create_default_context().wrap_socket(sock=sock, server_hostname=proxy_host)

    request = (
        f"GET {path}/{container_id} HTTP/1.1\r\n"
        f"Host: {proxy_host}\r\n"
        f"Authorization: Bearer {auth_token}\r\n"
        "\r\n"
    )

    sock.sendall(request.encode())

    wait_for_ok(sock)

    return sock


def wait_for_ok(sock: socket.socket, max_retries: int = 5, delay: float = 0.25):
    """
    Wait until 'OK' is received from a socket.
    """
    for _ in range(max_retries):
        if data := sock.recv(4096).decode():
            if "OK" in data:
                return
            elif "ERROR" in data:
                raise ConnectionError(f"Error received from server: {data.lstrip('ERROR: ')}")
        delay *= 2
        time.sleep(delay)

    raise ConnectionError(f"Failed to setup socket after {max_retries} retries")


EXIT_STATUS_CTRL_C = 130
EXIT_STATUS_NOT_FOUND = 127


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
    transport: Optional["paramiko.Transport"] = None

    def _open(self):
        self.socket: Optional[socket.socket] = None
        self.channel: Optional["paramiko.Channel"] = None

        try:
            self.socket = create_socket(
                self.host,
                self.port,
                self.path,
                self.container_id,
                self.auth_token,
            )
        except BaseException:
            return terminal.error("Failed to establish ssh tunnel.")

        self.transport = paramiko.Transport(self.socket)
        self.transport.connect(username=self.username, password=self.password)
        self.channel = self.transport.open_session()

        # Get terminal size - https://stackoverflow.com/a/943921
        rows, columns = os.popen("stty size", "r").read().split()

        self.channel.get_pty(
            term=os.getenv("TERM", "xterm-256color"), width=int(columns), height=int(rows)
        )
        self.channel.invoke_shell()

    def _close(self):
        if self.channel:
            self.channel.close()

        if self.transport:
            self.transport.close()

        if self.socket:
            try:
                # Set SO_LINGER to zero to forcefully close the socket
                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
                self.socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass  # Ignore any errors that occur after the socket is already closed
            finally:
                self.socket.close()

    def __enter__(self):
        try:
            with terminal.progress("Connecting..."):
                self._open()
        except paramiko.SSHException:
            self._close()
            terminal.error("SSH error occurred.")
        except BaseException:
            self._close()
            terminal.error("Unexpected error occurred in shell.")

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self._close()

    def start(self):
        """Start the interactive shell session."""
        try:
            interactive_shell(self.channel)

            # Check the exit status after the shell session ends
            exit_status = self.channel.recv_exit_status()
            if (
                exit_status != 0
                and exit_status != EXIT_STATUS_CTRL_C
                and exit_status != EXIT_STATUS_NOT_FOUND
            ):
                terminal.warn("Lost connection to shell, attempting to reconnect in 5 seconds...")
                time.sleep(5)

                with terminal.progress("Connecting..."):
                    self._open()

                self.start()

        except paramiko.SSHException:
            self._close()
            terminal.error("SSH error occurred in shell.")
        except BaseException:
            self._close()
            terminal.error("Unexpected error occurred in shell.")


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


# https://invisible-island.net/xterm/ctlseqs/ctlseqs.html#h2-Bracketed-Paste-Mode
START_PASTE = "\x1b\x5b\x32\x30\x30\x7e"  # ESC[200~
END_PASTE = "\x1b\x5b\x32\x30\x31\x7e"  # ESC[201~

# windows does not have termios...
try:
    import termios
    import tty

    has_termios = True
except ImportError:
    has_termios = False


def is_int(val: str) -> bool:
    try:
        int(val)
        return True
    except Exception:
        return False


def interactive_shell(chan: "paramiko.Channel"):
    if has_termios:
        posix_shell(chan)
    else:
        windows_shell(chan)


def posix_readkey() -> str:
    """Get a keypress. If an escaped key is pressed, the full sequence is
    read and returned.

        Copied from readchar:
        https://github.com/magmax/python-readchar/blob/master/readchar/_posix_read.py#L30
    """

    c1 = sys.stdin.read(1)

    if c1 != "\x1b":  # ESC
        return c1

    c2 = sys.stdin.read(1)
    if c2 not in "\x4f\x5b":  # O[
        return c1 + c2

    c3 = sys.stdin.read(1)
    if c3 not in "\x31\x32\x33\x35\x36":  # 12356
        return c1 + c2 + c3

    c4 = sys.stdin.read(1)
    if c4 not in "\x30\x31\x33\x34\x35\x37\x38\x39":  # 01345789
        return c1 + c2 + c3 + c4

    c5 = sys.stdin.read(1)
    key = c1 + c2 + c3 + c4 + c5

    # Bracketed Paste Mode: # https://invisible-island.net/xterm/ctlseqs/ctlseqs.html#h2-Bracketed-Paste-Mode
    if key == START_PASTE[:-1] or key == END_PASTE[:-1]:
        c6 = sys.stdin.read(1)
        return key + c6

    return key


def windows_readkey() -> str:
    """Reads the next keypress. If an escaped key is pressed, the full
    sequence is read and returned.

        Copied from readchar:
        https://github.com/magmax/python-readchar/blob/master/readchar/_win_read.py#LL14C1-L30C24
    """

    ch = sys.stdin.read(1)

    # if it is a normal character:
    if ch not in "\x00\xe0":
        return ch

    # if it is a scpeal key, read second half:
    ch2 = sys.stdin.read(1)

    return "\x00" + ch2


def posix_shell(chan: "paramiko.Channel"):  # noqa: C901
    import select

    oldtty = termios.tcgetattr(sys.stdin)

    try:
        tty.setraw(sys.stdin.fileno())
        tty.setcbreak(sys.stdin.fileno())
        chan.settimeout(0.0)
        while True:
            r, w, e = select.select([chan, sys.stdin], [], [])
            if chan in r:
                try:
                    x = chan.recv(1024).decode("utf-8", errors="replace")
                    if len(x) == 0:
                        sys.stdout.write("\r\n")
                        break
                    sys.stdout.write(x)
                    sys.stdout.flush()
                except socket.timeout:
                    pass
            if sys.stdin in r:
                key = posix_readkey()
                # When pasting something, we need to read the entire pasted blob at once
                # Otherwise it'll hang until the next key press.
                # This has to do with how 'select.select' detects changes.
                # A paste is a single event of many characters, so we must handle them all as one event
                if key == START_PASTE:
                    # Start reading the pasted text
                    key = posix_readkey()
                    # Until we reach the end of the pasted text
                    while key != END_PASTE:
                        chan.send(key)
                        key = posix_readkey()
                    # We've exhausted the paste event, wait for next event
                    continue

                if len(key) == 0:
                    break
                chan.send(key)

    finally:
        termios.tcsetattr(sys.stdin, termios.TCSADRAIN, oldtty)


# thanks to Mike Looijmans for this code
def windows_shell(chan: "paramiko.Channel"):
    import threading

    sys.stdout.write("Line-buffered terminal emulation. Press F6 or ^Z to send EOF.\r\n\r\n")

    def writeall(sock):
        while True:
            data = sock.recv(256)
            if not data:
                sys.stdout.write("\r\n*** EOF ***\r\n\r\n")
                sys.stdout.flush()
                break
            sys.stdout.write(data)
            sys.stdout.flush()

    writer = threading.Thread(target=writeall, args=(chan,))
    writer.start()

    try:
        while True:
            d = windows_readkey()
            if not d:
                break
            chan.send(d)
    except EOFError:
        # user hit ^Z or F6
        pass
