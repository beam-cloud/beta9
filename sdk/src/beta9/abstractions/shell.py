import os
import socket
import sys
from dataclasses import dataclass

from ..env import is_local

if is_local():
    import paramiko


def create_connect_tunnel(
    proxy_host: str, proxy_port: int, stub_id: str, container_id: str, auth_token: str
) -> socket.socket:
    """
    1. Connect to the proxy_host:proxy_port over TCP.
    2. Send an HTTP CONNECT request for the correct path.
    3. If we get a 200 response, return the socket as a raw tunnel.
    """

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((proxy_host, proxy_port))

    # Construct the correct CONNECT request path
    connect_path = f"/shell/{stub_id}/{container_id}"

    connect_req = (
        f"CONNECT {connect_path} HTTP/1.1\r\n"
        f"Host: {proxy_host}:{proxy_port}\r\n"
        f"Proxy-Connection: Keep-Alive\r\n"
        f"Authorization: Bearer {auth_token}\r\n"
        f"\r\n"
    )
    s.sendall(connect_req.encode("ascii"))

    response = b""
    while b"\r\n\r\n" not in response:
        chunk = s.recv(4096)
        if not chunk:
            break
        response += chunk

    response_str = response.decode("ascii", errors="replace")
    if "200 OK" not in response_str:
        s.close()
        raise Exception(f"CONNECT failed. Response:\n{response_str}")

    # If we reach here, we have a raw TCP tunnel to "container_id"
    return s


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

    # input_history = []

    try:
        tty.setraw(sys.stdin.fileno())
        tty.setcbreak(sys.stdin.fileno())
        chan.settimeout(0.0)
        while True:
            r, w, e = select.select([chan, sys.stdin], [], [])
            if chan in r:
                try:
                    x = chan.recv(1024).decode()
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
                        # input_history.append(key)
                        key = posix_readkey()
                    # We've exhausted the paste event, wait for next event
                    continue

                if len(key) == 0:
                    break
                chan.send(key)
                # input_history.append(key)

    finally:
        termios.tcsetattr(sys.stdin, termios.TCSADRAIN, oldtty)

    # Useful in debugging how control characters were send
    # print(input_history)


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


@dataclass
class SSHShell:
    """Interactive SSHShell that can be used as a contextmanager"""

    channel: "paramiko.Channel"

    def connect(self):
        self._open()
        self._launch()
        self._close()

    def _open(self):
        # Get terminal size - https://stackoverflow.com/a/943921
        rows, columns = os.popen("stty size", "r").read().split()

        self.channel.get_pty(
            term=os.getenv("TERM", "xterm-256color"), width=int(columns), height=int(rows)
        )
        self.channel.invoke_shell()

        self._channel = self.channel

        return self._channel

    def _launch(self):
        interactive_shell(self._channel)

    def _close(self):
        pass

    def __enter__(
        self,
    ):
        return self._open()

    def __exit__(self, exception_type, exception_value, traceback):
        self._launch()
        self._close()
