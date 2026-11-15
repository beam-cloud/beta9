"""Carry an explicitly shared laptop service over the private VM SSH tunnel."""

import os
import re
import subprocess
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Sequence

from ...utils import local_process_kwargs

_CONNECT_TIMEOUT = 20
_FORWARD_TIMEOUT = 10
_SHUTDOWN_TIMEOUT = 3


@contextmanager
def share_desktop(ssh_options: Sequence[str], target: str, local_port: int):
    """Yield the VM loopback port and its foreground SSH master until closed."""
    if not 1 <= local_port <= 65535:
        raise RuntimeError("Desktop sharing requires a valid local listener port")

    # A short private path also fits macOS's Unix socket address limit.
    with tempfile.TemporaryDirectory(prefix="beam-desktop-", dir="/tmp") as directory:
        control_path = Path(directory) / "control"
        command = ["ssh", "-F", os.devnull, "-S", str(control_path), "-p", "2222"]
        destination = ["--", target]
        with tempfile.TemporaryFile(dir=directory) as diagnostics:
            try:
                process_options = local_process_kwargs(command[0])
                master = subprocess.Popen(
                    [
                        *command,
                        "-M",
                        "-N",
                        "-T",
                        "-a",
                        "-x",
                        "-o",
                        "BatchMode=yes",
                        "-o",
                        "IdentityAgent=none",
                        "-o",
                        "ControlPersist=no",
                        "-o",
                        "ServerAliveInterval=15",
                        "-o",
                        "ServerAliveCountMax=2",
                        *ssh_options,
                        *destination,
                    ],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=diagnostics,
                    **process_options,
                )
            except OSError as exc:
                raise RuntimeError(f"Could not start OpenSSH for desktop sharing: {exc}") from exc

            try:
                deadline = time.monotonic() + _CONNECT_TIMEOUT
                while not control_path.is_socket():
                    if master.poll() is not None:
                        end = diagnostics.seek(0, os.SEEK_END)
                        diagnostics.seek(max(0, end - 2048))
                        lines = diagnostics.read().decode(errors="replace").strip().splitlines()
                        detail = (
                            lines[-1] if lines else f"SSH exited with status {master.returncode}"
                        )
                        raise RuntimeError(f"Could not connect to the VM: {detail}")
                    if time.monotonic() >= deadline:
                        raise RuntimeError("Timed out connecting to the VM for desktop sharing")
                    time.sleep(0.05)

                # OpenSSH prints the allocated remote port to stdout when
                # a forward requests port 0 via the control socket.
                try:
                    result = subprocess.run(
                        [
                            *command,
                            "-O",
                            "forward",
                            "-R",
                            f"127.0.0.1:0:127.0.0.1:{local_port}",
                            *destination,
                        ],
                        stdin=subprocess.DEVNULL,
                        capture_output=True,
                        text=True,
                        timeout=_FORWARD_TIMEOUT,
                        **process_options,
                    )
                except subprocess.TimeoutExpired as exc:
                    raise RuntimeError("Timed out opening the VM desktop share port") from exc
                except OSError as exc:
                    raise RuntimeError(f"Could not open the VM desktop share port: {exc}") from exc
                if result.returncode:
                    detail = result.stderr.strip() or f"SSH exited with status {result.returncode}"
                    raise RuntimeError(f"Could not open the VM desktop share port: {detail}")
                value = result.stdout.strip()
                if not re.fullmatch(r"[0-9]{1,5}", value) or not 1 <= int(value) <= 65535:
                    raise RuntimeError("OpenSSH returned an invalid VM desktop share port")
                yield int(value), master
            finally:
                if master.poll() is None:
                    master.terminate()
                try:
                    master.wait(timeout=_SHUTDOWN_TIMEOUT)
                except subprocess.TimeoutExpired:
                    master.kill()
                    try:
                        master.wait(timeout=_SHUTDOWN_TIMEOUT)
                    except subprocess.TimeoutExpired as exc:
                        raise RuntimeError("OpenSSH did not close the VM desktop share") from exc
