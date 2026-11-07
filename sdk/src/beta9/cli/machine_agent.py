import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional
from uuid import uuid4

import click

from ..clients.gateway import Machine
from ..utils import local_process_kwargs


def _agent_ssh_command(machine: Machine, ssh_key: Optional[Path]) -> List[str]:
    if ssh_key is not None:
        if not machine.public_ip:
            raise click.ClickException("This machine has no public IP for SSH with --ssh-key.")
        return [
            "ssh",
            "-i",
            str(ssh_key),
            "-o",
            "IdentitiesOnly=yes",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=10",
            "-o",
            "StrictHostKeyChecking=accept-new",
            "root@" + machine.public_ip,
        ]
    if not machine.tailnet_ssh or not (machine.tailnet_hostname or machine.tailnet_ip):
        raise click.ClickException(
            "Tailscale SSH is not enabled on this machine. Use --ssh-key for public-IP SSH."
        )
    tailscale = shutil.which("tailscale")
    if tailscale is None and sys.platform == "darwin":
        app = Path("/Applications/Tailscale.app/Contents/MacOS/Tailscale")
        if app.is_file():
            tailscale = str(app)
    if tailscale is None:
        raise click.ClickException("Install the Tailscale CLI or use --ssh-key for public-IP SSH.")
    return [tailscale, "ssh", "root@" + (machine.tailnet_ip or machine.tailnet_hostname)]


def restart_machine_agent(machine: Machine, ssh_key: Optional[Path], timeout: int) -> None:
    ssh = _agent_ssh_command(machine, ssh_key)

    def run(command: str):
        return subprocess.run(
            [*ssh, command],
            capture_output=True,
            text=True,
            timeout=15,
            **local_process_kwargs(ssh[0]),
        )

    try:
        before = run(
            "systemctl show --property=LoadState --property=InvocationID beam-agent.service"
        )
        if before.returncode:
            raise click.ClickException(
                before.stderr.strip() or "Unable to inspect the agent service."
            )
        properties = dict(line.split("=", 1) for line in before.stdout.splitlines() if "=" in line)
        if properties.get("LoadState") != "loaded":
            raise click.ClickException("beam-agent.service is not installed on this machine.")
        invocation = properties.get("InvocationID", "")
        # The agent owns the Tailscale connection. Schedule the restart after
        # this SSH session exits, then reconnect to verify a new invocation.
        scheduled = run(
            "systemd-run --quiet --collect --on-active=2s "
            f"--unit=beam-agent-restart-{uuid4().hex[:12]} systemctl restart beam-agent.service"
        )
        if scheduled.returncode:
            raise click.ClickException(
                scheduled.stderr.strip() or "Unable to schedule agent restart."
            )
    except (OSError, subprocess.SubprocessError) as exc:
        raise click.ClickException(f"Unable to request agent restart: {exc}") from exc

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        time.sleep(min(3, max(0, deadline - time.monotonic())))
        try:
            after = run(
                "systemctl is-active --quiet beam-agent.service && "
                "systemctl show --property=InvocationID --value beam-agent.service"
            )
        except (OSError, subprocess.SubprocessError):
            continue
        current = after.stdout.strip()
        if after.returncode == 0 and current and current != invocation:
            return
    raise click.ClickException(
        f"Restart was scheduled, but a new active agent was not confirmed within {timeout}s."
    )
