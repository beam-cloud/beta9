import subprocess
from pathlib import Path
from types import SimpleNamespace

import click
import pytest
from click.testing import CliRunner

from beta9.cli import extraclick, machine_agent
from beta9.cli.machine import management
from beta9.clients.gateway import ListMachinesResponse, Machine


@pytest.fixture
def machine_service(monkeypatch):
    machine = Machine(
        id="machine-one",
        pool_name="microvm-cpu",
        public_ip="203.0.113.7",
        tailnet_ip="100.64.0.7",
        tailnet_ssh=True,
    )
    requests = []
    machines = [machine]

    class FakeServiceClient:
        def __init__(self, _config):
            self.channel = object()
            self.gateway = self

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def list_machines(self, request):
            requests.append(request)
            return ListMachinesResponse(ok=True, machines=machines)

    monkeypatch.setattr(extraclick, "ServiceClient", FakeServiceClient)
    monkeypatch.setattr(extraclick, "get_config_context", lambda _context: SimpleNamespace())
    monkeypatch.setattr(machine_agent, "local_process_kwargs", lambda _command: {})
    monkeypatch.setattr(machine_agent.shutil, "which", lambda _command: "/usr/bin/tailscale")
    monkeypatch.setattr(machine_agent.time, "sleep", lambda _seconds: None)
    return machine, machines, requests


@pytest.mark.parametrize("use_key", [False, True])
def test_restart_agent_waits_for_new_active_invocation(
    machine_service, monkeypatch, tmp_path, use_key
):
    machine, _, requests = machine_service
    commands = []
    replies = iter(
        [
            (0, "LoadState=loaded\nInvocationID=old\n", ""),
            (0, "", ""),
            (0, "old\n", ""),
            (255, "", "connection refused"),
            (0, "new\n", ""),
        ]
    )

    def run(argv, **kwargs):
        commands.append(argv)
        code, stdout, stderr = next(replies)
        return subprocess.CompletedProcess(argv, code, stdout, stderr)

    monkeypatch.setattr(machine_agent.subprocess, "run", run)
    args = ["restart-agent", "--pool", machine.pool_name, "--context", "staging-admin"]
    key = tmp_path / "host key.pem"
    if use_key:
        key.touch()
        args += ["--ssh-key", str(key)]

    result = CliRunner().invoke(management, args)

    assert result.exit_code == 0, result.output
    assert "Agent restarted on machine-one" in result.output
    assert requests[0].pool_name == "microvm-cpu"
    assert requests[0].limit == 0
    assert len(commands) == 5
    assert "systemd-run --quiet --collect --on-active=2s" in commands[1][-1]
    assert commands[1][-1].endswith("systemctl restart beam-agent.service")
    if use_key:
        assert commands[0][:3] == ["ssh", "-i", str(key)]
        assert commands[0][-2] == "root@203.0.113.7"
    else:
        assert commands[0][:3] == ["/usr/bin/tailscale", "ssh", "root@100.64.0.7"]


def test_restart_agent_refuses_ambiguous_pool(machine_service, monkeypatch):
    _, machines, _ = machine_service
    machines.append(Machine(id="machine-two", pool_name="microvm-cpu"))
    monkeypatch.setattr(
        machine_agent.subprocess, "run", lambda *_args, **_kwargs: pytest.fail("SSH must not run")
    )

    result = CliRunner().invoke(management, ["restart-agent", "--pool", "microvm-cpu"])

    assert result.exit_code != 0
    assert "multiple machines" in result.output


def test_restart_agent_requires_enabled_tailnet_ssh_or_key(machine_service, monkeypatch):
    machine, _, _ = machine_service
    machine.tailnet_ssh = False
    monkeypatch.setattr(
        machine_agent.subprocess, "run", lambda *_args, **_kwargs: pytest.fail("SSH must not run")
    )

    result = CliRunner().invoke(management, ["restart-agent", "machine-one"])

    assert result.exit_code != 0
    assert "--ssh-key" in result.output


def test_restart_agent_reports_schedule_failure(machine_service, monkeypatch):
    machine, _, _ = machine_service
    replies = iter(
        [
            subprocess.CompletedProcess([], 0, "LoadState=loaded\nInvocationID=old\n", ""),
            subprocess.CompletedProcess([], 1, "", "permission denied"),
        ]
    )
    monkeypatch.setattr(machine_agent.subprocess, "run", lambda *_args, **_kwargs: next(replies))

    with pytest.raises(click.ClickException, match="permission denied"):
        machine_agent.restart_machine_agent(machine, None, 10)


def test_restart_agent_does_not_confirm_unchanged_invocation(machine_service, monkeypatch):
    machine, _, _ = machine_service
    replies = iter(
        [
            subprocess.CompletedProcess([], 0, "LoadState=loaded\nInvocationID=old\n", ""),
            subprocess.CompletedProcess([], 0, "", ""),
            subprocess.CompletedProcess([], 0, "old\n", ""),
        ]
    )
    monkeypatch.setattr(machine_agent.subprocess, "run", lambda *_args, **_kwargs: next(replies))
    times = iter([0, 0, 0, 11])
    monkeypatch.setattr(machine_agent.time, "monotonic", lambda: next(times))

    with pytest.raises(click.ClickException, match="not confirmed"):
        machine_agent.restart_machine_agent(machine, Path("key.pem"), 10)
