from types import SimpleNamespace
from unittest.mock import MagicMock
import io

import pytest
from click.testing import CliRunner

from beta9.abstractions.image import Image
from beta9.abstractions.vm import VM, prepare_image
from beta9.channel import GatewayHTTPError
from beta9.cli import extraclick
from beta9.cli import vm as vm_cli
from beta9.cli.main import load_cli


def service():
    return SimpleNamespace(channel=MagicMock(), gateway=MagicMock(), http=MagicMock())


def test_vm_image_is_deterministic_and_uses_selected_profile():
    image = Image(base_image="ubuntu:22.04").add_commands(["echo custom"])
    original = list(image.build_steps)
    selected = service()
    a = prepare_image(image, selected, True)
    b = prepare_image(image, selected, True)
    assert a.build_steps == b.build_steps
    assert image.build_steps == original
    assert a.channel is selected.channel
    assert a.gateway_stub is selected.gateway
    assert len(a.build_steps) == len(original) + 1


def test_vm_rejects_gpu_build_and_does_not_modify_explicit_image():
    selected = service()
    image = Image(base_image="ubuntu:22.04")
    image.gpu = "A10G"
    with pytest.raises(ValueError, match="cannot be built with a GPU"):
        prepare_image(image, selected, False)
    image = Image.from_id("already-built")
    prepared = prepare_image(image, selected, True)
    assert prepared.build_steps == image.build_steps
    assert prepared._explicit_image_id == "already-built"


def test_template_preserves_unspecified_fields_and_channel():
    selected = service()
    selected.http.json.return_value = {"id": "resource", "name": "dev", "status": "starting"}
    vm = VM("dev", template="base", ssh=False, cpu=2.5, env={"VALUE": "a=b c"}, _service=selected)
    vm.create(wait=False)
    body = selected.http.json.call_args.kwargs["json"]
    assert body == {
        "name": "dev",
        "template": "base",
        "spec": {"ssh": False, "cpu": 2500, "env": ["VALUE=a=b c"]},
    }
    assert vm._service is selected


def test_runtime_connection_is_invalidated_on_resume():
    vm = VM(_service=service())
    vm._set({"name": "dev", "container_id": "old"})
    vm._connected = object()
    vm._set({"name": "dev", "container_id": "old"})
    assert vm._connected is not None
    vm._set({"name": "dev", "container_id": "new"})
    assert vm._connected is None


def test_wait_checks_every_service_and_tcp_readiness(monkeypatch):
    vm = VM(_service=service())._set(
        {"name": "dev", "status": "running", "spec": {"ssh": True, "desktop": True}}
    )
    monkeypatch.setattr(vm, "refresh", lambda: vm)
    sandbox = SimpleNamespace(process=MagicMock())
    sandbox.process.exec.return_value.wait.return_value = 0
    monkeypatch.setattr(vm, "_sandbox", lambda: sandbox)
    assert vm.wait() is vm
    calls = [call.args for call in sandbox.process.exec.call_args_list]
    assert calls[:3] == [
        ("systemctl", "is-active", "--quiet", "beam-terminal.service"),
        ("systemctl", "is-active", "--quiet", "ssh.service"),
        ("systemctl", "is-active", "--quiet", "beam-desktop.service"),
    ]
    assert calls[3][:2] == ("python3", "-c")
    assert "[7681, 2222, 8080]" in calls[3][2]


@pytest.fixture
def cli_service(monkeypatch):
    selected = service()
    client = MagicMock()
    client.__enter__.return_value = selected
    monkeypatch.setattr(extraclick, "ServiceClient", lambda config: client)
    monkeypatch.setattr(extraclick, "get_config_context", lambda context: SimpleNamespace())
    return selected


def test_vm_cli_registration_and_cpu_only_args():
    cli = load_cli(check_config=False)
    command = cli.management_group.get_command(None, "vm")
    assert {
        "new",
        "list",
        "get",
        "start",
        "resume",
        "stop",
        "rm",
        "fork",
        "exec",
        "ssh",
        "scp",
        "sync",
        "desktop",
        "terminal",
        "ports",
        "expose",
        "unexpose",
        "port-forward",
        "snapshot",
        "template",
        "prompt",
        "logs",
    } <= set(command.commands)
    options = {p.name for p in command.commands["new"].params}
    assert {
        "cpu",
        "memory",
        "disk_size",
        "template",
        "desktop",
        "docker_enabled",
        "env",
        "secret",
        "ttl",
        "pool",
        "build_context",
        "context",
    } <= options
    assert not {"gpu", "gpu_count"} & options


def test_cli_preserves_env_values_and_template_defaults(cli_service, monkeypatch):
    fake = MagicMock()
    fake.info = {"name": "dev", "id": "resource", "status": "running"}
    factory = MagicMock(return_value=fake)
    monkeypatch.setattr(vm_cli, "VM", factory)
    result = CliRunner().invoke(
        vm_cli.management, ["new", "dev", "--template", "base", "--env", "VALUE=a=b c", "--json"]
    )
    assert result.exit_code == 0, result.output
    options = factory.call_args.kwargs
    assert options["env"] == {"VALUE": "a=b c"}
    assert options["cpu"] is None and options["desktop"] is None and options["ssh"] is None
    assert options["_service"] is cli_service
    fake.create.assert_called_once_with()


def test_cli_reports_api_failures_without_tracebacks(cli_service, monkeypatch):
    monkeypatch.setattr(
        vm_cli.VM,
        "get",
        MagicMock(side_effect=GatewayHTTPError(409, "VM operation already in progress")),
    )
    result = CliRunner().invoke(vm_cli.management, ["start", "dev"])
    assert result.exit_code == 1
    assert "Error: VM operation already in progress" in result.output
    assert "Traceback" not in result.output


def test_cli_rejects_no_ssh_sync_before_create(cli_service, monkeypatch, tmp_path):
    factory = MagicMock()
    monkeypatch.setattr(vm_cli, "VM", factory)
    result = CliRunner().invoke(
        vm_cli.management, ["new", "dev", "--no-ssh", "--sync", str(tmp_path)]
    )
    assert result.exit_code != 0
    assert "--sync requires SSH" in result.output
    factory.assert_not_called()


def test_ssh_preserves_remote_arguments_and_exit_status(cli_service, monkeypatch):
    vm = SimpleNamespace(name="dev")
    monkeypatch.setattr(vm_cli, "_vm", lambda *args: vm)
    monkeypatch.setattr(vm_cli, "_ssh_options", lambda vm: [])
    call = MagicMock(return_value=7)
    monkeypatch.setattr(vm_cli.subprocess, "call", call)
    result = CliRunner().invoke(vm_cli.management, ["ssh", "dev", "--", "echo", "a b;$USER"])
    assert result.exit_code == 7
    assert call.call_args.args[0] == ["ssh", "-p", "2222", "--", "root@dev", "echo 'a b;$USER'"]


def test_private_bind_does_not_create_public_url():
    selected = service()
    selected.http.json.return_value = {
        "id": "id",
        "name": "dev",
        "urls": {"7681": "terminal"},
        "spec": {"private_ports": [5432]},
    }
    vm = VM("dev", _service=selected)
    vm.bind(5432)
    assert selected.http.json.call_args.args[:2] == ("POST", "/api/v1/vm/{ws}/dev/bind")
    assert "5432" not in vm.info["urls"]


def test_ssh_helper_keeps_diagnostics_out_of_binary_transport(monkeypatch):
    output, diagnostics = io.BytesIO(), io.StringIO()
    client = MagicMock()
    monkeypatch.setattr(vm_cli, "ServiceClient", lambda config: client)
    monkeypatch.setattr(vm_cli, "get_config_context", lambda profile: SimpleNamespace())
    monkeypatch.setattr(vm_cli, "set_settings", lambda settings: None)
    monkeypatch.setattr(vm_cli, "_vm", lambda *args: SimpleNamespace())

    def bridge(vm, port, source, target):
        print("connection diagnostic")
        target.write(b"SSH-2.0-OpenSSH\r\n")

    monkeypatch.setattr(vm_cli, "_bridge", bridge)
    with monkeypatch.context() as patch:
        patch.setattr(vm_cli.sys, "stdout", SimpleNamespace(buffer=output))
        patch.setattr(vm_cli.sys, "stderr", diagnostics)
        patch.setattr(vm_cli.sys, "stdin", SimpleNamespace(buffer=io.BytesIO()))
        vm_cli._run_tunnel("local", "/tmp/config.ini", "Beam", "resource", 2222)
    assert output.getvalue() == b"SSH-2.0-OpenSSH\r\n"
    assert diagnostics.getvalue() == "connection diagnostic\n"
