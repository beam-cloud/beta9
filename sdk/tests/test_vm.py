from types import SimpleNamespace
from unittest.mock import MagicMock
from concurrent.futures import ThreadPoolExecutor
import json
import io
import shutil
import subprocess
import threading

import pytest
from click.testing import CliRunner

from beta9.abstractions.image import Image
from beta9.abstractions.vm import VM, prepare_image
from beta9.abstractions import vm as vm_module
from beta9.exceptions import SandboxProcessError, SandboxConnectionError
from beta9.channel import GatewayHTTPError
from beta9.cli import extraclick
from beta9.cli import vm as vm_cli
from beta9.cli import container as container_cli
from beta9.cli.main import load_cli


def service():
    return SimpleNamespace(
        channel=MagicMock(), gateway=MagicMock(), http=MagicMock(), close=MagicMock()
    )


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


def test_vm_cached_image_lookup_is_quiet(monkeypatch, capsys):
    from beta9.abstractions.image import ImageBuildResult

    image = prepare_image(Image.from_id("cached-vm"), service(), False)
    result = ImageBuildResult(success=True, image_id="cached-vm")
    monkeypatch.setattr(image, "_cached_build_result", lambda key: None)
    monkeypatch.setattr(image, "_exists", lambda: (True, result))
    assert image.build().image_id == "cached-vm"
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("status", [401, 404, 503])
def test_create_checks_api_before_keys_or_image_build(monkeypatch, status):
    selected = service()
    selected.http.base_url = "https://app.stage.beam.cloud"
    selected.http.json.side_effect = GatewayHTTPError(status, "Not Found")
    build = MagicMock()
    key = MagicMock()
    monkeypatch.setattr("beta9.abstractions.vm.prepare_image", build)
    monkeypatch.setattr("beta9.abstractions.vm.identity", key)
    with pytest.raises(GatewayHTTPError) as error:
        VM(_service=selected).create()
    assert error.value.status == status
    if status == 404:
        assert "Persistent VMs are unavailable at https://app.stage.beam.cloud" in str(error.value)
        assert "--context" in str(error.value)
    else:
        assert str(error.value) == "Not Found"
    build.assert_not_called()
    key.assert_not_called()
    selected.http.json.assert_called_once_with("GET", "/api/v1/vm/{ws}", timeout=240)


def test_template_preserves_unspecified_fields_and_channel():
    selected = service()
    selected.http.json.return_value = {
        "id": "resource",
        "name": "dev",
        "status": "starting",
    }
    vm = VM(
        "dev",
        template="base",
        ssh=False,
        cpu=2.5,
        env={"VALUE": "a=b c"},
        _service=selected,
    )
    vm.create(wait=False)
    body = selected.http.json.call_args.kwargs["json"]
    assert body == {
        "name": "dev",
        "template": "base",
        "spec": {"ssh": False, "cpu": 2500, "env": ["VALUE=a=b c"]},
        "request_id": vm.request_id,
    }
    assert vm._service is selected


def test_prepare_freezes_request_without_allocating_compute():
    selected = service()
    vm = VM("dev", template="base", ssh=False, _service=selected)
    assert vm.prepare() is vm
    assert [call.args[0] for call in selected.http.json.call_args_list] == ["GET"]
    body = json.loads(json.dumps(vm._creation_body))
    vm._spec["cpu"] = 9000
    vm.prepare()
    assert vm._creation_body == body
    assert selected.http.json.call_count == 1


def test_verified_launch_reuses_ready_connection_for_first_operation():
    selected = service()
    vm = VM(_service=selected)._set(
        {
            "name": "dev",
            "status": "running",
            "container_id": "runtime",
            "stub_id": "stub",
            "spec": {"idle_timeout": 0},
            "exec_ready": True,
        }
    )
    assert vm._sandbox() is vm._connected
    selected.http.json.assert_not_called()


def test_ready_connection_refreshes_after_lease_to_observe_external_stop(monkeypatch):
    selected = service()
    clock = [10.0]
    monkeypatch.setattr(vm_module.time, "monotonic", lambda: clock[0])
    vm = VM(_service=selected)._set(
        {
            "name": "dev",
            "status": "running",
            "container_id": "runtime",
            "stub_id": "stub",
            "spec": {"idle_timeout": 0},
            "exec_ready": True,
        }
    )
    selected.http.json.return_value = {
        "name": "dev",
        "status": "stopped",
        "container_id": "",
        "spec": {},
    }
    clock[0] += 1.1
    with pytest.raises(SandboxConnectionError, match="not running"):
        vm._sandbox()
    assert vm._connected is None
    selected.http.json.assert_called_once()


def test_retry_after_lost_creation_response_reuses_exact_request():
    selected = service()
    selected.http.json.side_effect = [
        [],
        TimeoutError("lost response"),
        {"id": "resource", "name": "dev", "status": "starting"},
    ]
    vm = VM("dev", template="base", ssh=False, _service=selected)
    with pytest.raises(TimeoutError):
        vm.create(wait=False)
    vm.create(wait=False)
    calls = selected.http.json.call_args_list
    assert calls[1].kwargs["json"] == calls[2].kwargs["json"]
    assert calls[1].kwargs["json"]["request_id"] == vm.request_id
    assert len(calls) == 3


def test_update_can_clear_metadata_and_network_rules():
    selected = service()
    selected.http.json.return_value = {"id": "resource", "name": "dev", "spec": {}}
    vm = VM("dev", _service=selected)
    vm.update(ttl=0, auto_resume=False, metadata={})
    assert selected.http.json.call_args.kwargs["json"] == {
        "idle_timeout": 0,
        "auto_resume": False,
        "metadata": {},
    }
    vm.update_network_permissions()
    assert selected.http.json.call_args.kwargs["json"] == {
        "block_network": False,
        "allow_list": [],
    }


def test_metadata_list_filter_and_status_use_gateway_query():
    selected = service()
    VM.list(metadata={"user": "a=b"}, status="stopped", _service=selected)
    params = selected.http.json.call_args.kwargs["params"]
    assert json.loads(params["metadata"]) == {"user": "a=b"}
    assert params["status"] == "stopped"
    selected.close.assert_not_called()


def test_runtime_connection_is_invalidated_on_resume():
    vm = VM(_service=service())
    vm._set({"name": "dev", "container_id": "old"})
    vm._connected = object()
    vm._set({"name": "dev", "container_id": "old"})
    assert vm._connected is not None
    vm._set({"name": "dev", "container_id": "new"})
    assert vm._connected is None


@pytest.fixture
def sandbox_vm(monkeypatch):
    vm = VM(_service=service())._set(
        {
            "name": "dev",
            "status": "running",
            "container_id": "test-container",
            "spec": {},
        }
    )
    vm._services_container = "test-container"
    monkeypatch.setattr(vm, "refresh", lambda: vm)
    sandbox = SimpleNamespace(process=MagicMock())
    monkeypatch.setattr(vm, "_sandbox", lambda **kwargs: sandbox)
    return vm, sandbox


def test_wait_checks_every_service_and_tcp_readiness(sandbox_vm):
    vm, sandbox = sandbox_vm
    vm._services_container = None
    vm.info["spec"].update(ssh=True, desktop=True)
    sandbox.process.exec.return_value.wait.return_value = 0
    assert vm.wait(services=True) is vm
    calls = [call.args for call in sandbox.process.exec.call_args_list]
    assert calls[:3] == [
        ("systemctl", "is-active", "--quiet", "beam-terminal.service"),
        ("systemctl", "is-active", "--quiet", "ssh.service"),
        ("systemctl", "is-active", "--quiet", "beam-desktop.service"),
    ]
    assert calls[3][:2] == ("python3", "-c")
    assert "[7681, 2222, 8080]" in calls[3][2]


def test_wait_for_exec_uses_live_connection_without_status_ticker(sandbox_vm, monkeypatch):
    vm, sandbox = sandbox_vm
    vm.info["status"] = "starting"
    refresh = MagicMock()
    monkeypatch.setattr(vm, "refresh", refresh)
    assert vm.wait() is vm
    assert vm.info["status"] == "running"
    refresh.assert_not_called()
    sandbox.process.exec.assert_not_called()


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
        vm_cli.management,
        ["new", "dev", "--template", "base", "--env", "VALUE=a=b c", "--json"],
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == fake.info
    options = factory.call_args.kwargs
    assert options["env"] == {"VALUE": "a=b c"}
    assert options["cpu"] is None and options["desktop"] is None and options["ssh"] is None
    assert options["_service"] is cli_service
    fake.create.assert_called_once_with(wait=False)
    fake.wait.assert_called_once_with()


def test_cli_creation_shows_only_result(cli_service, monkeypatch):
    vm = MagicMock()
    vm.info = {
        "name": "calm-otter-a3b19f",
        "id": "vm-12ab34cd56ef7890",
        "status": "running",
        "container_id": "vm-long-internal-runtime",
    }
    monkeypatch.setattr(vm_cli, "VM", lambda *args, **kwargs: vm)
    result = CliRunner().invoke(vm_cli.management, ["new", "--template", "base"])
    assert result.exit_code == 0, result.output
    assert "calm-otter-a3b19f" in result.output
    assert "vm-12ab34cd56ef7890" in result.output
    assert "Checking image cache" not in result.output
    assert "Waiting for VM" not in result.output
    assert "vm-long-internal-runtime" not in result.output


def test_vm_new_help_has_one_aligned_description_column():
    result = CliRunner().invoke(vm_cli.management, ["new", "--help"], terminal_width=80)
    assert result.exit_code == 0, result.output
    starts = []
    for option, description in [
        ("--cpu", "CPUs;"),
        ("--memory", "RAM in"),
        ("--auto-resume", "Wake on"),
        ("--ttl", "Snapshot and"),
    ]:
        line = next(line for line in result.output.splitlines() if line.strip().startswith(option))
        starts.append(line.index(description))
    assert len(set(starts)) == 1
    assert "INTEGER RANGE" not in result.output


@pytest.fixture
def identity_settings(monkeypatch, tmp_path):
    monkeypatch.setattr(
        vm_module,
        "get_settings",
        lambda: SimpleNamespace(config_path=tmp_path / "config.ini"),
    )


@pytest.mark.skipif(shutil.which("ssh-keygen") is None, reason="OpenSSH unavailable")
def test_identity_is_atomic_and_never_replaces_existing_keys(identity_settings):
    with ThreadPoolExecutor(max_workers=8) as executor:
        paths = list(executor.map(lambda _: vm_module.identity(), range(8)))
    assert len(set(paths)) == 1
    path = paths[0]
    private = path.read_bytes()
    assert (
        subprocess.check_output(["ssh-keygen", "-y", "-f", str(path)]).strip()
        == path.with_suffix(".pub").read_bytes().strip()
    )
    assert path.stat().st_mode & 0o777 == 0o600
    assert vm_module.identity().read_bytes() == private


def test_identity_reports_missing_openssh(monkeypatch, identity_settings):
    monkeypatch.setattr(vm_module.subprocess, "run", MagicMock(side_effect=FileNotFoundError()))
    with pytest.raises(RuntimeError, match="install ssh-keygen"):
        vm_module.identity()


@pytest.mark.parametrize("hung_command", ["systemctl", "python3"])
def test_wait_retries_hung_services_until_its_deadline(monkeypatch, hung_command, sandbox_vm):
    vm, sandbox = sandbox_vm
    vm._services_container = None
    clock = [0.0]
    monkeypatch.setattr(vm_module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        vm_module.time,
        "sleep",
        lambda seconds: clock.__setitem__(0, clock[0] + seconds),
    )

    def execute(command, *args):
        process = MagicMock()
        if command == hung_command:
            process.wait.side_effect = SandboxProcessError("timed out")
        else:
            process.wait.return_value = 0
        return process

    sandbox.process.exec.side_effect = execute
    with pytest.raises(TimeoutError, match="VM did not become ready"):
        vm.wait(timeout=1, services=True)
    assert sandbox.process.exec.call_count >= 2


def test_fork_connection_survives_parent_close(monkeypatch):
    parent_service, child_service = service(), service()
    config = SimpleNamespace()
    parent_service.channel.config = config
    clients = MagicMock(side_effect=[parent_service, child_service])
    monkeypatch.setattr(vm_module, "ServiceClient", clients)
    monkeypatch.setattr(vm_module, "public_key", lambda: "public-key")
    parent_service.http.json.return_value = {
        "id": "child",
        "name": "child",
        "status": "starting",
    }
    parent = VM("parent", context=config)
    child = parent.fork(wait=False)
    parent.close()
    parent_service.close.assert_called_once()
    child_service.close.assert_not_called()
    assert child._service is child_service
    child.close()
    child_service.close.assert_called_once()
    assert clients.call_args_list[1].args == (config,)


def test_exec_preserves_child_flags_that_match_cli_options(cli_service, monkeypatch):
    vm = MagicMock()
    vm.info = {"container_id": "runtime"}
    monkeypatch.setattr(vm_cli, "_vm", lambda *args: vm)
    execute = MagicMock()
    monkeypatch.setattr(
        container_cli.exec_container, "callback", SimpleNamespace(__wrapped__=execute)
    )
    result = CliRunner().invoke(
        vm_cli.management,
        ["exec", "--cwd", "/root", "dev", "sh", "-c", "echo --context literal"],
    )
    assert result.exit_code == 0, result.output
    execute.assert_called_once_with(
        cli_service, "runtime", ("sh", "-c", "echo --context literal"), "/root", 0
    )
    execute.reset_mock()
    result = CliRunner().invoke(
        vm_cli.management, ["exec", "dev", "--", "sh", "-c", "echo literal"]
    )
    assert result.exit_code == 0, result.output
    execute.assert_called_once_with(
        cli_service, "runtime", ("sh", "-c", "echo literal"), "/workspace", 0
    )


def test_ssh_preserves_child_flags_that_match_cli_options(cli_service, monkeypatch):
    vm = SimpleNamespace(name="dev")
    monkeypatch.setattr(vm_cli, "_vm", lambda *args: vm)
    monkeypatch.setattr(vm_cli, "_ssh_options", lambda vm: [])
    call = MagicMock(return_value=0)
    monkeypatch.setattr(vm_cli.subprocess, "call", call)
    result = CliRunner().invoke(
        vm_cli.management, ["ssh", "dev", "sh", "-c", "echo --context literal"]
    )
    assert result.exit_code == 0, result.output
    assert call.call_args.args[0][-1] == "sh -c 'echo --context literal'"


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
    assert call.call_args.args[0] == [
        "ssh",
        "-p",
        "2222",
        "--",
        "root@dev",
        "echo 'a b;$USER'",
    ]


@pytest.mark.parametrize("vm_id", ["vm-12ab34cd56ef7890", "12ab34cd56ef7890"])
def test_ssh_host_identity_uses_vm_prefix(vm_id, monkeypatch, tmp_path):
    vm = MagicMock(id=vm_id, info={"spec": {"ssh": True}})
    monkeypatch.setattr(vm_cli, "identity", lambda: tmp_path / "identity")
    monkeypatch.setattr(vm_cli.extraclick, "selected_context", lambda: "staging")
    monkeypatch.setattr(
        vm_cli,
        "get_settings",
        lambda: SimpleNamespace(config_path=tmp_path / "config.ini", name="Beta9"),
    )
    options = vm_cli._ssh_options(vm)
    assert "HostKeyAlias=vm-12ab34cd56ef7890" in options
    vm.wait.assert_called_once_with(services=True)


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
    selected.http.json.assert_called_once_with(
        "POST", "/api/v1/vm/{ws}/dev/bind", timeout=240, json={"port": 5432}
    )
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


def test_tunnel_drains_response_after_stdin_eof(monkeypatch):
    class Socket:
        def __init__(self):
            self.eof = threading.Event()
            self.closed = False
            self.sent = []
            self.reads = 0

        def settimeout(self, value):
            pass

        def send_binary(self, data):
            self.sent.append(data)

        def send(self, message):
            assert message == "EOF"
            self.eof.set()

        def recv(self):
            assert self.eof.wait(5), "stdin EOF was never sent"
            assert not self.closed, "tunnel closed before reading the response"
            self.reads += 1
            return b"complete response" if self.reads == 1 else b""

        def close(self):
            self.closed = True

    remote = Socket()
    monkeypatch.setattr(vm_cli, "_socket", lambda *args: remote)
    output = io.BytesIO()
    vm_cli._bridge(None, 2222, io.BytesIO(b"request"), output)
    assert remote.sent == [b"request"]
    assert output.getvalue() == b"complete response"
    assert remote.closed


@pytest.fixture
def stopped_vm(monkeypatch):
    selected = service()
    vm = VM(_service=selected)._set(
        {"name": "dev", "status": "stopped", "spec": {"auto_resume": True}}
    )
    monkeypatch.setattr(vm, "refresh", lambda: vm)
    return vm, selected


def test_disabled_desktop_does_not_wake_a_vm(stopped_vm, monkeypatch):
    vm, _ = stopped_vm
    acquire = MagicMock()
    monkeypatch.setattr(vm, "_sandbox", acquire)
    with pytest.raises(ValueError, match="desktop=True"):
        vm.desktop.screen_size()
    acquire.assert_not_called()


def test_process_kill_transport_can_refuse_automatic_wake(stopped_vm):
    vm, selected = stopped_vm
    with pytest.raises(SandboxConnectionError, match="not running"):
        vm._sandbox(auto_resume=False)
    selected.http.json.assert_not_called()


def test_cli_stale_kill_rejects_before_acquiring_runtime(cli_service, monkeypatch):
    vm = MagicMock()
    vm.info = {"container_id": "current"}
    monkeypatch.setattr(vm_cli, "_vm", lambda *_: vm)
    result = CliRunner().invoke(vm_cli.management, ["kill", "--container-id", "old", "dev", "44"])
    assert result.exit_code == 1
    assert "previous runtime" in result.output
    vm._sandbox.assert_not_called()


def test_cli_ps_uses_pid_indexed_processes(cli_service, monkeypatch):
    vm = MagicMock()
    vm.process.list_processes.return_value = {
        44: SimpleNamespace(pid=44, args=["echo", "two words"], cwd="/", exit_code=0)
    }
    monkeypatch.setattr(vm_cli, "_vm", lambda *_: vm)
    result = CliRunner().invoke(vm_cli.management, ["ps", "--json", "dev"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == [
        {"pid": 44, "args": ["echo", "two words"], "cwd": "/", "exit_code": 0}
    ]


def test_recording_selects_mp4_for_extensionless_paths(sandbox_vm, monkeypatch):
    vm, sandbox = sandbox_vm
    vm.info["spec"]["desktop"] = True
    desktop = vm.desktop
    monkeypatch.setattr(desktop, "screen_size", lambda: (1280, 720))
    desktop.record("/workspace/extensionless")
    assert sandbox.process.exec.call_args.args[-3:] == (
        "-f",
        "mp4",
        "/workspace/extensionless",
    )


def test_desktop_timeout_cancels_command_and_preserves_wait_error(sandbox_vm):
    vm, sandbox = sandbox_vm
    vm.info["spec"]["desktop"] = True
    process = sandbox.process.exec.return_value
    error = SandboxProcessError("did not exit within 30 seconds")
    process.wait.side_effect = error
    process.kill.side_effect = SandboxProcessError("already exited")
    with pytest.raises(SandboxProcessError) as raised:
        vm.desktop.screen_size()
    assert raised.value is error
    process.kill.assert_called_once()
    process.stderr.read.assert_not_called()


def test_creation_retry_freezes_caller_owned_metadata_and_lists():
    selected = service()
    selected.http.json.side_effect = [
        [],
        TimeoutError("lost response"),
        {"id": "resource", "name": "dev", "status": "starting"},
    ]
    metadata, ports, networks = {"project": "original"}, [9000], ["1.1.1.1/32"]
    vm = VM(
        "dev",
        template="base",
        ssh=False,
        metadata=metadata,
        ports=ports,
        allow_list=networks,
        _service=selected,
    )
    with pytest.raises(TimeoutError):
        vm.create(wait=False)
    metadata["project"] = "changed"
    ports.append(9001)
    networks.clear()
    vm.create(wait=False)
    submitted = selected.http.json.call_args.kwargs["json"]
    assert submitted["metadata"] == {"project": "original"}
    assert submitted["spec"]["ports"] == [9000]
    assert submitted["spec"]["allow_list"] == ["1.1.1.1/32"]


def test_activity_touch_retries_transient_lifecycle_lock():
    selected = service()
    selected.http.json.side_effect = [
        GatewayHTTPError(409, "VM operation already in progress"),
        {},
    ]
    vm = VM(_service=selected)._set({"id": "resource", "name": "dev"})
    vm._touch(timeout=3)
    assert selected.http.json.call_count == 2
    assert all(call.kwargs["timeout"] <= 3 for call in selected.http.json.call_args_list)


def test_cli_rejects_detached_command_deadline(cli_service, monkeypatch):
    resolve = MagicMock()
    monkeypatch.setattr(vm_cli, "_vm", resolve)
    result = CliRunner().invoke(
        vm_cli.management, ["exec", "--detach", "--timeout", "1", "dev", "sleep", "600"]
    )
    assert result.exit_code == 2
    assert "foreground" in result.output
    resolve.assert_not_called()


def test_activity_lease_refreshes_during_work_and_stops_on_exit():
    selected = service()
    heartbeats = threading.Event()
    info = {"id": "resource", "name": "dev", "spec": {"idle_timeout": 1}}

    def response(method, path, **kwargs):
        if path.endswith("/touch") and kwargs.get("timeout") == 3:
            heartbeats.set()
        return info

    selected.http.json.side_effect = response
    vm = VM(_service=selected)._set(info)
    with vm.keep_alive():
        assert heartbeats.wait(2), "active work must refresh before its idle deadline"
    calls = selected.http.json.call_count
    heartbeats.clear()
    assert not heartbeats.wait(0.4), "lease must stop when its owner exits"
    assert selected.http.json.call_count == calls


def test_activity_lease_surfaces_lost_authorization():
    selected = service()
    failed = threading.Event()
    info = {"id": "resource", "name": "dev", "spec": {"idle_timeout": 1}}

    def response(method, path, **kwargs):
        if kwargs.get("timeout") == 3:
            failed.set()
            raise GatewayHTTPError(403, "revoked")
        return info

    selected.http.json.side_effect = response
    vm = VM(_service=selected)._set(info)
    with pytest.raises(RuntimeError, match="activity lease failed"):
        with vm.keep_alive():
            assert failed.wait(2)


@pytest.mark.parametrize("detach", [False, True])
def test_cli_json_exec_passes_stdin_only_for_foreground(cli_service, monkeypatch, detach):
    vm = MagicMock()
    vm.id = "vm-test"
    vm.info = {"container_id": "current"}
    process = vm._sandbox.return_value.process.exec.return_value

    def execute(*args, **kwargs):
        if not detach:
            vm.keep_alive.return_value.__enter__.assert_called_once()
        return process

    vm._sandbox.return_value.process.exec.side_effect = execute
    process.pid = 44
    process.wait.return_value = 0
    process.stdout.read.return_value = "output"
    process.stderr.read.return_value = ""
    monkeypatch.setattr(vm_cli, "_vm", lambda *_: vm)
    args = ["exec", "--json"] + (["--detach"] if detach else []) + ["dev", "cat"]
    result = CliRunner().invoke(vm_cli.management, args, input="piped input")
    assert result.exit_code == 0, result.output
    stdin = vm._sandbox.return_value.process.exec.call_args.kwargs["stdin"]
    if detach:
        assert stdin is None
    else:
        assert stdin is not None
