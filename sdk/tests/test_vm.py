from types import SimpleNamespace
from unittest.mock import MagicMock
from concurrent.futures import ThreadPoolExecutor
import json
import io
import shutil
import subprocess
import threading
import socket

import pytest
from click.testing import CliRunner

from beta9.abstractions.image import Image
from beta9.abstractions.vm import VM, prepare_image
from beta9.abstractions import vm as vm_module
from beta9.exceptions import SandboxProcessError, SandboxConnectionError
from beta9.channel import GatewayHTTPError
from beta9.config import ConfigContext, SDKSettings
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
        public_ports=[7681],
        _service=selected,
    )
    vm.create(wait=False)
    body = selected.http.json.call_args.kwargs["json"]
    assert body == {
        "name": "dev",
        "template": "base",
        "spec": {"ssh": False, "cpu": 2500, "env": ["VALUE=a=b c"], "public_ports": [7681]},
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


@pytest.mark.parametrize("address", ["2600:1f18:1234:5678:100:abcd:ef01:2345", "", None])
def test_public_ipv6_uses_resolved_metadata_without_an_extra_request(address):
    selected = service()
    selected.http.json.return_value = {"name": "dev", "id": "vm-id", "public_ipv6": address}
    vm = VM.get("dev", _service=selected)
    selected.http.json.reset_mock()
    assert vm.public_ipv6 == (address or None)
    selected.http.json.assert_not_called()


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
        "ip",
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
        "open",
        "access-url",
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
        "public_port",
        "context",
    } <= options
    assert not {"gpu", "gpu_count"} & options


@pytest.mark.parametrize("cli_name", ["Beta9", "Beam"])
@pytest.mark.parametrize("options,ttl", [([], 600), (["--ttl", "3600"], 3600)])
def test_cli_access_url_uses_selected_profile(cli_service, monkeypatch, cli_name, options, ttl):
    context = MagicMock(return_value=SimpleNamespace())
    monkeypatch.setattr(extraclick, "get_config_context", context)
    url = "https://dev-3000.example.test/?beam_vm_session=signed"
    cli_service.http.json.side_effect = [
        {"name": "dev", "id": "vm-resource", "status": "stopped"},
        {"url": url},
    ]
    cli = load_cli(check_config=False, settings=SDKSettings(name=cli_name))

    result = CliRunner().invoke(
        cli.common_group,
        ["vm", "access-url", "dev", "3000", "--context", "staging", *options],
    )

    assert result.exit_code == 0, result.output
    assert result.output == url + "\n"
    context.assert_called_once_with("staging")
    requests = cli_service.http.json.call_args_list
    assert [request.args for request in requests] == [
        ("GET", "/api/v1/vm/{ws}/dev"),
        ("POST", "/api/v1/vm/{ws}/vm-resource/access-session"),
    ]
    assert requests[1].kwargs["json"] == {"port": 3000, "ttl": ttl}


@pytest.mark.parametrize(
    "args", [["0"], ["65536"], ["3000", "--ttl", "0"], ["3000", "--ttl", "3601"]]
)
def test_cli_access_url_rejects_invalid_port_or_ttl(cli_service, args):
    result = CliRunner().invoke(vm_cli.management, ["access-url", "dev", *args])
    assert result.exit_code == 2
    assert "Invalid value" in result.output
    cli_service.http.json.assert_not_called()


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


def test_cli_creation_shows_progress_without_internal_startup_logs(cli_service, monkeypatch):
    vm = MagicMock()
    vm.info = {
        "name": "calm-otter-a3b19f",
        "id": "vm-12ab34cd56ef7890",
        "status": "running",
        "public_ipv6": "2600:1f18:1234:5678:100:abcd:ef01:2345",
        "container_id": "vm-long-internal-runtime",
    }
    monkeypatch.setattr(vm_cli, "VM", lambda *args, **kwargs: vm)
    result = CliRunner().invoke(vm_cli.management, ["new", "--template", "base"])
    assert result.exit_code == 0, result.output
    assert "Creating VM" in result.output
    assert result.output.index("✓ VM ready") < result.output.index("calm-otter-a3b19f")
    assert "calm-otter-a3b19f" in result.output
    assert "vm-12ab34cd56ef7890" in result.output
    assert "IPv6" in result.output and vm.info["public_ipv6"] in result.output
    assert "Checking image cache" not in result.output
    assert "Waiting for VM" not in result.output
    assert "vm-long-internal-runtime" not in result.output


@pytest.mark.parametrize("as_json", [False, True])
def test_cli_get_shows_ipv6_and_preserves_json(cli_service, as_json):
    info = {
        "name": "dev",
        "id": "vm-id",
        "status": "running",
        "public_ipv6": "2600:1f18:1234:5678:100:abcd:ef01:2345",
    }
    cli_service.http.json.return_value = info
    result = CliRunner().invoke(vm_cli.management, ["get", "dev", *(["--json"] if as_json else [])])
    assert result.exit_code == 0, result.output
    if as_json:
        assert json.loads(result.output) == info
    else:
        assert "IPv6" in result.output and info["public_ipv6"] in result.output


@pytest.mark.parametrize("protected", [None, [], [7681], [7681, 8080]])
@pytest.mark.parametrize("as_json", [False, True])
@pytest.mark.parametrize("dashboard", [False, True])
def test_cli_browser_views_never_mint_credentials(cli_service, protected, as_json, dashboard):
    cli_service._config = (
        ConfigContext(vm_url_template="https://dash.example/vms/{vm_id}?tab=logs")
        if dashboard
        else ConfigContext()
    )
    info = {
        "name": "dev",
        "id": "vm-id",
        "terminal_url": "https://terminal.example/",
        "desktop_url": "https://desktop.example/",
        "spec": {"protected_ports": protected},
    }
    cli_service.http.json.return_value = info
    result = CliRunner().invoke(vm_cli.management, ["get", "dev", *(["--json"] if as_json else [])])
    assert result.exit_code == 0, result.output
    if as_json:
        assert json.loads(result.output) == info
    else:
        for feature, port in (("terminal", 7681), ("desktop", 8080)):
            expected = info[feature + "_url"]
            assert expected in result.output
        assert "beam_vm_session" not in result.output
        assert "dash.example" not in result.output
        assert "expire" not in result.output
    cli_service.http.json.assert_called_once()
    assert info["terminal_url"] == "https://terminal.example/"


def test_self_hosted_browser_views_show_direct_endpoints_without_credentials(
    cli_service, monkeypatch
):
    monkeypatch.setattr(vm_cli.terminal, "cli_name", lambda: "beam")
    cli_service.http.json.return_value = {
        "name": "dev",
        "id": "vm-id",
        "terminal_url": "https://terminal.example/",
        "spec": {"protected_ports": [7681]},
    }
    result = CliRunner().invoke(vm_cli.management, ["get", "dev", "--context", "staging"])
    assert result.exit_code == 0, result.output
    assert "vm-id" in result.output
    assert "https://terminal.example/" in result.output
    assert "beam --context staging vm share dev" in result.output
    cli_service.http.json.assert_called_once()


@pytest.mark.parametrize("feature,port", [("terminal", 7681), ("desktop", 8080)])
def test_browser_commands_open_authenticated_links(cli_service, monkeypatch, feature, port):
    signed = "https://vm.example/?beam_vm_session=signed"
    cli_service.http.json.side_effect = [
        {
            "name": "dev",
            "id": "vm-id",
            feature + "_url": "https://vm.example/",
            "spec": {"protected_ports": [port]},
        },
        {"url": signed},
    ]
    opened = MagicMock()
    monkeypatch.setattr(vm_cli.webbrowser, "open", opened)
    result = CliRunner().invoke(vm_cli.management, [feature, "dev"])
    assert result.exit_code == 0, result.output
    assert result.output == "https://vm.example/\n"
    opened.assert_called_once_with(signed)
    assert cli_service.http.json.call_args.kwargs["json"]["ttl"] == 3600


@pytest.mark.parametrize("protected", [False, True])
@pytest.mark.parametrize("url_only", [False, True])
def test_open_any_port_handles_public_and_private_access(
    cli_service, monkeypatch, protected, url_only
):
    stable = "https://app.example/"
    signed = stable + "?beam_vm_session=signed"
    info = {
        "name": "dev",
        "id": "vm-id",
        "urls": {"3000": stable},
        "spec": {"protected_ports": [3000] if protected else []},
    }
    cli_service.http.json.side_effect = [info, {"url": signed}]
    opened = MagicMock()
    monkeypatch.setattr(vm_cli.webbrowser, "open", opened)
    result = CliRunner().invoke(
        vm_cli.management, ["open", "dev", "3000", *(["--url"] if url_only else [])]
    )
    assert result.exit_code == 0, result.output
    assert result.output == stable + "\n"
    if url_only:
        opened.assert_not_called()
    else:
        opened.assert_called_once_with(signed if protected else stable)
    assert cli_service.http.json.call_count == (2 if protected and not url_only else 1)


def test_open_uses_root_cli_profile_and_prints_a_copyable_link(cli_service, monkeypatch):
    context = MagicMock(return_value=SimpleNamespace())
    monkeypatch.setattr(extraclick, "get_config_context", context)
    signed = "https://app.example/?beam_vm_session=signed"
    cli_service.http.json.side_effect = [
        {
            "name": "dev",
            "id": "vm-id",
            "urls": {"3000": "https://app.example/"},
            "spec": {"protected_ports": [3000]},
        },
        {"url": signed},
    ]
    cli = load_cli(check_config=False, settings=SDKSettings(name="Beam"))
    result = CliRunner().invoke(
        cli.common_group, ["--context", "staging", "vm", "open", "dev", "3000", "--url"]
    )
    assert result.exit_code == 0, result.output
    assert result.output == "https://app.example/\n"
    cli_service.http.json.assert_called_once()
    context.assert_called_once_with("staging")


@pytest.mark.parametrize("command", ["get", "ports"])
def test_global_json_never_mints_browser_credentials(cli_service, monkeypatch, command):
    info = {
        "name": "dev",
        "id": "vm-id",
        "urls": {"3000": "https://app.example/"},
        "spec": {"protected_ports": [3000]},
    }
    cli_service.http.json.return_value = info
    cli = load_cli(check_config=False, settings=SDKSettings(name="Beam"))
    result = CliRunner().invoke(cli.common_group, ["--json", "vm", command, "dev"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == (info if command == "get" else info["urls"])
    cli_service.http.json.assert_called_once()


@pytest.mark.parametrize(
    "port,spec,hint",
    [
        (2222, {"ssh": True}, "vm ssh dev"),
        (5432, {"private_ports": [5432]}, "vm port-forward dev 5432"),
        (3000, {}, "vm expose dev 3000"),
    ],
)
def test_open_reports_the_correct_access_command(cli_service, port, spec, hint):
    cli_service.http.json.return_value = {"name": "dev", "id": "vm-id", "urls": {}, "spec": spec}
    result = CliRunner().invoke(vm_cli.management, ["open", "dev", str(port)])
    assert result.exit_code == 1
    assert hint in result.output
    cli_service.http.json.assert_called_once()


@pytest.mark.parametrize("as_json", [False, True])
def test_ports_explains_all_access_modes_and_preserves_json(cli_service, monkeypatch, as_json):
    monkeypatch.setattr(vm_cli.terminal, "cli_name", lambda: "beam")
    info = {
        "name": "dev",
        "id": "vm-id",
        "urls": {"3000": "https://app.example/", "8000": "https://public.example/"},
        "spec": {"protected_ports": [3000], "private_ports": [5432], "ssh": True},
    }
    signed = "https://app.example/?beam_vm_session=signed"
    cli_service.http.json.side_effect = [info, {"url": signed}]
    result = CliRunner().invoke(
        vm_cli.management, ["ports", "dev", *(["--json"] if as_json else [])]
    )
    assert result.exit_code == 0, result.output
    if as_json:
        assert json.loads(result.output) == info["urls"]
        cli_service.http.json.assert_called_once()
    else:
        for text in [
            "https://app.example/",
            "https://public.example/",
            "Private",
            "Public",
            "Tunnel only",
            "SSH key",
            "beam vm port-forward dev 5432",
            "beam vm ssh dev",
            "beam vm open dev PORT",
        ]:
            assert text in result.output
        cli_service.http.json.assert_called_once()


@pytest.mark.parametrize("protected", [False, True])
@pytest.mark.parametrize("mode", [None, "--url", "--json"])
def test_expose_shows_usable_links_and_access_mode(cli_service, protected, mode):
    stable = "https://app.example/"
    signed = stable + "?beam_vm_session=signed"
    info = {
        "name": "dev",
        "id": "vm-id",
        "urls": {"3000": stable},
        "spec": {"protected_ports": [3000] if protected else []},
    }
    cli_service.http.json.side_effect = [{"name": "dev", "id": "vm-id"}, info, {"url": signed}]
    result = CliRunner().invoke(
        vm_cli.management,
        [
            "expose",
            "dev",
            "3000",
            "--protected" if protected else "--public",
            *([mode] if mode else []),
        ],
    )
    assert result.exit_code == 0, result.output
    if mode == "--json":
        assert json.loads(result.output) == info
    elif mode == "--url":
        assert result.output == stable + "\n"
    else:
        assert f"Port 3000 ({'private' if protected else 'public'})" in result.output
        assert stable in result.output
    assert cli_service.http.json.call_count == 2
    assert cli_service.http.json.call_args_list[1].kwargs["json"] == {
        "port": 3000,
        "protected": protected,
    }


def test_expose_rejects_conflicting_output_flags_before_publishing(cli_service):
    result = CliRunner().invoke(vm_cli.management, ["expose", "dev", "3000", "--json", "--url"])
    assert result.exit_code == 2
    cli_service.http.json.assert_not_called()


@pytest.mark.parametrize("address", ["2600:1f18:1234:5678:100:abcd:ef01:2345", None])
def test_cli_ip_is_copyable_and_reports_unallocated_addresses(cli_service, address):
    cli_service.http.json.return_value = {"name": "dev", "id": "vm-id", "public_ipv6": address}
    result = CliRunner().invoke(vm_cli.management, ["ip", "dev"])
    if address:
        assert result.exit_code == 0, result.output
        assert result.output == address + "\n"
    else:
        assert result.exit_code == 1
        assert "does not have a public IPv6 address" in result.output
    cli_service.http.json.assert_called_once()


@pytest.mark.parametrize(
    "args",
    [
        ["new", "dev", "--template", "base"],
        ["start", "dev"],
        ["resume", "dev"],
        ["pause", "dev"],
        ["stop", "dev"],
        ["stop", "dev", "--no-snapshot"],
        ["fork", "dev", "copy"],
        ["fork", "snapshot", "copy"],
        ["snapshot", "create", "dev"],
        ["template", "create", "dev", "base"],
    ],
)
def test_long_operations_keep_json_free_of_progress(cli_service, monkeypatch, args):
    vm = MagicMock(info={"id": "vm-12ab34cd56ef7890", "name": "dev", "status": "running"})
    for method in ("create", "start", "pause", "stop", "fork"):
        getattr(vm, method).return_value = vm
    vm._api.return_value = [{"id": "snapshot"}]
    vm.snapshot.return_value = vm.create_template.return_value = vm.info
    factory = MagicMock(return_value=vm)
    factory.get.return_value = vm
    monkeypatch.setattr(vm_cli, "VM", factory)

    result = CliRunner().invoke(vm_cli.management, [*args, "--json"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == vm.info


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
    assert "✗ Starting VM" in result.output
    assert "✓ VM ready" not in result.output
    assert vm_cli.terminal._current_status is None


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


@pytest.mark.parametrize("terminate", [None, "SIGHUP", "SIGTERM"])
def test_ssh_helper_keeps_diagnostics_out_of_binary_transport(monkeypatch, terminate):
    if terminate and not hasattr(vm_cli.signal, terminate):
        pytest.skip(f"{terminate} is unavailable")
    output, diagnostics = io.BytesIO(), io.StringIO()
    previous_handlers = {
        getattr(vm_cli.signal, name): vm_cli.signal.getsignal(getattr(vm_cli.signal, name))
        for name in ("SIGHUP", "SIGTERM")
        if hasattr(vm_cli.signal, name)
    }
    ignored_signals = []
    client = MagicMock()
    monkeypatch.setattr(vm_cli, "ServiceClient", lambda config: client)
    monkeypatch.setattr(vm_cli, "get_config_context", lambda profile: SimpleNamespace())
    monkeypatch.setattr(vm_cli, "set_settings", lambda settings: None)
    monkeypatch.setattr(vm_cli, "_vm", lambda *args: SimpleNamespace())

    def bridge(vm, port, source, target):
        print("connection diagnostic")
        target.write(b"SSH-2.0-OpenSSH\r\n")
        if terminate:
            try:
                vm_cli.signal.raise_signal(getattr(vm_cli.signal, terminate))
            finally:
                for sig in previous_handlers:
                    vm_cli.signal.raise_signal(sig)
                    ignored_signals.append(sig)

    monkeypatch.setattr(vm_cli, "_bridge", bridge)
    with monkeypatch.context() as patch:
        patch.setattr(vm_cli.sys, "stdout", SimpleNamespace(buffer=output))
        patch.setattr(vm_cli.sys, "stderr", diagnostics)
        patch.setattr(vm_cli.sys, "stdin", SimpleNamespace(buffer=io.BytesIO()))
        if terminate:
            with pytest.raises(SystemExit) as exit:
                vm_cli._run_tunnel("local", "/tmp/config.ini", "Beam", "resource", 2222)
            assert exit.value.code == 0
        else:
            vm_cli._run_tunnel("local", "/tmp/config.ini", "Beam", "resource", 2222)
    assert output.getvalue() == b"SSH-2.0-OpenSSH\r\n"
    assert diagnostics.getvalue() == "connection diagnostic\n"
    assert ignored_signals == (list(previous_handlers) if terminate else [])
    assert all(
        vm_cli.signal.getsignal(sig) == handler for sig, handler in previous_handlers.items()
    )


def test_port_forward_closes_socket_with_pending_input(cli_service, monkeypatch):
    peer, client = socket.socketpair()
    listener = MagicMock()
    listener.accept.side_effect = [(client, None), KeyboardInterrupt()]
    monkeypatch.setattr(vm_cli.socket, "socket", lambda *args: listener)
    monkeypatch.setattr(
        vm_cli, "_vm", lambda *args: SimpleNamespace(name="dev", info={"spec": {"ports": [80]}})
    )
    reading, finished = threading.Event(), threading.Event()

    def bridge(vm, port, source, target):
        def read():
            reading.set()
            source.read1(1)
            finished.set()

        threading.Thread(target=read, daemon=True).start()
        assert reading.wait(1)
        target.write(b"response")
        target.flush()

    monkeypatch.setattr(vm_cli, "_bridge", bridge)
    try:
        CliRunner().invoke(vm_cli.management, ["port-forward", "dev", "80"])
        peer.settimeout(2)
        assert peer.recv(8) == b"response"
        assert peer.recv(1) == b""
        assert finished.wait(1)
    finally:
        peer.close()
        client.close()


def test_tunnel_drains_response_after_stdin_eof(monkeypatch):
    from queue import Queue
    from uuid import UUID
    import json

    class Socket:
        def __init__(self):
            self.messages = Queue()
            self.closed = False
            self.sent = []
            self.read = None
            self.eof = False
            self.responded = False

        def settimeout(self, value):
            pass

        def send_binary(self, data):
            self.sent.append(data[32:])
            self.messages.put(json.dumps({"type": "input", "id": str(UUID(bytes=data[:16]))}))

        def send(self, message):
            control = json.loads(message)
            if control["type"] == "close":
                self.messages.put(json.dumps({"type": "input", "id": control["id"]}))
            if control["type"] == "eof":
                self.eof = True
                self.messages.put(json.dumps({"type": "input", "id": control["id"]}))
                if self.read:
                    self.reply(self.read)
            elif control["type"] == "read":
                self.read = control["id"]
                if self.eof:
                    self.reply(self.read)

        def reply(self, request):
            if self.responded:
                self.messages.put(json.dumps({"type": "eof", "id": request}))
            else:
                self.messages.put(UUID(request).bytes + b"complete response")
                self.responded = True

        def recv(self):
            assert not self.closed
            return self.messages.get(timeout=5)

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


@pytest.mark.parametrize(
    "selectors",
    [
        ["--unit", "ssh", "--session", "prompt-test"],
        ["--pid", "44", "--session", "prompt-test"],
        ["--pid", "44", "--unit", "ssh"],
    ],
)
def test_cli_logs_rejects_conflicting_sources_before_acquiring_vm(
    cli_service, monkeypatch, selectors
):
    acquire = MagicMock()
    monkeypatch.setattr(vm_cli, "_vm", acquire)
    result = CliRunner().invoke(vm_cli.management, ["logs", "dev", *selectors])
    assert result.exit_code == 2
    assert "Choose one of --pid, --unit, or --session" in result.output
    acquire.assert_not_called()


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


def test_desktop_resize_registers_custom_resolution(sandbox_vm, monkeypatch):
    vm, _ = sandbox_vm
    desktop = vm.desktop
    run = MagicMock(side_effect=["1920x1080 60.00", "", "", ""])
    monkeypatch.setattr(desktop, "_run", run)
    desktop.resize(2560, 1440)
    assert run.call_args_list[1].args == (
        "xrandr",
        "--newmode",
        "2560x1440",
        "221.184",
        "2560",
        "0",
        "0",
        "2560",
        "1440",
        "0",
        "0",
        "1440",
    )
    assert run.call_args_list[2].args == ("xrandr", "--addmode", "VNC-0", "2560x1440")
    assert run.call_args_list[3].args == (
        "xrandr",
        "--output",
        "VNC-0",
        "--mode",
        "2560x1440",
    )


def test_desktop_resize_reuses_existing_resolution(sandbox_vm, monkeypatch):
    vm, _ = sandbox_vm
    desktop = vm.desktop
    run = MagicMock(side_effect=["2560x1440 60.00", ""])
    monkeypatch.setattr(desktop, "_run", run)
    desktop.resize(2560, 1440)
    assert run.call_count == 2
    assert run.call_args.args == ("xrandr", "--output", "VNC-0", "--mode", "2560x1440")


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


def test_activity_lease_survives_gateway_restart_and_releases_same_lease():
    selected = service()
    restored = threading.Event()
    info = {"id": "resource", "name": "dev", "spec": {"idle_timeout": 1}}
    leases = []
    failed_once = False

    def response(method, path, **kwargs):
        nonlocal failed_once
        if path.endswith("/touch"):
            leases.append(kwargs["params"])
            if kwargs.get("timeout") == 3 and not kwargs["params"].get("release") == "1":
                if not failed_once:
                    failed_once = True
                    raise GatewayHTTPError(503, "gateway restarting")
                restored.set()
        return info

    selected.http.json.side_effect = response
    vm = VM(_service=selected)._set(info)
    with vm.keep_alive():
        assert restored.wait(2)
    assert len({params["lease"] for params in leases}) == 1
    assert leases[-1]["release"] == "1"


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js unavailable")
def test_desktop_browser_routing_and_reconnect():
    from pathlib import Path

    subprocess.run(
        [
            "node",
            str(Path(__file__).with_name("test_vm_desktop.js")),
            str(Path(__file__).parents[2] / "pkg/abstractions/vm/desktop.html"),
        ],
        check=True,
    )


@pytest.mark.parametrize("port", [7681, 8080, 3000])
@pytest.mark.parametrize("url_only", [False, True])
def test_direct_browser_links_never_mint_cli_credentials(cli_service, monkeypatch, port, url_only):
    info = {
        "name": "dev",
        "id": "vm-id",
        "browser_auth": True,
        "urls": {str(port): "https://port.example/"},
        "spec": {"protected_ports": [port]},
    }
    cli_service.http.json.return_value = info
    opened = MagicMock()
    monkeypatch.setattr(vm_cli.webbrowser, "open", opened)
    result = CliRunner().invoke(
        vm_cli.management, ["open", "dev", str(port), *(["--url"] if url_only else [])]
    )
    assert result.exit_code == 0, result.output
    assert result.output == "https://port.example/\n"
    if url_only:
        opened.assert_not_called()
    else:
        opened.assert_called_once_with("https://port.example/")
    cli_service.http.json.assert_called_once()


@pytest.mark.parametrize("feature,port", [("terminal", 7681), ("desktop", 8080)])
def test_terminal_and_desktop_open_the_direct_vm_not_a_dashboard(
    cli_service, monkeypatch, feature, port
):
    cli_service._config = ConfigContext(vm_url_template="https://dash.example/vms/{vm_id}")
    cli_service.http.json.return_value = {
        "name": "dev",
        "id": "vm-id",
        "browser_auth": True,
        feature + "_url": "https://vm.example/",
        "spec": {"protected_ports": [port]},
    }
    opened = MagicMock()
    monkeypatch.setattr(vm_cli.webbrowser, "open", opened)
    result = CliRunner().invoke(vm_cli.management, [feature, "dev"])
    assert result.exit_code == 0, result.output
    assert result.output == "https://vm.example/\n"
    opened.assert_called_once_with("https://vm.example/")
    cli_service.http.json.assert_called_once()


@pytest.mark.parametrize(
    "args,expected",
    [
        ([], True),
        (["--desktop"], True),
        (["--no-desktop"], False),
        (["--template", "base"], None),
        (["--image-id", "prepared"], None),
    ],
)
def test_new_vm_includes_desktop_and_preserves_prebuilt_settings(
    cli_service, monkeypatch, args, expected
):
    vm = MagicMock(info={"name": "dev", "id": "vm-id"})
    factory = MagicMock(return_value=vm)
    monkeypatch.setattr(vm_cli, "VM", factory)
    result = CliRunner().invoke(vm_cli.management, ["new", "dev", "--json", *args])
    assert result.exit_code == 0, result.output
    assert factory.call_args.kwargs["desktop"] is expected
