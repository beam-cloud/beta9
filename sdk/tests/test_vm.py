import asyncio
import base64
from concurrent.futures import ThreadPoolExecutor
import datetime
import gzip
import importlib.machinery
import importlib.util
import io
import json
import os
from pathlib import Path
import runpy
import shlex
import shutil
import socket
import subprocess
import sys
import tarfile
import textwrap
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import grpc
import pytest
from click.testing import CliRunner
from jsonschema import Draft202012Validator

from beta9.abstractions import vm as vm_module
from beta9.abstractions.image import Image
from beta9.abstractions.vm import Kiosk, MCPServer, VM, prepare_image
from beta9.abstractions.vm.mcp_api import VMMCP
from beta9.channel import GatewayHTTPError, ServiceClient
from beta9.cli import container as container_cli
from beta9.cli import extraclick
from beta9.cli import vm as vm_cli
from beta9.cli.main import load_cli
from beta9.config import ConfigContext, SDKSettings, get_settings
from beta9.deployment import onboarding
from beta9.exceptions import SandboxConnectionError, SandboxProcessError
from beta9.mcp.vm import definitions
from beta9.type import GpuType


def service():
    return SimpleNamespace(
        channel=MagicMock(), gateway=MagicMock(), http=MagicMock(), close=MagicMock()
    )


@pytest.mark.parametrize(
    "gpu,base",
    [
        (None, vm_module.DEFAULT_VM_BASE_IMAGE),
        (GpuType.RTX4090, vm_module.DEFAULT_VM_GPU_BASE_IMAGE),
    ],
)
def test_default_creation_uses_one_http_request_and_freezes_retry(monkeypatch, gpu, base):
    from requests import ConnectionError

    monkeypatch.setattr(vm_module, "public_key", lambda: "ssh-ed25519 test")
    with ServiceClient(ConfigContext(token="test", api_url="https://vm.example")) as selected:
        selected._gateway = MagicMock()
        direct = selected.vm_http
        direct.json = MagicMock(
            side_effect=[
                ConnectionError("lost response"),
                {
                    "id": "vm-id",
                    "name": "fast",
                    "status": "running",
                    "exec_ready": True,
                    "container_id": "container",
                    "stub_id": "stub",
                },
            ]
        )
        metadata = {"owner": "original"}
        vm = VM("fast", desktop=True, gpu=gpu, metadata=metadata, _service=selected)
        with pytest.raises(ConnectionError):
            vm.create()
        body = json.dumps(direct.json.call_args.kwargs["json"], sort_keys=True)
        metadata["owner"] = "changed"
        vm.create()
        assert json.dumps(direct.json.call_args.kwargs["json"], sort_keys=True) == body
        assert direct.workspace_id == "self"
        assert all(
            call.args[:2] == ("POST", "/api/v1/vm/{ws}") for call in direct.json.call_args_list
        )
        request = direct.json.call_args.kwargs["json"]
        assert request["image"]["existing_image_uri"] == base
        assert vm_module.DEFAULT_VM_BASE_IMAGE.startswith("ubuntu:24.04@sha256:")
        assert request["image"]["ignore_python"] is True
        assert request["spec"].get("gpu") == (gpu.value if gpu else None)
        assert request["spec"]["ssh_public_key"] == "ssh-ed25519 test"
        selected._gateway.export_workspace_config.assert_not_called()


@pytest.mark.parametrize("status", [401, 404, 428])
def test_default_creation_falls_back_before_allocation(monkeypatch, status):
    monkeypatch.setattr(vm_module, "public_key", lambda: "ssh-ed25519 test")
    with ServiceClient(ConfigContext(token="test", api_url="https://vm.example")) as selected:
        selected.vm_http.json = MagicMock(side_effect=GatewayHTTPError(status, "not supported"))
        vm = VM("fast", _service=selected)
        prepare = MagicMock()
        monkeypatch.setattr(vm, "prepare", prepare)
        # The second submission represents the established image-build path.
        submit = vm._submit_creation
        legacy = MagicMock(return_value=vm)
        monkeypatch.setattr(
            vm, "_submit_creation", lambda wait: submit(wait) if vm._vm_http else legacy(wait)
        )
        assert vm.create() is vm
        prepare.assert_called_once()
        legacy.assert_called_once_with(True)
        assert vm._creation_body is None


@pytest.mark.parametrize("status", [400, 500])
def test_default_creation_preserves_request_after_server_error(monkeypatch, status):
    monkeypatch.setattr(vm_module, "public_key", lambda: "ssh-ed25519 test")
    monkeypatch.setattr(vm_module, "transient_error", lambda _: False)
    with ServiceClient(ConfigContext(token="test", api_url="https://vm.example")) as selected:
        selected.vm_http.json = MagicMock(side_effect=GatewayHTTPError(status, "creation failed"))
        vm = VM("fast", _service=selected)
        prepare = MagicMock()
        monkeypatch.setattr(vm, "prepare", prepare)
        with pytest.raises(GatewayHTTPError):
            vm.create()
        prepare.assert_not_called()
        assert vm._creation_body["request_id"] == vm.request_id
        assert vm._vm_http is selected.vm_http


@pytest.mark.parametrize("gpu", [GpuType.Any, GpuType.V100, GpuType.V100_32, GpuType.GAUDI2])
def test_incompatible_default_gpu_driver_fails_before_allocation(gpu):
    with ServiceClient(ConfigContext(token="test", api_url="https://vm.example")) as selected:
        selected.vm_http.json = MagicMock()
        with pytest.raises(ValueError, match="provide an image with a compatible guest driver"):
            VM("dev", gpu=gpu, ssh=False, _service=selected).create(wait=False)
        selected.vm_http.json.assert_not_called()


@pytest.mark.parametrize("source", ["image", "template", "snapshot"])
def test_custom_vm_sources_keep_their_gpu_driver_contract(source, monkeypatch):
    selected = service()
    selected.http.json.return_value = {"name": "dev", "id": "vm-id", "status": "starting"}
    monkeypatch.setattr(
        vm_module,
        "default_base_image",
        MagicMock(side_effect=AssertionError("Custom sources do not use the default driver")),
    )
    monkeypatch.setattr(
        vm_module._VMImage,
        "build",
        lambda self: SimpleNamespace(success=True, image_id="custom-image"),
    )
    options = {source: Image.from_id("custom-image") if source == "image" else "saved"}
    VM("dev", gpu=GpuType.V100, ssh=False, _service=selected, **options).create(wait=False)
    assert selected.http.json.call_args.kwargs["json"]["spec"]["gpu"] == "V100"


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
    # The shipped bundle has a fixed gzip header on every client.
    archive = base64.b64decode(a.build_steps[-1].command.split("'")[3])
    assert archive[9] == 255
    with tarfile.open(fileobj=io.BytesIO(gzip.decompress(archive))) as assets:
        assert "beam-desktop.service" in assets.getnames()
        assert assets.getmember("browser-trust").mode == 0o755
        installer = assets.extractfile("install").read().decode()
        assert "libnss3-tools" in installer
        assert "env GDK_SCALE=1 GDK_DPI_SCALE=1 pcmanfm --desktop" in installer
        assert "/etc/ca-certificates/update.d/beam-browser" in installer


def test_vm_image_bundle_matches_guest_files():
    directory = Path(vm_module.__file__).parent
    sources = {path.name: path for path in (directory / "assets").iterdir() if path.is_file()}
    sources["kiosk_config.py"] = directory / "kiosk_config.py"
    with tarfile.open(directory / "assets.tar.gz") as archive:
        assert set(archive.getnames()) == set(sources), "Run python bin/generate_vm_assets.py"
        for name, path in sources.items():
            assert archive.extractfile(name).read() == path.read_bytes(), (
                f"Stale VM bundle: run python bin/generate_vm_assets.py after updating {name}"
            )
            mode = 0o644 if path.suffix in (".service", ".desktop", ".svg", ".xml") else 0o755
            assert archive.getmember(name).mode == mode


@pytest.mark.parametrize("resource_exit", [None, 0, 1])
def test_xstartup_preserves_defaults_and_tolerates_failed_user_resources(
    tmp_path, monkeypatch, resource_exit
):
    directory = Path(vm_module.__file__).parent
    script = tmp_path / "xstartup"
    script.write_text(
        (directory / "assets/xstartup")
        .read_text()
        .replace("HOME=/root", f"HOME={shlex.quote(str(tmp_path))}")
    )
    commands = {
        "xrdb": (
            'if [ "$#" -eq 1 ]; then cat > "$TEST_DIR/defaults"; '
            'else cat "$2" > "$TEST_DIR/overrides"; exit "$RESOURCE_EXIT"; fi\n'
        ),
        "xsetroot": "exit 0\n",
        "dbus-run-session": 'printf "%s\\n" "$@" > "$TEST_DIR/launched"\n',
    }
    binaries = tmp_path / "bin"
    binaries.mkdir()
    for name, body in commands.items():
        executable = binaries / name
        executable.write_text("#!/bin/sh\n" + body)
        executable.chmod(0o755)
    if resource_exit is not None:
        (tmp_path / ".Xresources").write_text("XTerm*faceSize: 20\n")
    monkeypatch.setenv("PATH", str(binaries) + os.pathsep + os.defpath)
    monkeypatch.setenv("TEST_DIR", str(tmp_path))
    monkeypatch.setenv("RESOURCE_EXIT", str(resource_exit or 0))
    result = subprocess.run(["bash", str(script)], capture_output=True, text=True, timeout=5)
    assert result.returncode == 0, result.stderr
    assert "XTerm*faceSize: 12\n" in (tmp_path / "defaults").read_text()
    assert (tmp_path / "launched").read_text() == "--\nopenbox-session\n"
    if resource_exit is not None:
        assert (tmp_path / "overrides").read_text() == "XTerm*faceSize: 20\n"
    else:
        assert not (tmp_path / "overrides").exists()
    assert ("Unable to load ~/.Xresources" in result.stderr) == (resource_exit == 1)


def test_vm_image_does_not_depend_on_client_archive_libraries(monkeypatch):
    def unexpected(*args, **kwargs):
        raise AssertionError("VM image identity must not depend on client tar/gzip output")

    monkeypatch.setattr(tarfile, "open", unexpected)
    monkeypatch.setattr(gzip, "GzipFile", unexpected)
    prepared = prepare_image(Image(base_image="ubuntu:24.04"), service(), True)
    encoded = prepared.build_steps[-1].command.split("'")[3]
    assert (
        base64.b64decode(encoded)
        == Path(vm_module.__file__).with_name("assets.tar.gz").read_bytes()
    )


def test_desktop_clipboard_patch_is_idempotent_and_checks_upstream(tmp_path):
    installer = (Path(vm_module.__file__).parent / "assets/install").read_text()
    patch = installer.rsplit("  python3 - <<'PY'\n", 1)[1].split("\nPY\n", 1)[0]
    patch = patch.replace(
        "Path('/usr/share/kasmvnc/www/assets')", "Path(" + repr(str(tmp_path)) + ")"
    )
    client = tmp_path / "ui.js"
    client.write_text("class RFB { async clipboardPasteDataFrom(data) { return data; } }")
    exec(patch, {})
    once = client.read_text()
    exec(patch, {})
    assert client.read_text() == once, "Custom images may already contain the clipboard fix"
    assert "async beamClipboardPasteDataFrom(data)" in once
    client.write_text("class RFB { async differentClipboardAPI(data) { return data; } }")
    with pytest.raises(AssertionError, match="no longer matches"):
        exec(patch, {})


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


@pytest.mark.parametrize(
    "gpu,expected",
    [
        (None, None),
        ("RTX4090", "RTX4090"),
        ("NVIDIA GeForce RTX 4090", "RTX4090"),
        (GpuType.RTX4090, "RTX4090"),
        (GpuType.NoGPU, ""),
    ],
)
def test_vm_gpu_serialization_preserves_template_defaults(gpu, expected):
    selected = service()
    selected.http.json.return_value = {"id": "resource", "name": "dev", "status": "starting"}
    VM("dev", template="base", ssh=False, gpu=gpu, _service=selected).create(wait=False)
    spec = selected.http.json.call_args.kwargs["json"]["spec"]
    assert spec == ({"ssh": False} if expected is None else {"ssh": False, "gpu": expected})


@pytest.mark.parametrize("gpu", ["unknown", "RTX4090,L4", ["RTX4090"], 0])
def test_vm_rejects_invalid_gpu_before_creation(gpu):
    selected = service()
    with pytest.raises(ValueError):
        VM("dev", gpu=gpu, _service=selected)
    selected.http.json.assert_not_called()


def test_advertised_vm_support_skips_list_preflight():
    selected = service()
    selected.http.persistent_vms = True
    selected.http.json.return_value = {"id": "resource", "name": "dev", "status": "starting"}
    VM("dev", template="base", ssh=False, _service=selected).create(wait=False)
    assert [call.args[0] for call in selected.http.json.call_args_list] == ["POST"]


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


def test_metrics_asset_accounts_for_idle_cpu_and_available_memory(monkeypatch, capsys):
    reads = iter(
        [
            "cpu 100 0 0 80 20 0 0 0\n",
            "cpu 130 0 0 100 30 0 0 0\n",
            "MemTotal: 1000 kB\nMemAvailable: 250 kB\nHugePages_Total: 0\n",
        ]
    )
    monkeypatch.setattr("builtins.open", lambda *_: io.StringIO(next(reads)))
    monkeypatch.setattr(vm_module.time, "sleep", lambda _: None)
    monkeypatch.setattr(vm_module.time, "time", lambda: 123)
    monkeypatch.setattr(vm_module.os, "cpu_count", lambda: 4)
    monkeypatch.setattr(
        vm_module.os,
        "statvfs",
        lambda _: SimpleNamespace(f_blocks=100, f_bfree=25, f_frsize=4096),
    )
    script = Path(vm_module.__file__).parent / "assets/metrics"
    exec(compile(script.read_text(), str(script), "exec"), {})
    assert json.loads(capsys.readouterr().out) == {
        "timestamp": 123,
        "cpu_count": 4,
        "cpu_used_percent": 50,
        "memory_total_bytes": 1000 * 1024,
        "memory_used_bytes": 750 * 1024,
        "disk_total_bytes": 100 * 4096,
        "disk_used_bytes": 75 * 4096,
    }


@pytest.mark.parametrize("method", ["metrics", "kiosk_status"])
def test_guest_json_commands_preserve_errors(sandbox_vm, method):
    vm, sandbox = sandbox_vm
    vm.info["spec"]["kiosk"] = {"command": ["editor"]}
    process = sandbox.process.exec.return_value
    process.wait.return_value = 1
    process.stderr.read.return_value = "guest command failed"
    with pytest.raises(RuntimeError, match="guest command failed"):
        getattr(vm, method)()
    process.stdout.read.assert_not_called()


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


@pytest.mark.parametrize("window", [None, "0x0", "0x400001"])
def test_desktop_readiness_waits_for_window_manager(sandbox_vm, monkeypatch, window):
    vm, sandbox = sandbox_vm
    vm.info["spec"].update(desktop=True)
    connections = MagicMock()
    monkeypatch.setattr("socket.create_connection", connections)
    property_value = (
        f"_NET_SUPPORTING_WM_CHECK(WINDOW): window id # {window}"
        if window
        else "_NET_SUPPORTING_WM_CHECK: no such atom on any window."
    )
    monkeypatch.setattr(subprocess, "check_output", lambda *_: property_value.encode())

    def execute(command, *args):
        process = MagicMock()
        code = 0
        if command == "python3":
            try:
                exec(compile(args[1], "<readiness>", "exec", optimize=2), {})
            except SystemExit as error:
                code = error.code
        process.wait.return_value = code
        return process

    sandbox.process.exec.side_effect = execute
    assert vm._services_ready(sandbox, vm_module.time.monotonic() + 10) == (window == "0x400001")
    assert connections.called, "Listening ports alone do not make a desktop usable"


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


def test_vm_cli_registration_and_resource_args():
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
        "gpu",
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
    assert "gpu_count" not in options


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
    assert options["cpu"] is None and options["gpu"] is None
    assert options["desktop"] is None and options["ssh"] is None
    assert options["_service"] is cli_service
    fake.create.assert_called_once_with()
    fake.wait.assert_called_once_with(services=True)


def test_cli_forwards_gpu_with_typed_resources(cli_service):
    cli_service.http.json.return_value = {"name": "dev", "id": "resource", "status": "starting"}
    with patch.object(vm_cli.VM, "wait") as wait:
        result = CliRunner().invoke(
            vm_cli.management,
            [
                "new",
                "dev",
                "--template",
                "base",
                "--no-ssh",
                "--cpu",
                "2.5",
                "--memory",
                "4096",
                "--gpu",
                "NVIDIA GeForce RTX 4090",
                "--json",
            ],
        )
    assert result.exit_code == 0, result.output
    assert [call.kwargs for call in wait.call_args_list] == [{}, {"services": True}]
    assert cli_service.http.json.call_args.kwargs["json"]["spec"] == {
        "cpu": 2500,
        "memory": 4096,
        "gpu": "RTX4090",
        "ssh": False,
    }


def test_cli_rejects_invalid_gpu_before_creation(cli_service):
    result = CliRunner().invoke(vm_cli.management, ["new", "--gpu", "unknown"])
    assert result.exit_code == 1
    assert "not a valid GpuType" in result.output
    cli_service.http.json.assert_not_called()


@pytest.mark.parametrize(
    "args,base_image",
    [
        ([], None),
        (["--template", "base"], None),
        (["--image", "alpine:3"], "alpine:3"),
        (["--image", "custom-driver:latest", "--gpu", "any"], "custom-driver:latest"),
        (["--image-id", "custom-driver", "--gpu", "V100"], ""),
        (["--build-secret", "TOKEN"], vm_module.DEFAULT_VM_BASE_IMAGE),
        (
            ["--gpu", "RTX4090", "--build-secret", "TOKEN"],
            vm_module.DEFAULT_VM_GPU_BASE_IMAGE,
        ),
    ],
)
def test_cli_preserves_default_image_fast_path(cli_service, monkeypatch, args, base_image):
    vm = MagicMock()
    vm.info = {"name": "fast", "id": "vm-id", "status": "running"}
    factory = MagicMock(return_value=vm)
    monkeypatch.setattr(vm_cli, "VM", factory)
    result = CliRunner().invoke(vm_cli.management, ["new", *args])
    assert result.exit_code == 0, result.output
    image = factory.call_args.kwargs["image"]
    assert (image.base_image if image else None) == base_image
    vm.create.assert_called_once_with()
    vm.wait.assert_called_once_with(services=True)


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
@pytest.mark.parametrize(
    "args",
    [
        ["new", "dev", "--template", "base"],
        ["start", "dev"],
        ["start", "dev", "--cold"],
        ["resume", "dev"],
        ["fork", "dev", "copy"],
        ["fork", "snapshot", "copy"],
    ],
)
def test_cli_does_not_report_ready_when_services_fail(cli_service, monkeypatch, as_json, args):
    vm = MagicMock(info={"name": "dev", "id": "vm-id", "status": "running"})
    for method in ("create", "start", "fork"):
        getattr(vm, method).return_value = vm
    vm._api.return_value = [{"id": "snapshot"}]
    vm.wait.side_effect = TimeoutError("VM did not become ready; inspect vm get/logs")
    factory = MagicMock(return_value=vm)
    factory.get.return_value = vm
    monkeypatch.setattr(vm_cli, "VM", factory)
    result = CliRunner().invoke(vm_cli.management, [*args, *(["--json"] if as_json else [])])
    assert result.exit_code == 1
    assert "VM did not become ready" in result.output
    assert "✓ VM ready" not in result.output
    assert "vm-id" not in result.output
    vm.wait.assert_called_once_with(services=True)


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
    monkeypatch.setattr(vm_cli, "_launch_browser", opened)
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
    monkeypatch.setattr(vm_cli, "_launch_browser", opened)
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
@pytest.mark.parametrize("json_mode", ["command", "global", "env"])
def test_long_operations_keep_json_free_of_progress(cli_service, monkeypatch, args, json_mode):
    vm = MagicMock(info={"id": "vm-12ab34cd56ef7890", "name": "dev", "status": "running"})
    for method in ("create", "start", "pause", "stop", "fork"):
        getattr(vm, method).return_value = vm
    vm._api.return_value = [{"id": "snapshot"}]
    vm.snapshot.return_value = vm.create_template.return_value = vm.info
    factory = MagicMock(return_value=vm)
    factory.get.return_value = vm
    monkeypatch.setattr(vm_cli, "VM", factory)

    cli = load_cli(check_config=False, settings=SDKSettings(name="Beam"))
    argv = (["--json"] if json_mode == "global" else []) + ["vm", *args]
    if json_mode == "command":
        argv.append("--json")
    result = CliRunner().invoke(
        cli.common_group, argv, env={"BETA9_JSON": "1" if json_mode == "env" else "0"}
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == vm.info
    if args[0] in ("new", "start", "resume", "fork"):
        vm.wait.assert_called_once_with(services=True)
    else:
        vm.wait.assert_not_called()


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


@pytest.mark.skipif(sys.platform != "darwin", reason="macOS spawn regression")
def test_identity_does_not_fork(monkeypatch, identity_settings):
    def forbidden(*args, **kwargs):
        raise AssertionError("Forking while gRPC threads are active")

    monkeypatch.setattr(subprocess, "_fork_exec", forbidden)
    assert vm_module.identity().with_suffix(".pub").exists()


@pytest.mark.skipif(sys.platform != "darwin", reason="macOS spawn regression")
@pytest.mark.parametrize("command", ["ssh", "scp", "sync"])
def test_local_vm_commands_do_not_fork(cli_service, monkeypatch, tmp_path, command):
    for name in ("ssh", "scp", "rsync"):
        (tmp_path / name).symlink_to("/usr/bin/true")
    monkeypatch.setenv("PATH", str(tmp_path))
    monkeypatch.setattr(vm_cli, "_vm", lambda *_: SimpleNamespace(name="dev"))
    monkeypatch.setattr(vm_cli, "_ssh_options", lambda *_: [])

    def forbidden(*args, **kwargs):
        raise AssertionError("Forking while gRPC threads are active")

    monkeypatch.setattr(subprocess, "_fork_exec", forbidden)
    arguments = {
        "ssh": ["ssh", "dev", "echo", "a b;$USER"],
        "scp": ["scp", str(tmp_path), "dev:/workspace/"],
        "sync": ["sync", "dev", str(tmp_path)],
    }
    result = CliRunner().invoke(vm_cli.management, arguments[command])
    assert result.exit_code == 0, result.output


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


@pytest.mark.parametrize("mode", ["command", "global", "env"])
def test_cli_ps_uses_pid_indexed_processes(cli_service, monkeypatch, mode):
    vm = MagicMock()
    vm.process.list_processes.return_value = {
        44: SimpleNamespace(pid=44, args=["echo", "two words"], cwd="/", exit_code=0)
    }
    monkeypatch.setattr(vm_cli, "_vm", lambda *_: vm)
    cli = load_cli(check_config=False, settings=SDKSettings(name="Beam"))
    argv = (["--json"] if mode == "global" else []) + ["vm", "ps", "dev"]
    if mode == "command":
        argv.append("--json")
    result = CliRunner().invoke(
        cli.common_group, argv, env={"BETA9_JSON": "1" if mode == "env" else "0"}
    )
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


@pytest.mark.parametrize("size", [(2560, 1440), (1919, 1079)])
def test_desktop_size_reads_root_window_geometry(sandbox_vm, monkeypatch, size):
    vm, _ = sandbox_vm
    desktop = vm.desktop
    run = MagicMock(
        return_value=f"""xwininfo: Window id: 0x400 (the root window) (has no name)

  Absolute upper-left X:  0
  Absolute upper-left Y:  0
  Width: {size[0]}
  Height: {size[1]}
  Depth: 24
"""
    )
    monkeypatch.setattr(desktop, "_run", run)
    assert desktop.screen_size() == size
    run.assert_called_once_with("xwininfo", "-root")


def test_desktop_size_reports_unreadable_geometry(sandbox_vm, monkeypatch):
    vm, _ = sandbox_vm
    desktop = vm.desktop
    monkeypatch.setattr(desktop, "_run", lambda *args: "")
    with pytest.raises(RuntimeError, match="Unable to read desktop geometry"):
        desktop.screen_size()


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


@pytest.mark.parametrize(
    "method,path", [("GET", "/resource"), ("POST", "/resource/start"), ("POST", "")]
)
def test_idempotent_vm_requests_retry_lost_connections(monkeypatch, method, path):
    monkeypatch.setattr(vm_module.time, "sleep", lambda _: None)
    selected = service()
    selected.http.json.side_effect = [
        GatewayHTTPError(0, "connection closed"),
        {"status": "running"},
    ]
    body = {"request_id": "00000000-0000-4000-8000-000000000001"} if not path else {"cold": True}
    result = VM(_service=selected)._api(method, path, timeout=3, json=body)
    assert result == {"status": "running"}
    assert selected.http.json.call_count == 2
    assert all(call.kwargs["json"] == body for call in selected.http.json.call_args_list)
    assert all(call.kwargs["timeout"] <= 3 for call in selected.http.json.call_args_list)


@pytest.mark.parametrize(
    "method,path,status,attempts",
    [
        ("GET", "/resource", 0, 3),
        ("POST", "/resource/start", 0, 3),
        ("POST", "/resource/snapshot", 0, 1),
        ("POST", "/resource", 0, 1),
        ("POST", "", 0, 1),
        ("POST", "/resource/start", 403, 1),
    ],
)
def test_vm_connection_retries_are_bounded_and_only_idempotent(
    monkeypatch, method, path, status, attempts
):
    monkeypatch.setattr(vm_module.time, "sleep", lambda _: None)
    selected = service()
    selected.http.json.side_effect = GatewayHTTPError(status, "request failed")
    with pytest.raises(GatewayHTTPError):
        VM(_service=selected)._api(method, path, timeout=3)
    assert selected.http.json.call_count == attempts


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
@pytest.mark.parametrize("mode", ["command", "global", "env"])
def test_cli_json_exec_passes_stdin_only_for_foreground(cli_service, monkeypatch, detach, mode):
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
    cli = load_cli(check_config=False, settings=SDKSettings(name="Beam"))
    args = (["--json"] if mode == "global" else []) + ["vm", "exec"]
    args += (["--json"] if mode == "command" else []) + (["--detach"] if detach else [])
    args += ["dev", "cat"]
    result = CliRunner().invoke(
        cli.common_group,
        args,
        input="piped input",
        env={"BETA9_JSON": "1" if mode == "env" else "0"},
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["pid"] == 44
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
@pytest.mark.parametrize(
    "scenario,source",
    [
        ("desktop", "desktop.html"),
        ("kiosk", "kiosk.html"),
        ("webrtc", "kiosk-webrtc.js"),
        ("input", "kiosk-webrtc.js"),
    ],
)
def test_browser_client(scenario, source):
    subprocess.run(
        [
            "node",
            str(Path(__file__).with_suffix(".js")),
            scenario,
            str(Path(__file__).parents[2] / "pkg/abstractions/vm" / source),
        ],
        check=True,
        timeout=15,
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
    monkeypatch.setattr(vm_cli, "_launch_browser", opened)
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
    monkeypatch.setattr(vm_cli, "_launch_browser", opened)
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


@pytest.mark.parametrize("mode", ["command", "global", "env"])
@pytest.mark.parametrize("kiosk", [False, True])
def test_image_build_json_is_clean(cli_service, monkeypatch, tmp_path, mode, kiosk):
    (tmp_path / "Dockerfile").write_text("FROM ubuntu:24.04\n")
    image = MagicMock()

    def build():
        print("Image build output belongs in diagnostics")
        return SimpleNamespace(success=True, image_id="image-test")

    image.build.side_effect = build
    monkeypatch.setattr(vm_cli, "prepare_image", lambda *_, **__: image)
    cli = load_cli(check_config=False, settings=SDKSettings(name="Beam"))
    args = (["--json"] if mode == "global" else []) + ["vm", "image", "build", str(tmp_path)]
    if mode == "command":
        args.append("--json")
    if kiosk:
        args.append("--kiosk")
    result = CliRunner(mix_stderr=False).invoke(
        cli.common_group, args, env={"BETA9_JSON": "1" if mode == "env" else "0"}
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == {"image_id": "image-test", "desktop": kiosk}
    assert "Image build output belongs in diagnostics" in result.stderr


@pytest.mark.skipif(sys.platform != "darwin", reason="macOS browser fork regression")
def test_browser_opener_does_not_fork_and_preserves_url(monkeypatch, tmp_path):
    (tmp_path / "webbrowser.py").write_text(
        "import os\nfrom pathlib import Path\n"
        "def open(url):\n Path(os.environ['BROWSER_RESULT']).write_text(url)\n return True\n"
    )
    output = tmp_path / "opened-url"
    monkeypatch.setenv("PYTHONPATH", str(tmp_path))
    monkeypatch.setenv("BROWSER_RESULT", str(output))

    def forbidden(*args, **kwargs):
        raise AssertionError("Forking while gRPC threads are active")

    monkeypatch.setattr(subprocess, "_fork_exec", forbidden)
    url = "https://vm.example/?beam_vm_session=literal&x=$VALUE"
    vm_cli._launch_browser(url)
    assert output.read_text() == url


@pytest.mark.parametrize("background", [False, True])
def test_codex_prompt_supports_fresh_workspace(cli_service, monkeypatch, background):
    vm = MagicMock()
    vm.process.exec.return_value.wait.return_value = 0
    monkeypatch.setattr(vm_cli, "_vm", lambda *_: vm)
    foreground = MagicMock()
    monkeypatch.setattr(vm_cli.exec_vm.callback, "__wrapped__", foreground)
    args = ["prompt", "dev", "literal ;$prompt", "--agent", "codex"]
    if background:
        args.append("--background")
    result = CliRunner().invoke(vm_cli.management, args)
    assert result.exit_code == 0, result.output
    command = vm.process.exec.call_args.args[-1] if background else foreground.call_args.args[2][-1]
    assert "codex exec --skip-git-repo-check 'literal ;$prompt'" in command


# Connection errors


@pytest.mark.parametrize("command", [["exec", "dev", "true"], ["ssh", "dev"]])
@pytest.mark.parametrize("name", ["Beta9", "Beam"])
def test_stopped_vm_shows_start_command(monkeypatch, command, name):
    monkeypatch.setattr(get_settings(), "name", name)
    start_command = f"{name.lower()} vm start dev"
    selected = SimpleNamespace(channel=MagicMock(), http=MagicMock())
    vm = VM(_service=selected)._set(
        {
            "id": "vm-0123456789abcdef",
            "name": "dev",
            "status": "stopped",
            "spec": {"ssh": True},
        }
    )
    monkeypatch.setattr(vm, "refresh", lambda: vm)
    client = MagicMock()
    client.__enter__.return_value = selected
    monkeypatch.setattr(extraclick, "ServiceClient", lambda _: client)
    monkeypatch.setattr(extraclick, "get_config_context", lambda _: SimpleNamespace())
    monkeypatch.setattr(vm_cli, "_vm", lambda *_: vm)
    with pytest.raises(SandboxConnectionError, match=start_command):
        vm._sandbox()
    result = CliRunner().invoke(vm_cli.management, command)
    assert result.exit_code == 1
    assert result.output == (f"Error: VM is not running. Start it with: {start_command}\n")


# Listing


def record(status="running"):
    return {
        "name": "warm-willow",
        "id": "vm-ff074591ef40c10d",
        "status": status,
        "container_id": "current-boot",
        "created_at": "2020-01-01T00:00:00Z",
        "spec": {"cpu": 1500, "memory": 512, "disk_size": str(100 << 30)},
    }


@pytest.mark.parametrize("width", [80, 120])
def test_list_uses_current_boot_and_preserves_filters(cli_service, monkeypatch, width):
    records = [record(), {**record("paused"), "name": "paused-vm", "id": "vm-0000000000000001"}]
    listing = MagicMock(return_value=records)
    monkeypatch.setattr(vm_cli.VM, "list", listing)
    now = datetime.datetime.now(datetime.timezone.utc)
    cli_service.gateway.list_containers.return_value = SimpleNamespace(
        ok=True,
        containers=[
            SimpleNamespace(
                container_id="previous-boot", started_at=now - datetime.timedelta(days=10)
            ),
            SimpleNamespace(
                container_id="current-boot", started_at=now - datetime.timedelta(minutes=7)
            ),
        ],
    )
    result = CliRunner().invoke(
        vm_cli.management,
        ["list", "--metadata", "team=a=b", "--status", "running", "--all"],
        env={"COLUMNS": str(width)},
    )

    assert result.exit_code == 0, result.output
    assert all(len(line) <= width for line in result.output.splitlines())
    assert all(
        value in result.output
        for value in [
            "Name",
            "Status",
            "Size",
            "Disk",
            "Uptime",
            "ID",
            "1.50CPU",
            "512MiB",
            "100GiB",
            "7m",
            "2 VMs",
        ]
    )
    assert "10d" not in result.output.split()
    assert "7m" not in next(line for line in result.output.splitlines() if "paused-vm" in line)
    listing.assert_called_once_with(
        all=True, metadata={"team": "a=b"}, status="running", _service=cli_service
    )
    cli_service.gateway.list_containers.assert_called_once()


@pytest.mark.parametrize("records", [[], [record()]])
@pytest.mark.parametrize("mode", ["command", "global", "env"])
def test_json_is_unchanged_and_needs_no_container_lookup(cli_service, monkeypatch, records, mode):
    monkeypatch.setattr(vm_cli.VM, "list", lambda **kwargs: records)
    cli = load_cli(check_config=False, settings=SDKSettings(name="Beam"))
    argv = (["--json"] if mode == "global" else []) + ["vm", "list"]
    if mode == "command":
        argv.append("--json")
    result = CliRunner().invoke(
        cli.common_group, argv, env={"BETA9_JSON": "1" if mode == "env" else "0"}
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == records
    cli_service.gateway.list_containers.assert_not_called()


def test_stopped_list_needs_no_container_lookup(cli_service, monkeypatch):
    monkeypatch.setattr(vm_cli.VM, "list", lambda **kwargs: [record("stopped")])
    result = CliRunner().invoke(vm_cli.management, ["list"], env={"COLUMNS": "120"})
    assert result.exit_code == 0, result.output
    assert "100GiB" in result.output and "1 VM" in result.output
    cli_service.gateway.list_containers.assert_not_called()


def test_runtime_lookup_failure_does_not_hide_vms(cli_service, monkeypatch):
    monkeypatch.setattr(vm_cli.VM, "list", lambda **kwargs: [record()])
    cli_service.gateway.list_containers.side_effect = grpc.RpcError()
    result = CliRunner().invoke(vm_cli.management, ["list"], env={"COLUMNS": "120"})
    assert result.exit_code == 0, result.output
    assert "warm-willow" in result.output and "100GiB" in result.output


@pytest.mark.parametrize("name", ["beta9", "beam"])
def test_empty_state_uses_the_cli_name(cli_service, monkeypatch, name):
    monkeypatch.setattr(vm_cli.VM, "list", lambda **kwargs: [])
    monkeypatch.setattr(vm_cli.terminal, "cli_name", lambda: name)
    result = CliRunner().invoke(vm_cli.management, ["list"])
    assert result.exit_code == 0, result.output
    assert "No VMs found" in result.output and f"{name} vm new" in result.output
    cli_service.gateway.list_containers.assert_not_called()


# Sharing


@pytest.fixture
def sharing(monkeypatch):
    info = {
        "id": "vm-id",
        "name": "dev",
        "desktop_url": "https://desktop.example/",
        "terminal_url": "https://terminal.example/",
        "urls": {"3000": "https://app.example/"},
        "spec": {"protected_ports": [7681, 8080, 3000]},
    }
    client = MagicMock()
    service = client.__enter__.return_value
    monkeypatch.setattr(extraclick, "ServiceClient", lambda _: client)
    monkeypatch.setattr(extraclick, "get_config_context", lambda _: SimpleNamespace())
    session = {"url": "https://terminal.example/?beam_vm_session=share", "expires_at": 1792080000}
    service.http.json.side_effect = [info, session]
    copied = MagicMock(return_value=True)
    monkeypatch.setattr(vm_cli, "_copy_share_link", copied)
    monkeypatch.setattr(vm_cli.terminal, "is_interactive", lambda: True)
    opened = MagicMock()
    monkeypatch.setattr(vm_cli.webbrowser, "open", opened)
    return SimpleNamespace(
        service=service, info=info, session=session, copied=copied, opened=opened
    )


def test_share_defaults_to_a_terminal_link_copied_for_one_hour(sharing):
    result = CliRunner().invoke(vm_cli.management, ["share", "dev"])
    assert result.exit_code == 0, result.output
    assert "Terminal link copied to clipboard" in result.output
    assert sharing.session["url"] in result.output
    assert "Expires in 1 hour" in result.output
    assert "Anyone with this link can use this terminal. No sign-in needed." in result.output
    assert sharing.service.http.json.call_args.kwargs["json"] == {"port": 7681, "ttl": 3600}
    sharing.copied.assert_called_once_with(sharing.session["url"])
    sharing.opened.assert_not_called()


@pytest.mark.parametrize(
    "args,port,label",
    [
        (["--desktop"], 8080, "Desktop"),
        (["--port", "3000"], 3000, "Port 3000"),
        (["8080"], 8080, "Desktop"),
    ],
)
def test_desktop_and_app_ports_are_explicit(sharing, args, port, label):
    result = CliRunner().invoke(vm_cli.management, ["share", "dev", *args])
    assert result.exit_code == 0, result.output
    assert f"{label} link copied" in result.output
    assert sharing.service.http.json.call_args.kwargs["json"] == {"port": port, "ttl": 3600}


@pytest.mark.parametrize(
    "args,ttl,copy",
    [
        (["--expires-in", "10m"], 600, "10 minutes"),
        (["--expires-in", "45s"], 45, "45 seconds"),
        (["--ttl", "900"], 900, "15 minutes"),
    ],
)
def test_shorter_shares_use_readable_durations(sharing, args, ttl, copy):
    result = CliRunner().invoke(vm_cli.management, ["share", "dev", *args])
    assert result.exit_code == 0, result.output
    assert "Expires in " + copy in result.output
    assert sharing.service.http.json.call_args.kwargs["json"]["ttl"] == ttl


@pytest.mark.parametrize(
    "args",
    [
        ["--expires-in", "0m"],
        ["--expires-in", "2h"],
        ["--expires-in", "forever"],
        ["--desktop", "--port", "3000"],
        ["8080", "--desktop"],
        ["--ttl", "600", "--expires-in", "10m"],
        ["--url", "--json"],
    ],
)
def test_invalid_or_ambiguous_shares_fail_before_creating_a_link(sharing, args):
    result = CliRunner().invoke(vm_cli.management, ["share", "dev", *args])
    assert result.exit_code == 2, result.output
    sharing.service.http.json.assert_not_called()
    sharing.copied.assert_not_called()


@pytest.mark.parametrize("mode", ["--url", "--json", "global-json"])
def test_machine_output_does_not_touch_the_clipboard(sharing, monkeypatch, mode):
    if mode == "global-json":
        monkeypatch.setenv("BETA9_JSON", "1")
    result = CliRunner().invoke(
        vm_cli.management, ["share", "dev", *([] if mode == "global-json" else [mode])]
    )
    assert result.exit_code == 0, result.output
    if mode == "--url":
        assert result.output == sharing.session["url"] + "\n"
    else:
        assert json.loads(result.output) == {
            "name": "dev",
            "port": 7681,
            "public": False,
            **sharing.session,
        }
    sharing.copied.assert_not_called()


def test_missing_clipboard_does_not_prevent_sharing(sharing):
    sharing.copied.return_value = False
    result = CliRunner().invoke(vm_cli.management, ["share", "dev"])
    assert result.exit_code == 0, result.output
    assert sharing.session["url"] in result.output
    assert "copied" not in result.output


def test_piped_output_does_not_overwrite_the_users_clipboard(sharing, monkeypatch):
    monkeypatch.setattr(vm_cli.terminal, "is_interactive", lambda: False)
    result = CliRunner().invoke(vm_cli.management, ["share", "dev"])
    assert result.exit_code == 0, result.output
    assert sharing.session["url"] in result.output
    sharing.copied.assert_not_called()


def test_public_port_returns_its_existing_url_without_claiming_expiry(sharing):
    sharing.info["spec"]["protected_ports"] = []
    result = CliRunner().invoke(vm_cli.management, ["share", "dev", "--port", "3000"])
    assert result.exit_code == 0, result.output
    assert "https://app.example/" in result.output
    assert "public" in result.output and "does not expire" in result.output
    sharing.service.http.json.assert_called_once()


def test_an_expiring_share_cannot_silently_become_a_public_permanent_link(sharing):
    sharing.info["spec"]["protected_ports"] = []
    result = CliRunner().invoke(
        vm_cli.management, ["share", "dev", "--port", "3000", "--expires-in", "10m"]
    )
    assert result.exit_code == 1, result.output
    assert "public" in result.output and "--protected" in result.output
    sharing.service.http.json.assert_called_once()
    sharing.copied.assert_not_called()


def test_clipboard_failure_falls_back_without_exposing_the_link_to_a_shell(monkeypatch):
    monkeypatch.setattr(vm_cli.sys, "platform", "linux")
    monkeypatch.setattr(vm_cli.shutil, "which", lambda name: "/usr/bin/" + name)
    run = MagicMock(side_effect=[subprocess.CalledProcessError(1, "wl-copy"), None])
    monkeypatch.setattr(vm_cli.subprocess, "run", run)
    link = "https://terminal.example/?beam_vm_session=share"
    assert vm_cli._copy_share_link(link) is True
    assert run.call_count == 2
    assert run.call_args.args == (["/usr/bin/xclip", "-selection", "clipboard"],)
    assert run.call_args.kwargs["input"] == link
    assert "shell" not in run.call_args.kwargs


@pytest.mark.skipif(
    sys.platform == "win32", reason="macOS uses the POSIX subprocess implementation"
)
def test_macos_clipboard_avoids_fork(monkeypatch):
    monkeypatch.setattr(vm_cli.sys, "platform", "darwin")
    # Consume stdin without touching the user's real clipboard.
    monkeypatch.setattr(vm_cli.shutil, "which", lambda name: "/bin/cat")

    def unexpected_fork(*args, **kwargs):
        raise AssertionError("Clipboard copying must not fork active gRPC threads")

    monkeypatch.setattr(subprocess, "_fork_exec", unexpected_fork)
    assert vm_cli._copy_share_link("https://desktop.example/?beam_vm_session=test") is True


# Onboarding


@pytest.mark.parametrize("signed_in", [False, True])
def test_agent_login_connects_workspace_without_workload_onboarding(monkeypatch, signed_in):
    from click.testing import CliRunner
    from beta9.cli import setup

    monkeypatch.setenv("BETA9_JSON", "1")
    context = ConfigContext(token="workspace-token")
    monkeypatch.setattr("beta9.config.context_defaults", lambda _: context)
    monkeypatch.setattr("beta9.config.get_config_context", lambda _: context)
    monkeypatch.setattr(
        "beta9.mcp.server.context_or_none", lambda _: context if signed_in else None
    )
    start = MagicMock()
    start.return_value.verification_uri_complete = "https://login.example/device"
    start.return_value.wait.return_value = context
    monkeypatch.setattr("beta9.auth.DeviceLogin.start", start)
    save = MagicMock()
    monkeypatch.setattr("beta9.auth.save_login", save)
    credits = MagicMock(side_effect=AssertionError("installation must not check funding"))
    vm = MagicMock(side_effect=AssertionError("installation must not create a VM"))
    example = MagicMock(side_effect=AssertionError("installation must not create a workload"))
    monkeypatch.setattr(setup, "ensure_credit", credits)
    monkeypatch.setattr(onboarding, "prepare_vm", vm)
    monkeypatch.setattr(onboarding, "prepare_example", example)
    configured = {"signed_in": True, "clients": [], "skills": {}, "mcp": {}, "failed": {}}
    configure = MagicMock(return_value=configured)
    monkeypatch.setattr(setup, "configure_agents", configure)

    result = CliRunner(mix_stderr=False).invoke(
        setup.common, ["setup", "agent", "--login", "--context", "staging"]
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["signed_in"] is True
    configure.assert_called_once_with((), False, False, "staging")
    if signed_in:
        start.assert_not_called()
        save.assert_not_called()
    else:
        start.assert_called_once_with(context)
        save.assert_called_once_with(context, "staging")
        assert "https://login.example/device" in result.stderr
    credits.assert_not_called()
    vm.assert_not_called()
    example.assert_not_called()


def test_agent_registration_without_login_remains_local(monkeypatch):
    from click.testing import CliRunner
    from beta9.cli import setup

    monkeypatch.setenv("BETA9_JSON", "1")
    connect = MagicMock(side_effect=AssertionError("local registration must not sign in"))
    monkeypatch.setattr(setup, "connect_workspace", connect)
    monkeypatch.setattr(
        setup,
        "configure_agents",
        MagicMock(
            return_value={
                "signed_in": False,
                "clients": [],
                "skills": {},
                "mcp": {},
                "failed": {},
            }
        ),
    )
    result = CliRunner().invoke(setup.common, ["setup", "agent"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["signed_in"] is False
    connect.assert_not_called()


def test_agent_login_failure_stops_setup(monkeypatch):
    from click.testing import CliRunner
    from beta9.auth import LoginError
    from beta9.cli import setup

    monkeypatch.setattr(
        setup, "connect_workspace", MagicMock(side_effect=LoginError("Sign-in expired"))
    )
    configure = MagicMock()
    monkeypatch.setattr(setup, "configure_agents", configure)
    result = CliRunner().invoke(setup.common, ["setup", "agent", "--login"])
    assert result.exit_code != 0
    assert "Sign-in expired" in result.output
    configure.assert_not_called()


def test_vm_hourly_price_uses_live_vm_rates(monkeypatch):
    from decimal import Decimal
    from beta9.cli import setup

    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"vm": {"cpu": "0.0000000011875", "memory": "0.00000000059375"}}
    get = MagicMock(return_value=response)
    monkeypatch.setattr("requests.get", get)
    assert setup.vm_hourly_price(ConfigContext(api_url="https://api.example/v2")) == Decimal(
        "0.0342"
    )
    get.assert_called_once_with(
        "https://api.example/v2/payment/get-resource-rates/", timeout=10, allow_redirects=False
    )


@pytest.mark.parametrize(
    "rates", [{}, {"vm": {"cpu": "NaN", "memory": "1"}}, {"vm": {"cpu": "-1", "memory": "1"}}]
)
def test_invalid_vm_pricing_is_not_presented_as_a_free_vm(monkeypatch, rates):
    from beta9.cli import setup

    response = MagicMock(status_code=200)
    response.json.return_value = rates
    monkeypatch.setattr("requests.get", lambda *_, **__: response)
    with pytest.raises(RuntimeError, match="hourly price"):
        setup.vm_hourly_price(ConfigContext(api_url="https://api.example/v2"))


def test_setup_vm_confirmation_includes_hourly_price(monkeypatch):
    from decimal import Decimal
    from click.testing import CliRunner
    from beta9.cli import setup

    monkeypatch.setattr(setup, "connect_onboarding", lambda *_: (ConfigContext(), None))
    monkeypatch.setattr(setup, "vm_hourly_price", lambda _: Decimal("0.0342"))
    result = CliRunner().invoke(setup.common, ["setup", "vm"], input="n\n")
    assert result.exit_code == 0, result.output
    assert "$0.0342/hour" in result.output


def test_setup_vm_displays_allocated_ipv6(tmp_path, monkeypatch):
    from click.testing import CliRunner
    from beta9.cli import setup

    monkeypatch.setattr(setup, "connect_onboarding", lambda *_: (ConfigContext(), None))
    monkeypatch.setattr(
        setup, "get_settings", lambda: SimpleNamespace(config_path=tmp_path / "config")
    )
    monkeypatch.setattr(setup, "dashboard_link", lambda *_: None)
    monkeypatch.setattr("beta9.config.get_config_context", lambda _: ConfigContext())
    monkeypatch.setattr("beta9.channel.ServiceClient", lambda _: MagicMock())
    address = "2600:1f18:1234:5678:100:abcd:ef01:2345"
    monkeypatch.setattr(
        onboarding,
        "prepare_vm",
        lambda *_, **__: {"name": "dev", "id": "vm-id", "public_ipv6": address},
    )
    result = CliRunner().invoke(setup.common, ["setup", "vm", "--skip-agents", "--yes"])
    assert result.exit_code == 0, result.output
    assert "IPv6" in result.output and address in result.output


@pytest.mark.parametrize("as_json", [False, True])
def test_setup_browser_links_use_owner_auth_without_minting_credentials(
    tmp_path, monkeypatch, as_json
):
    from click.testing import CliRunner
    from beta9.cli import setup

    if as_json:
        monkeypatch.setenv("BETA9_JSON", "1")
    else:
        monkeypatch.delenv("BETA9_JSON", raising=False)
    monkeypatch.setattr(setup, "connect_onboarding", lambda *_: (ConfigContext(), None))
    monkeypatch.setattr(
        setup, "get_settings", lambda: SimpleNamespace(config_path=tmp_path / "config")
    )
    monkeypatch.setattr(
        setup, "dashboard_link", lambda *_: "https://dash.example/vms/vm-id?tab=logs"
    )
    monkeypatch.setattr("beta9.config.get_config_context", lambda _: ConfigContext())
    selected = MagicMock(_config=ConfigContext())
    client = MagicMock()
    client.__enter__.return_value = selected
    monkeypatch.setattr("beta9.channel.ServiceClient", lambda _: client)
    info = {
        "name": "dev",
        "id": "vm-id",
        "terminal_url": "https://terminal.example/",
        "desktop_url": "https://desktop.example/",
        "spec": {"protected_ports": [7681, 8080]},
    }
    monkeypatch.setattr(onboarding, "prepare_vm", lambda *_, **__: info)
    result = CliRunner(mix_stderr=False).invoke(
        setup.common, ["setup", "vm", "--skip-agents", "--yes"]
    )
    assert result.exit_code == 0, result.output
    if as_json:
        assert json.loads(result.stdout)["vm"] == info
        selected.http.json.assert_not_called()
    else:
        assert "https://terminal.example/" in result.stdout
        assert "https://desktop.example/" in result.stdout
        assert "dash.example" not in result.stdout
        assert "beam_vm_session" not in result.stdout
        assert "expire" not in result.stdout
    selected.http.json.assert_not_called()
    assert info["terminal_url"] == "https://terminal.example/"


def test_lost_creation_response_resumes_identical_request(tmp_path, monkeypatch):
    selected = SimpleNamespace(http=SimpleNamespace(workspace_id="workspace"))
    vm = MagicMock(info={"id": "vm-id"})
    vm._creation_body = None
    vm.prepare.side_effect = lambda: setattr(
        vm, "_creation_body", {"request_id": "original", "spec": {"image_id": "immutable"}}
    )
    vm.create.side_effect = [TimeoutError("lost response"), None]
    vm.process.exec.return_value.wait.return_value = 0
    vm.process.exec.return_value.stdout.read.return_value = "Linux\n"
    vm.refresh.return_value.info = {"name": "first", "id": "vm-id"}
    factory = MagicMock(return_value=vm)
    factory.get.side_effect = GatewayHTTPError(404, "missing")
    monkeypatch.setattr(onboarding, "VM", factory)
    with pytest.raises(TimeoutError):
        onboarding.prepare_vm(selected, tmp_path, "first")
    saved = json.loads(next(tmp_path.rglob("*.json")).read_text())
    assert saved["creation_body"] == vm._creation_body
    result = onboarding.prepare_vm(selected, tmp_path, "first")
    assert result["id"] == "vm-id"
    assert vm.prepare.call_count == 1
    assert vm.create.call_count == 2
    assert (next(tmp_path.rglob("*.json")).stat().st_mode & 0o777) == 0o600


@pytest.mark.parametrize("state", ["stopped", "paused"])
def test_unnamed_vm_takes_the_generated_name_and_resumes_by_id(tmp_path, monkeypatch, state):
    selected = SimpleNamespace(http=SimpleNamespace(workspace_id="workspace"))
    vm = MagicMock(info={"id": "vm-id"})
    vm._creation_body = None
    vm.prepare.side_effect = lambda: setattr(vm, "_creation_body", {"request_id": "original"})
    vm.process.exec.return_value.wait.return_value = 0
    vm.process.exec.return_value.stdout.read.return_value = "Linux\n"
    vm.refresh.return_value.info = {"name": "happy-otter-1a2b3c", "id": "vm-id"}
    factory = MagicMock(return_value=vm)
    monkeypatch.setattr(onboarding, "VM", factory)

    assert onboarding.prepare_vm(selected, tmp_path)["name"] == "happy-otter-1a2b3c"
    assert factory.call_args.args == (None,)
    assert factory.call_args.kwargs["desktop"] is True
    factory.get.assert_not_called()

    vm.info.update(
        metadata={"beam.onboarding": factory.call_args.kwargs["request_id"]},
        desired_state=state,
    )
    factory.get.return_value = vm
    onboarding.prepare_vm(selected, tmp_path)
    factory.get.assert_called_once_with("vm-id", _service=selected)
    vm.start.assert_called_once_with(wait=False)
    assert factory.call_count == 1


@pytest.mark.parametrize("lose_response", [False, True])
def test_removed_vm_renews_request_and_can_resume(tmp_path, monkeypatch, lose_response):
    directory = tmp_path / "state"
    directory.mkdir()
    path = directory / "vm.json"
    old_request = {
        "request_id": "removed",
        "creation_body": {"request_id": "removed", "spec": {"image_id": "old"}},
    }
    onboarding._save_state(path, old_request)
    removed, replacement = MagicMock(), MagicMock(info={"id": "vm-id"})
    removed.create.side_effect = GatewayHTTPError(410, "the VM for this request_id was removed")
    replacement._creation_body = None
    replacement.prepare.side_effect = lambda: setattr(
        replacement,
        "_creation_body",
        {"request_id": factory.call_args.kwargs["request_id"], "spec": {"image_id": "new"}},
    )
    replacement.create.side_effect = (
        [TimeoutError("lost response"), None] if lose_response else None
    )
    factory = MagicMock(side_effect=[removed, replacement, replacement])
    monkeypatch.setattr(onboarding, "VM", factory)

    if lose_response:
        with pytest.raises(TimeoutError):
            onboarding._create_vm(object(), path, old_request, "first", False)
        saved = json.loads(path.read_text())
        vm = onboarding._create_vm(object(), path, saved, "first", False)
        assert factory.call_args.kwargs["request_id"] == saved["request_id"]
    else:
        vm = onboarding._create_vm(object(), path, old_request, "first", False)

    saved = json.loads(path.read_text())
    assert vm is replacement
    assert saved["request_id"] != "removed"
    assert saved["creation_body"]["request_id"] == saved["request_id"]
    assert saved["creation_body"]["spec"]["image_id"] == "new"
    assert factory.call_args.kwargs["metadata"] == {"beam.onboarding": saved["request_id"]}
    replacement.prepare.assert_called_once()


@pytest.mark.parametrize("status", [403, 409, 410, 500])
def test_creation_retries_only_one_removed_request(tmp_path, monkeypatch, status):
    vm = MagicMock()
    vm._creation_body = {"request_id": "original"}
    vm.create.side_effect = GatewayHTTPError(status, "creation failed")
    factory = MagicMock(return_value=vm)
    monkeypatch.setattr(onboarding, "VM", factory)
    state = {"request_id": "original", "creation_body": vm._creation_body}
    path = tmp_path / "vm.json"
    onboarding._save_state(path, state)
    with pytest.raises(GatewayHTTPError):
        onboarding._create_vm(object(), path, state, "first", False)
    assert vm.create.call_count == (2 if status == 410 else 1)
    if status != 410:
        assert json.loads(path.read_text()) == state


@pytest.mark.parametrize("kind", ["vm", "sandboxes", "inference", "functions"])
@pytest.mark.parametrize(
    "answer,exit_code,message", [("n\n", 0, "Setup canceled."), (None, 1, "Aborted!")]
)
def test_declining_or_interrupting_setup_never_starts_compute(
    monkeypatch, kind, answer, exit_code, message
):
    from click.testing import CliRunner
    import click

    from beta9.cli import setup

    monkeypatch.setattr(setup, "connect_onboarding", lambda *_: (ConfigContext(), None))
    if answer is None:
        monkeypatch.setattr(setup.click, "confirm", MagicMock(side_effect=click.Abort()))
    service = MagicMock()
    monkeypatch.setattr("beta9.channel.ServiceClient", service)
    result = CliRunner().invoke(setup.common, ["setup", kind], input=answer)
    assert result.exit_code == exit_code, result.output
    assert message in result.output
    assert "Error:" not in result.output
    service.assert_not_called()


@pytest.mark.parametrize("kind", ["vm", "sandboxes", "inference", "functions"])
def test_cancelled_setup_emits_one_json_result(monkeypatch, kind):
    from click.testing import CliRunner

    from beta9.cli import setup

    monkeypatch.setenv("BETA9_JSON", "1")
    monkeypatch.setattr(setup, "connect_onboarding", lambda *_: (ConfigContext(), None))
    result = CliRunner(mix_stderr=False).invoke(setup.common, ["setup", kind], input="n\n")
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == {"ready": False, "cancelled": True}


@pytest.mark.parametrize("kind", ["vm", "sandboxes", "inference", "functions"])
def test_interrupting_credit_checkpoint_is_not_wrapped_as_empty_error(monkeypatch, kind):
    import click
    from click.testing import CliRunner

    from beta9.cli import setup

    monkeypatch.setattr(setup, "connect_onboarding", MagicMock(side_effect=click.Abort()))
    result = CliRunner().invoke(setup.common, ["setup", kind])
    assert result.exit_code == 1
    assert "Aborted!" in result.output
    assert "Error:" not in result.output


def test_existing_vm_is_never_adopted_by_name(tmp_path, monkeypatch):
    factory = MagicMock()
    factory.get.return_value.info = {"metadata": {}}
    monkeypatch.setattr(onboarding, "VM", factory)
    with pytest.raises(ValueError, match="already exists"):
        onboarding.prepare_vm(
            SimpleNamespace(http=SimpleNamespace(workspace_id="ws")), tmp_path, "existing"
        )
    factory.get.return_value.start.assert_not_called()
    factory.get.return_value.process.exec.assert_not_called()


def test_authorization_failure_does_not_create_vm(tmp_path, monkeypatch):
    factory = MagicMock()
    factory.get.side_effect = GatewayHTTPError(403, "denied")
    monkeypatch.setattr(onboarding, "VM", factory)
    with pytest.raises(GatewayHTTPError):
        onboarding.prepare_vm(
            SimpleNamespace(http=SimpleNamespace(workspace_id="ws")), tmp_path, "first"
        )
    factory.assert_not_called()


@pytest.mark.parametrize(
    ("args", "name", "desktop"),
    [(["--name", "first"], "first", True), ([], None, True), (["--no-desktop"], None, False)],
)
def test_setup_vm_emits_one_json_result_after_guest_readiness(
    tmp_path, monkeypatch, args, name, desktop
):
    from click.testing import CliRunner

    from beta9.cli import setup

    monkeypatch.setenv("BETA9_JSON", "1")
    monkeypatch.setattr(setup, "ensure_credit", lambda *_: None)
    monkeypatch.setattr(
        setup, "dashboard_link", lambda *_: "https://dashboard.example/vms/vm-ready?tab=logs"
    )
    monkeypatch.setattr("beta9.mcp.server.context_or_none", lambda _: object())
    monkeypatch.setattr(
        "beta9.config.get_config_context",
        lambda _: ConfigContext(),
    )
    monkeypatch.setattr(
        setup, "get_settings", lambda: SimpleNamespace(name="Beam", config_path=tmp_path / "config")
    )
    service = MagicMock()
    monkeypatch.setattr("beta9.channel.ServiceClient", lambda _: service)
    configured = {"failed": {}, "clients": ["codex"], "skills": {}, "mcp": {}}
    monkeypatch.setattr(setup, "configure_agents", lambda *_: configured)
    prepare = MagicMock(return_value={"name": "first", "id": "vm-ready"})
    monkeypatch.setattr(onboarding, "prepare_vm", prepare)
    result = CliRunner(mix_stderr=False).invoke(
        setup.common, ["setup", "vm", "--context", "staging", *args, "--yes"]
    )
    assert result.exit_code == 0, result.output
    report = json.loads(result.stdout)
    assert report == {
        "ready": True,
        "context": "staging",
        "agents": configured,
        "vm": {"name": "first", "id": "vm-ready"},
        "dashboard_url": "https://dashboard.example/vms/vm-ready?tab=logs",
    }
    prepare.assert_called_once()
    assert prepare.call_args.args[2] == name
    assert prepare.call_args.kwargs["desktop"] is desktop


def test_setup_vm_does_not_report_success_after_failed_probe(tmp_path, monkeypatch):
    from click.testing import CliRunner

    from beta9.cli import setup

    monkeypatch.setattr(setup, "ensure_credit", lambda *_: None)
    monkeypatch.setattr("beta9.mcp.server.context_or_none", lambda _: object())
    monkeypatch.setattr(
        "beta9.config.get_config_context",
        lambda _: ConfigContext(),
    )
    monkeypatch.setattr(
        setup, "get_settings", lambda: SimpleNamespace(name="Beam", config_path=tmp_path / "config")
    )
    monkeypatch.setattr("beta9.channel.ServiceClient", lambda _: MagicMock())
    monkeypatch.setattr(
        onboarding, "prepare_vm", MagicMock(side_effect=TimeoutError("Guest timed out"))
    )
    result = CliRunner().invoke(setup.common, ["setup", "vm", "--skip-agents", "--yes"])
    assert result.exit_code == 1
    assert "Guest timed out" in result.output
    assert "ready" not in result.output


def test_credit_checkpoint_never_opens_checkout_for_unattended_setup(monkeypatch):
    import webbrowser

    import click

    from beta9.cli import setup

    opening = MagicMock()
    monkeypatch.setattr(webbrowser, "open", opening)
    monkeypatch.setattr(
        "beta9.mcp.tools.call_remote",
        lambda *_: {
            "credit": {"ok": False, "message": "Add credits at https://account.example/credits"}
        },
    )
    with pytest.raises(click.ClickException, match="https://account.example/credits"):
        setup.ensure_credit(object(), unattended=True)
    opening.assert_not_called()


def unfunded_setup(monkeypatch, checkout_status=201):
    """An interactive credit checkpoint whose account is funded on the second poll."""
    import webbrowser

    import requests

    from beta9.cli import setup

    opening = MagicMock()
    prompt = MagicMock(return_value="")
    checkout = MagicMock(
        return_value=MagicMock(
            status_code=checkout_status,
            json=lambda: {"url": "https://checkout.stripe.com/c/pay/cs_test"},
        )
    )
    monkeypatch.setattr(webbrowser, "open", opening)
    monkeypatch.setattr(setup.click, "prompt", prompt)
    monkeypatch.setattr(requests, "post", checkout)
    monkeypatch.setattr(setup.sys.stdin, "isatty", lambda: True)
    statuses = iter(
        [
            {
                "workspace_name": "014913",
                "credit": {
                    "ok": False,
                    "message": "This workspace has no credits yet. Add credits at "
                    "https://account.example/settings/credits to run workloads.",
                },
            },
            {"credit": {"ok": False}},
            {"credit": {"ok": True}},
        ]
    )
    monkeypatch.setattr("beta9.mcp.tools.call_remote", lambda *_: next(statuses))
    monkeypatch.setattr("time.sleep", lambda _: None)
    context = ConfigContext(token="workspace-token", auth_url="https://api.example/v2/oauth")
    return setup, context, opening, prompt, checkout


def test_enter_opens_a_twenty_dollar_checkout_for_the_signed_in_account(monkeypatch, capsys):
    setup, context, opening, prompt, checkout = unfunded_setup(monkeypatch)

    setup.ensure_credit(context, unattended=False)

    prompt.assert_called_once_with(
        "Press enter to add $20 in credits (Ctrl+C to exit)",
        default="",
        show_default=False,
        prompt_suffix=" ",
    )
    checkout.assert_called_once()
    assert checkout.call_args.args == ("https://api.example/v2/oauth/cli/checkout",)
    assert checkout.call_args.kwargs["json"] == {"total_cents_amount": 2000}
    assert checkout.call_args.kwargs["headers"] == {"Authorization": "Bearer workspace-token"}
    opening.assert_called_once_with("https://checkout.stripe.com/c/pay/cs_test", new=2)
    output = capsys.readouterr().out
    assert "This workspace has no credits yet." in output
    assert "Added credits" in output
    assert "014913" not in output
    assert ";" not in output


def test_checkout_falls_back_to_the_credits_page(monkeypatch):
    setup, context, opening, _, _ = unfunded_setup(monkeypatch, checkout_status=404)

    setup.ensure_credit(context, unattended=False)

    opening.assert_called_once_with("https://account.example/settings/credits", new=2)


@pytest.mark.parametrize(
    "message",
    [
        "this workspace has no credits yet; add credits at https://x/settings/credits to run workloads",
        "This workspace has no credits yet. Add credits at https://x/settings/credits to run workloads.",
    ],
)
def test_credit_reason_is_one_sentence_without_the_link(message):
    from beta9.cli import setup

    assert setup.credit_reason(message) == "This workspace has no credits yet."


def test_product_dashboard_templates_are_optional_and_context_bound(monkeypatch):
    from beta9.cli import setup
    from beta9.config import ConfigContext

    monkeypatch.setattr(
        setup,
        "get_settings",
        lambda: SimpleNamespace(
            api_url="https://app.prod.example",
            app_url_template="https://dash.prod.example/app/{app_id}",
            vm_url_template="",
        ),
    )
    context = ConfigContext(api_url="https://app.stage.example")
    assert setup.dashboard_link(context, "app", "id") == ""
    context.app_url_template = "https://dash.stage.example/workloads/{app_id}/logs"
    assert (
        setup.dashboard_link(context, "app", "a/b")
        == "https://dash.stage.example/workloads/a%2Fb/logs"
    )
    context.vm_url_template = "https://dash.stage.example/machines/{vm_id}"
    assert (
        setup.dashboard_link(context, "vm", "vm-id") == "https://dash.stage.example/machines/vm-id"
    )


def test_example_setup_refuses_unrelated_existing_app(tmp_path):
    selected = SimpleNamespace(http=SimpleNamespace(workspace_id="workspace"))

    def call(tool, args):
        return (
            {"items": [{"app_id": "someone-elses-app"}]}
            if tool == "list_apps"
            else {"config": {"env": []}}
        )

    with pytest.raises(ValueError, match="already exists"):
        onboarding.prepare_example(selected, tmp_path, "existing", "inference", call)


@pytest.mark.parametrize("relative", [False, True])
def test_example_handler_uses_upload_root_with_symlinked_state(tmp_path, monkeypatch, relative):
    import os
    from pathlib import Path

    from beta9.abstractions.endpoint import _CallableWrapper

    root = tmp_path / "real"
    root.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(root, target_is_directory=True)
    monkeypatch.chdir(tmp_path)
    state_directory = Path("alias") if relative else alias
    selected = SimpleNamespace(
        http=SimpleNamespace(workspace_id="workspace"),
        channel=MagicMock(),
        _config=ConfigContext(token="test-token"),
    )

    def deploy(wrapper, **kwargs):
        wrapper.parent._map_callable_to_attr(attr="handler", func=wrapper.func)
        assert wrapper.parent.handler == "app:hello"
        assert Path("app.py").is_file()
        return {}, True

    monkeypatch.setattr(_CallableWrapper, "deploy", deploy)
    response = MagicMock()
    response.json.return_value = {"message": "Hello from the cloud"}
    monkeypatch.setattr("requests.post", lambda *args, **kwargs: response)

    def call(tool, arguments):
        return {"items": []} if tool == "list_apps" else {"url": "https://test.example"}

    before = os.getcwd()
    result = onboarding.prepare_example(selected, state_directory, "hello", "inference", call)
    assert result["result"] == {"message": "Hello from the cloud"}
    assert Path(result["source_path"]).is_file()
    auth = Path(result["curl_config"])
    assert auth.parent == root / "requests"
    assert auth.stat().st_mode & 0o777 == 0o600
    assert "Bearer test-token" in auth.read_text()
    assert os.getcwd() == before


@pytest.mark.parametrize("exit_code", [0, 1])
def test_sandbox_sample_stops_container_after_verification(tmp_path, monkeypatch, exit_code):
    import os

    from beta9.abstractions import base

    instance = MagicMock(ok=True, container_id="sandbox-owned")
    process = instance.process.exec.return_value
    process.status.side_effect = [(-1, "running"), (exit_code, "exited")]
    process.stdout.read.side_effect = ["Hashing 3 records\n", "Hello from the cloud\n", ""]
    instance.terminate.return_value = True
    factory = MagicMock()
    factory.return_value.create.return_value = instance
    monkeypatch.setattr("beta9.abstractions.sandbox.Sandbox", factory)
    selected = SimpleNamespace(
        http=SimpleNamespace(workspace_id="workspace"),
        channel=MagicMock(),
        _config=ConfigContext(token="test-token"),
    )
    responses = iter([{"items": []}, {"items": [{"app_id": "owned-app"}]}])

    def call(*_):
        return next(responses)

    before = os.getcwd()
    channel = base._channel
    output = []
    if exit_code:
        with pytest.raises(RuntimeError, match="not verified"):
            onboarding.prepare_example(
                selected, tmp_path, "hello", "sandboxes", call, on_output=output.append
            )
    else:
        result = onboarding.prepare_example(
            selected, tmp_path, "hello", "sandboxes", call, on_output=output.append
        )
        assert result["container_id"] == "sandbox-owned"
        assert result["app_id"] == "owned-app"
        assert "Sandbox" in Path(result["source_path"]).read_text()
    assert output == ["Hashing 3 records\n", "Hello from the cloud\n"]
    process.wait.assert_not_called()
    instance.terminate.assert_called_once()
    assert factory.call_args.kwargs["keep_warm_seconds"] == 60
    assert os.getcwd() == before
    assert base._channel is channel


def test_sandbox_sample_refuses_existing_app_without_ownership(tmp_path, monkeypatch):
    factory = MagicMock()
    monkeypatch.setattr("beta9.abstractions.sandbox.Sandbox", factory)
    service = SimpleNamespace(http=SimpleNamespace(workspace_id="workspace"))
    with pytest.raises(ValueError, match="already exists"):
        onboarding.prepare_example(
            service,
            tmp_path,
            "existing",
            "sandboxes",
            lambda *_: {"items": [{"app_id": "someone-elses-app"}]},
        )
    factory.assert_not_called()


# Application kiosks


def guest_module(name):
    import beta9.abstractions.vm.kiosk_config as config

    path = str(Path(vm_module.__file__).parent / "assets" / name)
    loader = importlib.machinery.SourceFileLoader("beam_vm_" + name.replace("-", "_"), path)
    spec = importlib.util.spec_from_loader(loader.name, loader)
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {"kiosk_config": config}):
        loader.exec_module(module)
    return module


def test_openbox_policy_rejects_changed_source_without_partial_writes(tmp_path, monkeypatch):
    script = (Path(vm_module.__file__).parent / "assets/kiosk-openbox-build").read_text()
    marker = "python3 - \"$kiosk_build_root/source/openbox/client.c\" <<'PY'\n"
    patcher = script.split(marker, 1)[1].split("\nPY\n", 1)[0]
    source = "    /* then, based on our type, we can update our transientness.. */\n"
    target = tmp_path / "openbox" / "client.c"
    target.parent.mkdir()
    target.write_text(source)
    monkeypatch.setattr("sys.argv", ["patch", str(target)])
    with pytest.raises(SystemExit, match="review kiosk window policy"):
        exec(compile(patcher, "kiosk-openbox-build", "exec"), {})
    assert target.read_text() == source
    assert not (tmp_path / "kiosk.patch").exists()


def test_kiosk_configuration_is_frozen_and_builds_a_distinct_image():
    from beta9.abstractions.image import Image

    service = SimpleNamespace(channel=MagicMock(), gateway=MagicMock())
    config = Kiosk(
        ["editor", "literal; $(text)"],
        env={"APP_SETTING": "saved"},
        mcp={"app": MCPServer("python3", ["mcp.py"])},
    )
    vm = VM("app", kiosk=config, ssh=False, _service=service)
    config.command.append("changed")
    config.env["APP_SETTING"] = "changed"
    assert vm._spec["desktop"] is True
    assert vm._spec["kiosk"]["command"] == ["editor", "literal; $(text)"]
    assert vm._spec["kiosk"]["env"] == {"APP_SETTING": "saved"}
    assert vm._spec["kiosk"]["mcp"]["app"]["args"] == ["mcp.py"]
    base = Image(base_image="ubuntu:24.04")
    kiosk_image = prepare_image(base, service, True, kiosk=True)
    assert kiosk_image.ignore_python is True
    desktop_image = prepare_image(base, service, True)
    assert kiosk_image.build_steps[-1].command.endswith("install true true")
    assert desktop_image.build_steps[-1].command.endswith("install true")
    assert kiosk_image.build_steps != desktop_image.build_steps
    with pytest.raises(ValueError, match="display"):
        VM(kiosk=config, desktop=False, _service=service)


def test_kiosk_cli_preserves_argv_and_reports_bad_json(tmp_path, monkeypatch):
    service = SimpleNamespace(channel=MagicMock(), gateway=MagicMock(), http=MagicMock())
    monkeypatch.setattr("beta9.cli.extraclick.get_config_context", lambda *_: SimpleNamespace())
    client = MagicMock()
    client.__enter__.return_value = service
    monkeypatch.setattr("beta9.cli.extraclick.ServiceClient", lambda *_: client)
    fake = MagicMock(info={"name": "app", "status": "starting"})
    factory = MagicMock(return_value=fake)
    monkeypatch.setattr(vm_cli, "VM", factory)
    path = tmp_path / "app.json"
    config = {"command": ["editor", "project with spaces"], "env": {"A": "value"}}
    path.write_text(json.dumps(config))
    result = CliRunner().invoke(
        vm_cli.management, ["new", "app", "--kiosk-config", str(path), "--json"]
    )
    assert result.exit_code == 0, result.output
    assert factory.call_args.kwargs["kiosk"] == config
    fake.create.assert_called_once()
    fake.wait.assert_called_once_with(services=True)
    factory.reset_mock()
    path.write_text("[]")
    result = CliRunner().invoke(vm_cli.management, ["new", "app", "--kiosk-config", str(path)])
    assert result.exit_code != 0
    assert "JSON object" in result.output
    factory.assert_not_called()


def test_guest_processes_share_the_display_but_not_the_bootstrap_config():
    guest = guest_module("kiosk")
    session = guest.Session(
        {"command": ["editor"], "env": {"DISPLAY": ":99", "APP_SETTING": "custom"}}
    )
    assert session.env["DISPLAY"] == ":1"
    assert session.env["APP_SETTING"] == "custom"
    assert "BEAM_VM_KIOSK" not in session.env
    assert [session.retry_allowed("application") for _ in range(4)] == [True, True, True, False]
    assert session.retry_allowed("mcp")


def test_kiosk_sdk_waits_for_application_not_just_the_stream_port():
    vm = VM("app", kiosk=Kiosk(["editor"]), ssh=False, _service=SimpleNamespace())
    vm.info = {"spec": {"desktop": True, "kiosk": {"command": ["editor"]}}}
    sandbox = MagicMock()
    processes = []

    def command(*args):
        processes.append(args)
        return MagicMock(wait=MagicMock(return_value=1 if args[-1] == "--check" else 0))

    sandbox.process.exec.side_effect = command
    import time

    assert not vm._services_ready(sandbox, time.monotonic() + 30)
    assert ("/opt/beam-vm/kiosk", "--check") in processes


def test_retina_scales_widgets_and_fonts_together():
    module = guest_module("kiosk")
    retina = module.toolkit_settings('Xft/Antialias 1\nXft/DPI 196608\nXft/RGBA "rgb"\n')
    assert "Gdk/WindowScalingFactor 2\n" in retina
    assert "Gdk/UnscaledDPI 98304\n" in retina
    assert "Xft/DPI 196608\n" in retina
    assert 'Xft/RGBA "none"\n' in retina
    normal = module.toolkit_settings("Xft/DPI 98304\n")
    assert "Gdk/WindowScalingFactor 1\n" in normal
    assert "Gdk/UnscaledDPI 98304\n" in normal
    assert module.toolkit_settings(retina) == retina


def test_project_reopens_at_saved_browser_geometry(tmp_path):
    module = guest_module("kiosk")
    path = tmp_path / "display.json"
    assert module.saved_display(path) == (3840, 2160)
    path.write_text('{"width":3456,"height":1578}')
    assert module.saved_display(path) == (3456, 1578)
    for value in (
        "not JSON",
        '{"width":true,"height":900}',
        '{"width":99999,"height":900}',
        "null",
    ):
        path.write_text(value)
        assert module.saved_display(path) == (3840, 2160)


def test_existing_display_geometry_preserves_logical_size_when_density_changes(tmp_path):
    module = guest_module("kiosk")
    path = tmp_path / "display.json"
    path.write_text('{"width":1920,"height":1080,"pixel_ratio":1}')
    assert module.saved_display(path) == (3840, 2160)
    path.write_text('{"width":1920,"height":1080}')
    assert module.saved_display(path, legacy_scale=1) == (3840, 2160)
    path.write_text('{"width":3456,"height":1892,"pixel_ratio":2}')
    assert module.saved_display(path) == (3456, 1892)


def test_repeated_restarts_keep_only_current_owned_process_groups(monkeypatch, tmp_path):
    module = guest_module("kiosk")
    from dataclasses import replace

    config = tmp_path / "mcp.json"
    config.write_text('{"mcpServers":{}}')
    monkeypatch.setattr(module, "POLICY", replace(module.POLICY, mcp_config=str(config)))
    started = []
    stopped = []

    def start(*args, **kwargs):
        p = SimpleNamespace(pid=len(started) + 1)
        started.append(p)
        return p

    monkeypatch.setattr(module.subprocess, "Popen", start)
    monkeypatch.setattr(module, "stop_process", lambda p: stopped.append(p.pid) if p else None)
    session = module.Session({"command": ["editor"], "mcp": {"app": {"command": "adapter"}}})
    for _ in range(10):
        session.launch()
        session.launch_mcp()
    assert len(session.processes) == 2
    assert set(stopped) == set(range(1, 19))
    session.stop("mcp", "application")
    assert session.processes == {}
    assert set(stopped) == set(range(1, 21))


def test_launcher_handoff_uses_owned_windows_and_closes_children(monkeypatch):
    module = guest_module("kiosk")
    clock = [0]
    windows = []
    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(module, "visible_windows", lambda _: windows)
    monkeypatch.setattr(module.subprocess, "Popen", lambda *a, **kw: MagicMock(poll=lambda: 0))
    stop = MagicMock()
    monkeypatch.setattr(module, "stop_process", stop)
    monkeypatch.setattr(module.Session, "launch_mcp", lambda self: None)
    session = module.Session({"command": ["launcher"]})
    session.launch()
    session.poll_application()
    assert session.state.application == "starting"
    windows.append("0x123")
    session.poll_application()
    assert session.state.application == "ready"
    windows.clear()
    session.poll_application()
    assert session.state.application == "closed"
    assert session.processes == {}
    # The launcher's process group still owns its children after it exits.
    assert stop.call_args.args[0].poll() == 0


@pytest.mark.parametrize("exit_code", [None, 0])
def test_transient_xprop_timeout_recovers_without_restarting_application(monkeypatch, exit_code):
    module = guest_module("kiosk")
    start = MagicMock(return_value=MagicMock(poll=lambda: exit_code))
    monkeypatch.setattr(module.subprocess, "Popen", start)
    monkeypatch.setattr(module.Session, "launch_mcp", MagicMock())
    monkeypatch.setattr(
        module.subprocess,
        "run",
        MagicMock(
            side_effect=[
                subprocess.TimeoutExpired(["xprop", "-root", "_NET_CLIENT_LIST"], 2),
                SimpleNamespace(returncode=0, stdout="_NET_CLIENT_LIST(WINDOW): window id # 0x123"),
                subprocess.TimeoutExpired(["xprop", "-root", "_NET_CLIENT_LIST"], 2),
                SimpleNamespace(returncode=0, stdout="_NET_CLIENT_LIST(WINDOW): window id # 0x123"),
            ]
        ),
    )
    session = module.Session({"command": ["editor"]})
    session.launch()
    session.poll_application()
    assert session.state.application == "starting"
    session.poll_application()
    assert session.state.application == "ready"
    assert session.state.windows == 1
    session.poll_application()
    assert session.state.application == "ready"
    assert session.state.windows == 1
    session.poll_application()
    assert session.state.application == "ready"
    start.assert_called_once()


@pytest.mark.parametrize("exit_code", [None, 2])
def test_unknown_window_probe_preserves_crash_and_startup_deadlines(monkeypatch, exit_code):
    module = guest_module("kiosk")
    clock = [0]
    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])
    start = MagicMock(return_value=MagicMock(poll=lambda: exit_code))
    monkeypatch.setattr(module.subprocess, "Popen", start)
    monkeypatch.setattr(module, "stop_process", MagicMock())
    monkeypatch.setattr(
        module.subprocess,
        "run",
        MagicMock(side_effect=subprocess.TimeoutExpired(["xprop"], 2)),
    )
    session = module.Session({"command": ["editor"]})
    session.launch()
    clock[0] = module.POLICY.application_timeout + 1
    session.poll_application()
    if exit_code is None:
        assert session.state.application == "error"
        assert "has not opened a window" in session.state.error
        assert session.processes == {}
    else:
        assert session.state.application == "starting"
        assert start.call_count == 2


def test_application_start_timeout_and_mcp_start_have_independent_deadlines(monkeypatch, tmp_path):
    module = guest_module("kiosk")
    clock = [0]
    windows = []
    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(module, "visible_windows", lambda _: windows)
    monkeypatch.setattr(module.subprocess, "Popen", lambda *a, **kw: MagicMock(poll=lambda: None))
    monkeypatch.setattr(module, "stop_process", MagicMock())
    monkeypatch.setattr(module.Session, "mcp_ready", lambda *a: False)
    from dataclasses import replace

    config = tmp_path / "mcp.json"
    config.write_text('{"mcpServers":{}}')
    monkeypatch.setattr(module, "POLICY", replace(module.POLICY, mcp_config=str(config)))
    session = module.Session({"command": ["editor"], "mcp": {"app": {"command": "adapter"}}})
    session.launch()
    clock[0] = module.POLICY.application_timeout - 1
    windows.append("0x123")
    session.poll_application()
    clock[0] += 2
    session.poll_mcp()
    assert session.state.mcp == "starting", "MCP initialization gets its own full deadline"
    clock[0] += module.POLICY.application_timeout
    session.poll_mcp()
    assert session.state.mcp == "error"
    assert "mcp" not in session.processes
    monkeypatch.setattr(module.subprocess, "Popen", MagicMock(side_effect=FileNotFoundError))
    session.launch_mcp()
    assert session.state.application == "ready", "a missing optional adapter must not close the app"
    assert session.state.mcp == "error"
    starts = module.subprocess.Popen.call_count
    session.poll_mcp()
    assert module.subprocess.Popen.call_count == starts, "failed startup waits for config repair"
    monkeypatch.setattr(module.subprocess, "Popen", lambda *a, **kw: MagicMock(poll=lambda: None))
    session.launch()
    windows.clear()
    clock[0] += module.POLICY.application_timeout + 1
    session.poll_application()
    assert session.state.application == "error"
    assert session.processes == {}


def test_guest_resize_keeps_physical_dpi_and_rejects_invalid_geometry(monkeypatch):
    module = guest_module("kiosk")
    commands = []
    monkeypatch.setattr(module.subprocess, "run", lambda argv, **kwargs: commands.append(argv))
    monkeypatch.setattr(module.sys, "argv", ["kiosk", "--resize", "2880", "1800"])
    assert module.main() == 0
    assert commands == [
        ["selkies", "resize", "2880x1800"],
        ["xrandr", "--fb", "2880x1800", "--dpi", "192"],
    ]
    commands.clear()
    for args in (
        ["kiosk", "--resize", "1", "900"],
        ["kiosk", "--resize", "900"],
        ["kiosk", "--resize", "x", "900"],
    ):
        monkeypatch.setattr(module.sys, "argv", args)
        assert module.main() == 1
    assert commands == []


def test_transient_settings_removal_keeps_display_live_and_accepts_replacement(
    tmp_path, monkeypatch
):
    module = guest_module("kiosk")
    session = module.Session({"command": ["editor"]})
    session.settings_file = tmp_path / ".xsettingsd"
    session.settings_source = module.toolkit_settings("Xft/DPI 196608\n")
    session.toolkit_file = tmp_path / "toolkit.conf"
    session.toolkit_file.write_text(session.settings_source)
    session.display_file = tmp_path / "display.json"
    session.saved_size = (2880, 1800)
    session.geometry_check = 0
    session.processes["settings"] = SimpleNamespace(pid=123)
    monkeypatch.setattr(module, "display_size", lambda _: (2048, 1536))
    reload_settings = MagicMock()
    monkeypatch.setattr(module.os, "kill", reload_settings)
    reconfigure = MagicMock()
    monkeypatch.setattr(module.subprocess, "run", reconfigure)
    session.sync_display()
    assert session.saved_size == (2048, 1536), "geometry persistence continues during replacement"
    assert session.toolkit_file.read_text() == session.settings_source
    reload_settings.assert_not_called()
    session.settings_file.write_text("Xft/DPI 98304\n")
    session.sync_display()
    assert "Gdk/WindowScalingFactor 1\n" in session.toolkit_file.read_text()
    reload_settings.assert_called_once_with(123, module.signal.SIGHUP)
    reconfigure.assert_called_once()


def test_pinned_backend_pointer_fix_preserves_clicks_and_real_drags(tmp_path):
    patcher = runpy.run_path(str(Path(vm_module.__file__).parent / "assets/kiosk-selkies-patch"))
    # Compact excerpt of Selkies 2.1.0's X11 dispatch. Test the generated event
    # stream without importing a Linux X11 backend into the SDK test process.
    original = textwrap.dedent("""\
        class Pointer:
            def __init__(self):
                self.last_x = self.last_y = -1
                self.button_mask = 0
                self.events = []
            def send_mouse(self, action, value):
                self.events.append((action, value))
            def dispatch(self, x, y, button_mask):
                relative = False
                position_changed = x != self.last_x or y != self.last_y
                final_x, final_y = self.last_x, self.last_y = x, y
                if relative:
                    pass
                elif position_changed or button_mask != self.button_mask:
                    # Button transitions warp unconditionally: an application may have
                    # moved the pointer, and a press must land where the client aims.
                    self.send_mouse(MOUSE_POSITION, (final_x, final_y))
                for bit in (1, 2, 4):
                    if (button_mask ^ self.button_mask) & bit:
                        self.send_mouse("press" if button_mask & bit else "release", bit)
                self.button_mask = button_mask
        """)
    path = tmp_path / "input_handler.py"
    patches = patcher["PATCHES"][path.name]
    path.write_text(original)
    patcher["patch_file"](path, patches)
    patched = path.read_text()
    patcher["patch_file"](path, patches)
    assert path.read_text() == patched, "repeated image preparation must be idempotent"
    namespace = {"MOUSE_POSITION": "move"}
    exec(compile(patched, str(path), "exec"), namespace)
    for button in (1, 2, 4):
        pointer = namespace["Pointer"]()
        pointer.dispatch(50, 60, 0)
        pointer.events.clear()
        pointer.dispatch(50, 60, button)
        pointer.dispatch(50, 60, 0)
        assert pointer.events == [("move", (50, 60)), ("press", button), ("release", button)]
        pointer.events.clear()
        pointer.dispatch(50, 60, button)
        pointer.dispatch(80, 90, button)
        pointer.dispatch(100, 110, 0)
        assert pointer.events == [
            ("move", (50, 60)),
            ("press", button),
            ("move", (80, 90)),
            ("move", (100, 110)),
            ("release", button),
        ]
    path.write_text(original.replace("button_mask != self.button_mask", "upstream_changed"))
    with pytest.raises(SystemExit, match="Selkies source changed"):
        patcher["patch_file"](path, patches)


def test_focused_viewport_requires_controller_authority(tmp_path):
    patcher = runpy.run_path(str(Path(vm_module.__file__).parent / "assets/kiosk-selkies-patch"))
    path = tmp_path / "webrtc_mode.py"
    path.write_text(
        textwrap.dedent("""\
        class Session:
            def message(self, msg, display_id="primary", conn_id=None):
                if msg == "REQUEST_KEYFRAME" and self.rtc_app is not None:
                    return "keyframe"
                return "ordinary input"
        """)
    )
    patches = {k: v for k, v in patcher["PATCHES"][path.name].items() if "viewport" in k}
    patcher["patch_file"](path, patches)
    patched = path.read_text()
    patcher["patch_file"](path, patches)
    assert path.read_text() == patched
    namespace = {"parse_resize_dims": lambda size: (3456, 1692) if size == "3456x1692" else None}
    exec(compile(patched, str(path), "exec"), namespace)
    session = namespace["Session"]()
    controller = {}
    session.rtc_app = SimpleNamespace(peer_connections={"allowed": controller, "viewer": object()})
    session._peer_tabs = {"allowed": "wide", "viewer": "read-only"}
    session._display_owner_tabs = {"primary": "narrow"}
    session._holds_full_control = lambda peer: peer is controller
    session.input_handler = MagicMock()
    for connection, size in [("viewer", "3456x1692"), ("unknown", "3456x1692"), ("allowed", "bad")]:
        session.message("KIOSK_VIEWPORT," + size, conn_id=connection)
        assert session._display_owner_tabs == {"primary": "narrow"}
        session.input_handler.on_message.assert_not_called()
    session.message("KIOSK_VIEWPORT,3456x1692", conn_id="allowed")
    assert session._display_owner_tabs == {"primary": "wide"}
    assert controller["held_owner_messages"]["r"] == "r,3456x1692"
    session.input_handler.on_message.assert_called_once_with(
        "r,3456x1692", "primary", conn_id="allowed"
    )
    assert session.message("ordinary") == "ordinary input"


def test_backend_patches_validate_a_whole_file_before_writing(tmp_path):
    patcher = runpy.run_path(str(Path(vm_module.__file__).parent / "assets/kiosk-selkies-patch"))
    path = tmp_path / "backend.py"
    original = "first = 1\nsecond = 2\n"
    path.write_text(original)
    patches = {
        "# first": {"first = 1": "first = 3  # first"},
        "# second": {"upstream changed": "second = 4  # second"},
    }
    with pytest.raises(SystemExit, match="Selkies source changed"):
        patcher["patch_file"](path, patches)
    assert path.read_text() == original
    patches["# second"] = {"second = 2": "second = 4  # second"}
    patcher["patch_file"](path, patches)
    patched = path.read_text()
    patcher["patch_file"](path, patches)
    assert path.read_text() == patched == "first = 3  # first\nsecond = 4  # second\n"


def test_client_bootstrap_rejects_changed_html_without_writing(tmp_path):
    patcher = runpy.run_path(str(Path(vm_module.__file__).parent / "assets/kiosk-selkies-patch"))
    page = tmp_path / "selkies_web/index.html"
    page.parent.mkdir()
    page.write_text("<html><body></body></html>")
    with pytest.raises(SystemExit, match="Selkies web client changed"):
        patcher["patch_client"](tmp_path, 2)
    assert page.read_text() == "<html><body></body></html>"


# Application MCPs


@pytest.fixture
def backend(tmp_path):
    pytest.importorskip("fastmcp")
    script = tmp_path / "server.py"
    script.write_text(
        textwrap.dedent("""\
        import asyncio, os
        from fastmcp import FastMCP
        server = FastMCP("Ordinary application server")
        count = 0
        busy = False
        @server.tool()
        async def edit():
            global count, busy
            overlap = busy
            busy = True
            await asyncio.sleep(0.02)
            count += 1
            busy = False
            return {"count": count, "pid": os.getpid(), "cwd": os.getcwd(), "env": os.getenv("PROJECT"), "overlap": overlap}
        @server.tool()
        def extend():
            @server.tool()
            def added(): return "new capability"
        @server.tool()
        def crash(): os._exit(1)
        @server.resource("project://current")
        def project(): return "shared project"
        @server.prompt()
        def edit_project(): return "Edit the current project"
        server.run(show_banner=False)
    """)
    )
    return MCPServer(sys.executable, [str(script)], env={"PROJECT": "a project"}, cwd=str(tmp_path))


def test_registry_defaults_overrides_removal_and_atomic_validation(tmp_path, backend, capsys):
    defaults = tmp_path / "defaults.json"
    defaults.write_text(json.dumps({"original": backend.to_dict()}))
    registry = guest_module("kiosk-mcp").Registry(tmp_path / "mcp.json", defaults)
    registry.update("second", backend.to_dict())
    registry.update("original", None)
    assert set(registry.read()) == {"second"}
    assert registry.path.stat().st_mode & 0o777 == 0o600
    before = registry.path.read_bytes()
    for name, config in [
        ("invalid/name", backend.to_dict()),
        ("bad", {}),
        ("bad", {"command": "node", "url": "http://localhost/mcp"}),
    ]:
        with pytest.raises(ValueError):
            registry.update(name, config)
        assert registry.path.read_bytes() == before
    # New image defaults apply without losing runtime installs or re-enabling removals.
    defaults.write_text(json.dumps({"original": backend.to_dict(), "new": backend.to_dict()}))
    assert set(registry.read()) == {"second", "new"}
    registry.update("original", backend.to_dict())
    assert set(registry.read()) == {"original", "second", "new"}
    registry.update("private", {"command": "node", "args": ["--token", "argument-secret"]})
    registry.update("http", {"url": "http://localhost/mcp?token=query-secret"})
    module = guest_module("kiosk-mcp")
    with patch.object(module, "Registry", lambda: registry), patch.object(
        sys, "argv", ["kiosk-mcp", "--list"]
    ):
        module.main()
    output = capsys.readouterr().out
    assert "argument-secret" not in output and "query-secret" not in output
    assert json.loads(output)["http"]["url"] == "[redacted]"
    assert registry.read()["private"]["args"][-1] == "argument-secret"
    for action in ["--install", "--remove"]:
        with patch.object(module, "Registry", lambda: registry), patch.object(
            sys, "argv", ["kiosk-mcp", action, "cli"]
        ), patch.object(sys, "stdin", io.StringIO(json.dumps(backend.to_dict()))):
            module.main()
        assert json.loads(capsys.readouterr().out) is None
        assert ("cli" in registry.read()) == (action == "--install")


def test_health_distinguishes_gateway_availability_from_adapter_readiness():
    pytest.importorskip("fastmcp")
    import httpx2 as httpx

    module = guest_module("kiosk-mcp")
    gateway = module.Gateway({}, "/tmp")

    async def exercise():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=gateway.server.http_app()), base_url="http://guest"
        ) as client:
            for states, expected in [
                ([], "ready"),
                (["starting"], "starting"),
                (["ready"], "ready"),
                (["error"], "error"),
                (["error", "ready"], "degraded"),
            ]:
                gateway.adapters = {
                    str(i): module.Adapter(str(i), MagicMock(), status=module.AdapterStatus(state))
                    for i, state in enumerate(states)
                }
                response = await client.get("/health")
                assert response.status_code == 200
                assert response.json() == {
                    "gateway": "ready",
                    "state": expected,
                    "servers": {
                        str(i): {"state": state, "tools": 0, "error": ""}
                        for i, state in enumerate(states)
                    },
                }

    asyncio.run(exercise())


def test_proxy_preserves_state_and_serializes_clients_across_namespaced_features(backend):
    from fastmcp import Client

    gateway = guest_module("kiosk-mcp").Gateway(
        {
            "app": backend.to_dict(),
            "other": backend.to_dict(),
            "broken": {"command": "/missing/mcp"},
        },
        "/tmp",
    )

    async def exercise():
        async with Client(gateway.server, mode="legacy") as first, Client(
            gateway.server, mode="legacy"
        ) as second:
            assert {tool.name for tool in await first.list_tools()} >= {"app_edit", "other_edit"}
            edits = await asyncio.gather(first.call_tool("app_edit"), second.call_tool("app_edit"))
            data = [result.data for result in edits]
            assert sorted(item["count"] for item in data) == [1, 2]
            assert len({item["pid"] for item in data}) == 1
            assert all(
                item["env"] == "a project" and item["cwd"] == backend.cwd and not item["overlap"]
                for item in data
            )
            other = (await first.call_tool("other_edit")).data
            assert other["count"] == 1 and other["pid"] != data[0]["pid"]
            assert "shared project" in (await first.read_resource("project://app/current"))[0].text
            assert (await first.get_prompt("app_edit_project")).messages
            await first.call_tool("app_extend")
            assert (await second.call_tool("app_added")).content[0].text == "new capability"
            return [item["pid"] for item in data] + [other["pid"]]

    pids = asyncio.run(exercise())
    for pid in pids:
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)


def test_proxy_recovers_a_crashed_adapter_without_restarting_the_application(backend):
    from fastmcp import Client

    gateway = guest_module("kiosk-mcp").Gateway({"app": backend.to_dict()}, "/tmp")

    async def exercise():
        async with Client(gateway.server, mode="legacy") as client:
            before = (await client.call_tool("app_edit")).data
            with pytest.raises(Exception):
                await client.call_tool("app_crash")
            after = (await client.call_tool("app_edit")).data
            assert after["count"] == 1
            assert after["pid"] != before["pid"]

    asyncio.run(exercise())


def test_sdk_uses_existing_vm_transport_and_standard_client_configuration():
    process = MagicMock()
    process.wait.return_value = 0
    process.stdout.read.return_value = "null"
    vm = MagicMock(mcp_url="https://application.example/mcp", traffic_access_token="private")
    vm.refresh.return_value.info = {"spec": {"kiosk": {}}}
    vm.process.exec.return_value = process
    vm._guest_json = VM._guest_json.__get__(vm)
    vm._guest_output = VM._guest_output
    mcp = VMMCP(vm)
    config = MCPServer("node", ["/opt/my-app/server.js"], env={"TOKEN": "secret"})
    assert mcp.install("app", config) is None
    args, kwargs = vm.process.exec.call_args
    assert args == ("/opt/beam-vm/kiosk-mcp", "--install", "app")
    assert json.loads(kwargs["stdin"]) == config.to_dict()
    assert mcp.client_config() == {
        "url": vm.mcp_url,
        "headers": {"X-Beam-VM-Token": "private"},
    }
    process.stdout.read.return_value = '{"state":"ready","servers":{}}'
    assert mcp.status()["state"] == "ready"
    mcp.remove("app")
    assert vm.process.exec.call_args.args[-2:] == ("--remove", "app")
    vm.refresh.return_value.info = {"spec": {}}
    with pytest.raises(ValueError, match="kiosk"):
        mcp.list()
    vm.process.exec.reset_mock()
    for invalid in [{"command": "node"}, "node", None]:
        with pytest.raises(TypeError, match="MCPServer"):
            mcp.install("app", invalid)
    vm.process.exec.assert_not_called()


def test_image_configuration_round_trips_both_standard_transports():
    kiosk = Kiosk(
        ["application"],
        mcp={
            "stdio": MCPServer("node", ["server.js"]),
            "http": MCPServer(
                url="http://127.0.0.1:9000/mcp", headers={"Authorization": "Bearer app-secret"}
            ),
        },
    )
    assert Kiosk.from_dict(kiosk.to_dict()) == kiosk


@pytest.mark.parametrize("application", ["kicad", "blender"])
def test_application_recipes_run_outside_their_build_directory(application, tmp_path, monkeypatch):
    from beta9 import Image

    recipe = (
        Path(vm_module.__file__).resolve().parents[4] / "examples/kiosk" / application / "app.py"
    )
    monkeypatch.chdir(tmp_path)
    with patch("beta9.VM") as create, patch.object(
        Image, "from_dockerfile", wraps=Image.from_dockerfile
    ) as image:
        runpy.run_path(str(recipe), run_name="__main__")
    assert Path(image.call_args.args[0]) == recipe.with_name("Dockerfile")
    config = create.call_args.kwargs["kiosk"]
    assert isinstance(config.mcp[application], MCPServer)
    assert config.transport == "webrtc"
    assert config.command[-1].startswith("/workspace/")
    create.return_value.create.assert_called_once_with()


def test_http_backend_keeps_its_credentials_separate_from_client_headers():
    pytest.importorskip("fastmcp")
    from contextlib import asynccontextmanager
    import socket
    import uvicorn
    from fastmcp import Client, FastMCP
    from fastmcp.client.transports import StreamableHttpTransport
    from fastmcp.server.dependencies import get_http_headers

    @asynccontextmanager
    async def serve(server):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            http = uvicorn.Server(uvicorn.Config(server.http_app(), log_level="error"))
            task = asyncio.create_task(http.serve(sockets=[listener]))
            try:
                for _ in range(100):
                    if http.started:
                        break
                    if task.done():
                        await task
                    await asyncio.sleep(0.01)
                assert http.started
                yield f"http://127.0.0.1:{listener.getsockname()[1]}/mcp"
            finally:
                http.should_exit = True
                await asyncio.wait_for(task, 5)

    backend = FastMCP("Application HTTP server")

    @backend.tool()
    def headers() -> dict:
        request = get_http_headers(include={"authorization"})
        return {
            "credential": request.get("authorization"),
            "client_header": request.get("x-client-secret"),
        }

    async def exercise():
        async with serve(backend) as url:
            gateway = guest_module("kiosk-mcp").Gateway(
                {"app": {"url": url, "headers": {"Authorization": "Bearer app-credential"}}}, "/tmp"
            )
            async with serve(gateway.server) as proxy:
                transport = StreamableHttpTransport(
                    proxy,
                    headers={
                        "Authorization": "Bearer viewer-credential",
                        "X-Client-Secret": "private",
                    },
                )
                async with Client(transport, mode="legacy") as client:
                    result = await client.call_tool("app_headers")
                    assert result.data == {
                        "credential": "Bearer app-credential",
                        "client_header": None,
                    }

    asyncio.run(exercise())


# Local VM MCP tools


def test_build_error_after_unterminated_progress_is_reported():
    from beta9.mcp.tools import DeployJob

    job = DeployJob(id="test", name="image", directory=".", command=[], operation="command")
    job.lines = [
        "Reading package lists...{",
        '  "error": "Build connection interrupted",',
        '  "code": "ERROR"',
        "}",
    ]
    from beta9.mcp.tools import _json_objects

    errors, _ = _json_objects(job.lines)
    assert job._failure(errors, 1) == "Build connection interrupted"


def local_mcp_tools(tmp_path):
    return SimpleNamespace(
        cwd=str(tmp_path),
        context_name="staging",
        start_command=MagicMock(return_value={"job_id": "job"}),
    )


def test_vm_local_tools_preserve_context_argv_and_confirmation(tmp_path, monkeypatch):
    monkeypatch.setattr("beta9.mcp.vm._cli_command", lambda: ["beam"])
    tools = local_mcp_tools(tmp_path)
    catalog = {d["name"]: (d, h) for d, h in definitions(tools)}
    for definition, _ in catalog.values():
        Draft202012Validator.check_schema(definition["inputSchema"])
    definition, handler = catalog["vm_ssh"]
    args = {
        "name": "vm",
        "command": ["printf", "two words; $(literal)"],
        "request_key": "same",
        "confirm": False,
    }
    assert handler(args)["isError"]
    tools.start_command.assert_not_called()
    args["confirm"] = True
    assert handler(args)["job_id"] == "job"
    assert tools.start_command.call_args.args[2] == [
        "beam",
        "--context",
        "staging",
        "vm",
        "ssh",
        "vm",
        "--",
        "printf",
        "two words; $(literal)",
    ]
    assert tools.start_command.call_args.kwargs["operation"] == "command"


def test_image_build_cannot_upload_home_or_escape_context(tmp_path):
    tools = local_mcp_tools(tmp_path)
    handler = next(h for d, h in definitions(tools) if d["name"] == "vm_build_image")
    for directory, dockerfile in [
        (str(Path.home()), "Dockerfile"),
        (str(tmp_path), "../Dockerfile"),
    ]:
        assert handler(
            {
                "directory": directory,
                "dockerfile": dockerfile,
                "confirm": True,
                "request_key": "key",
            }
        )["isError"]
    tools.start_command.assert_not_called()
