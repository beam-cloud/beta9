import importlib.machinery
import importlib.util
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import threading
import shutil
import subprocess
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from beta9 import Kiosk, KioskMCP
from beta9.abstractions.vm import VM, prepare_image
from beta9.abstractions.vm import __file__ as vm_file
from beta9.cli import vm as cli


def supervisor():
    path = str(Path(vm_file).with_name("kiosk"))
    loader = importlib.machinery.SourceFileLoader("beam_kiosk_guest", path)
    spec = importlib.util.spec_from_loader(loader.name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


def test_kiosk_configuration_is_frozen_and_builds_a_distinct_image():
    from beta9.abstractions.image import Image

    service = SimpleNamespace(channel=MagicMock(), gateway=MagicMock())
    config = Kiosk(
        ["editor", "literal; $(text)"],
        env={"APP_SETTING": "saved"},
        mcp=KioskMCP(["python3", "mcp.py"], 8000),
    )
    vm = VM("app", kiosk=config, ssh=False, _service=service)
    config.command.append("changed")
    config.env["APP_SETTING"] = "changed"
    assert vm._spec["desktop"] is True
    assert vm._spec["kiosk"]["command"] == ["editor", "literal; $(text)"]
    assert vm._spec["kiosk"]["env"] == {"APP_SETTING": "saved"}
    assert vm._spec["kiosk"]["mcp"]["path"] == "/mcp"
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
    monkeypatch.setattr(cli, "VM", factory)
    path = tmp_path / "app.json"
    config = {"command": ["editor", "project with spaces"], "env": {"A": "value"}}
    path.write_text(json.dumps(config))
    result = CliRunner().invoke(
        cli.management, ["new", "app", "--kiosk-config", str(path), "--json"]
    )
    assert result.exit_code == 0, result.output
    assert factory.call_args.kwargs["kiosk"] == config
    fake.create.assert_called_once()
    factory.reset_mock()
    path.write_text("[]")
    result = CliRunner().invoke(cli.management, ["new", "app", "--kiosk-config", str(path)])
    assert result.exit_code != 0
    assert "JSON object" in result.output
    factory.assert_not_called()


@pytest.mark.parametrize("sse", [False, True, "multiline"])
def test_mcp_readiness_performs_real_initialization_and_closes_probe_session(sse):
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            value = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            calls.append((value["method"], self.headers.get("Mcp-Session-Id")))
            if value["method"] != "initialize":
                self.send_response(202)
                self.end_headers()
                return
            message = {
                "jsonrpc": "2.0",
                "id": 1,
                "result": {"protocolVersion": "2025-06-18", "capabilities": {}},
            }
            body = json.dumps(message).encode()
            if sse:
                if sse == "multiline":
                    body = b"\n".join(
                        b"data: " + line
                        for line in json.dumps(message, indent=2).encode().splitlines()
                    )
                else:
                    body = b"data: " + body
                body = b": keepalive\n\nevent: message\n" + body + b"\n\n"
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream" if sse else "application/json")
            self.send_header("Mcp-Session-Id", "probe-session")
            self.end_headers()
            self.wfile.write(body)

        def do_DELETE(self):
            calls.append(("DELETE", self.headers.get("Mcp-Session-Id")))
            self.send_response(200)
            self.end_headers()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        assert supervisor().mcp_ready(server.server_port, "/mcp")
        assert calls == [
            ("initialize", None),
            ("notifications/initialized", "probe-session"),
            ("DELETE", "probe-session"),
        ]
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_guest_processes_share_the_display_but_not_the_bootstrap_config():
    guest = supervisor()
    session = guest.Session(
        {"command": ["editor"], "env": {"DISPLAY": ":99", "APP_SETTING": "custom"}}
    )
    assert session.env["DISPLAY"] == ":1"
    assert session.env["APP_SETTING"] == "custom"
    assert "BEAM_VM_KIOSK" not in session.env
    assert [session.retry_allowed(session.failures) for _ in range(4)] == [True, True, True, False]


def test_kiosk_sdk_waits_for_application_and_mcp_not_just_the_stream_port():
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


def test_kiosk_browser_readiness_routing_recovery_and_message_origins():
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node is required for the embedded browser client tests")
    subprocess.run(
        [
            node,
            str(Path(__file__).with_suffix(".js")),
            str(Path(__file__).parents[2] / "pkg/abstractions/vm/kiosk.html"),
        ],
        check=True,
        timeout=15,
    )


def test_webrtc_browser_registration_and_private_candidate_filtering():
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node is required for the embedded browser client tests")
    subprocess.run(
        [
            node,
            str(Path(__file__).with_name("test_vm_webrtc.js")),
            str(Path(__file__).parents[2] / "pkg/abstractions/vm/kiosk-webrtc.js"),
        ],
        check=True,
        timeout=15,
    )


def test_startup_dialogs_do_not_become_the_primary_window(monkeypatch):
    module = supervisor()
    replies = {
        "0x1": "_NET_WM_WINDOW_TYPE(ATOM) = _NET_WM_WINDOW_TYPE_DIALOG\nWM_TRANSIENT_FOR: not found.",
        "0x2": "_NET_WM_WINDOW_TYPE(ATOM) = _NET_WM_WINDOW_TYPE_NORMAL\nWM_TRANSIENT_FOR(WINDOW): window id # 0x4",
        "0x3": "_NET_WM_WINDOW_TYPE(ATOM) = _NET_WM_WINDOW_TYPE_SPLASH\nWM_TRANSIENT_FOR: not found.",
        "0x4": "_NET_WM_WINDOW_TYPE(ATOM) = _NET_WM_WINDOW_TYPE_NORMAL\nWM_TRANSIENT_FOR: not found.",
    }
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda args, **_: SimpleNamespace(returncode=0, stdout=replies[args[2]]),
    )
    assert module.primary_window(["0x1", "0x2", "0x3"], {}) is None
    assert module.primary_window(list(replies), {}) == "0x4"


def test_retina_scales_widgets_and_fonts_together():
    module = supervisor()
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
    module = supervisor()
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


def test_secondary_editors_fill_the_session_and_dialogs_remain_floating(monkeypatch):
    module = supervisor()
    session = module.Session({"command": ["editor"]})
    kinds = {"main": True, "second": True, "modal": False, "utility": False}
    monkeypatch.setattr(
        module, "primary_window", lambda windows, env: windows[0] if kinds[windows[0]] else None
    )
    fitted = []
    monkeypatch.setattr(module, "undecorate", lambda window, env: fitted.append(window))
    commands = []
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda argv, **kw: (commands.append(argv) or SimpleNamespace(returncode=0, stdout="")),
    )
    session.fit_application_windows(["main", "modal", "utility"])
    session.fit_application_windows(["main", "second", "modal", "utility"])
    assert fitted == []
    assert [cmd[2] for cmd in commands if cmd[0] == "wmctrl"] == ["main", "second"]
    session.fit_application_windows(["second", "modal"])
    assert session.primary == "second"
    assert fitted == []


def test_a_vanishing_window_does_not_interrupt_healthy_editors(monkeypatch):
    module = supervisor()
    session = module.Session({"command": ["editor"]})
    monkeypatch.setattr(module, "primary_window", lambda windows, env: windows[0])

    def fit(argv, **kwargs):
        if argv[2] == "gone":
            raise module.subprocess.CalledProcessError(1, argv)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(module.subprocess, "run", fit)
    session.fit_application_windows(["gone", "live"])
    assert session.managed_windows == {"live"}
    assert session.primary == "live"


def test_existing_display_geometry_preserves_logical_size_when_density_changes(tmp_path):
    module = supervisor()
    path = tmp_path / "display.json"
    path.write_text('{"width":1920,"height":1080,"pixel_ratio":1}')
    assert module.saved_display(path) == (3840, 2160)
    path.write_text('{"width":1920,"height":1080}')
    assert module.saved_display(path, legacy_scale=1) == (3840, 2160)
    path.write_text('{"width":3456,"height":1892,"pixel_ratio":2}')
    assert module.saved_display(path) == (3456, 1892)
