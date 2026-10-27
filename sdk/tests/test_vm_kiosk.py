import importlib.machinery
import importlib.util
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import threading
import shutil
import subprocess
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from beta9 import Kiosk, KioskMCP
from beta9.abstractions.vm import VM, prepare_image
from beta9.abstractions.vm import __file__ as vm_file
from beta9.cli import vm as cli


def supervisor():
    path = str(Path(vm_file).parent / "assets/kiosk")
    loader = importlib.machinery.SourceFileLoader("beam_kiosk_guest", path)
    spec = importlib.util.spec_from_loader(loader.name, loader)
    module = importlib.util.module_from_spec(spec)
    # The standalone guest imports this adjacent shipped module without the SDK.
    import beta9.abstractions.vm.kiosk_config as config

    with patch.dict("sys.modules", {"kiosk_config": config}):
        loader.exec_module(module)
    return module


def test_openbox_policy_rejects_changed_source_without_partial_writes(tmp_path, monkeypatch):
    script = (Path(vm_file).parent / "assets/kiosk-openbox-build").read_text()
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
    assert [session.retry_allowed("application") for _ in range(4)] == [True, True, True, False]
    assert session.retry_allowed("mcp")


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


@pytest.mark.parametrize("script", ["test_vm_webrtc.js", "test_vm_kiosk_input.js"])
def test_webrtc_browser_transport_and_input(script):
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node is required for the embedded browser client tests")
    subprocess.run(
        [
            node,
            str(Path(__file__).with_name(script)),
            str(Path(__file__).parents[2] / "pkg/abstractions/vm/kiosk-webrtc.js"),
        ],
        check=True,
        timeout=15,
    )


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


def test_existing_display_geometry_preserves_logical_size_when_density_changes(tmp_path):
    module = supervisor()
    path = tmp_path / "display.json"
    path.write_text('{"width":1920,"height":1080,"pixel_ratio":1}')
    assert module.saved_display(path) == (3840, 2160)
    path.write_text('{"width":1920,"height":1080}')
    assert module.saved_display(path, legacy_scale=1) == (3840, 2160)
    path.write_text('{"width":3456,"height":1892,"pixel_ratio":2}')
    assert module.saved_display(path) == (3456, 1892)


def test_repeated_restarts_keep_only_current_owned_process_groups(monkeypatch):
    module = supervisor()
    started = []
    stopped = []

    def start(*args, **kwargs):
        p = SimpleNamespace(pid=len(started) + 1)
        started.append(p)
        return p

    monkeypatch.setattr(module.subprocess, "Popen", start)
    monkeypatch.setattr(module, "stop_process", lambda p: stopped.append(p.pid) if p else None)
    session = module.Session({"command": ["editor"], "mcp": {"command": ["adapter"], "port": 8000}})
    for _ in range(10):
        session.launch()
        session.launch_mcp()
    assert len(session.processes) == 2
    assert set(stopped) == set(range(1, 19))
    session.stop("mcp", "application")
    assert session.processes == {}
    assert set(stopped) == set(range(1, 21))


def test_launcher_handoff_uses_owned_windows_and_closes_children(monkeypatch):
    module = supervisor()
    clock = [0]
    windows = []
    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(module, "visible_windows", lambda _: windows)
    monkeypatch.setattr(module.subprocess, "Popen", lambda *a, **kw: MagicMock(poll=lambda: 0))
    stop = MagicMock()
    monkeypatch.setattr(module, "stop_process", stop)
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


def test_application_start_timeout_and_mcp_start_have_independent_deadlines(monkeypatch):
    module = supervisor()
    clock = [0]
    windows = []
    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(module, "visible_windows", lambda _: windows)
    monkeypatch.setattr(module.subprocess, "Popen", lambda *a, **kw: MagicMock(poll=lambda: None))
    monkeypatch.setattr(module, "stop_process", MagicMock())
    monkeypatch.setattr(module, "mcp_ready", lambda *a: False)
    session = module.Session({"command": ["editor"], "mcp": {"command": ["adapter"], "port": 8000}})
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
    monkeypatch.setattr(module.subprocess, "Popen", lambda *a, **kw: MagicMock(poll=lambda: None))
    session.launch()
    windows.clear()
    clock[0] += module.POLICY.application_timeout + 1
    session.poll_application()
    assert session.state.application == "error"
    assert session.processes == {}


def test_guest_resize_keeps_physical_dpi_and_rejects_invalid_geometry(monkeypatch):
    module = supervisor()
    commands = []
    monkeypatch.setattr(module.subprocess, "run", lambda argv, **kwargs: commands.append(argv))
    monkeypatch.setattr(module.sys, "argv", ["kiosk", "--resize", "2880", "1800"])
    assert module.main() == 0
    assert commands == [["selkies", "resize", "2880x1800"], ["xrandr", "--dpi", "192"]]
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
    module = supervisor()
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


@pytest.mark.parametrize("sse", [True, False])
def test_mcp_readiness_cannot_be_held_open_by_continuous_keepalives(monkeypatch, sse):
    from dataclasses import replace
    import time

    module = supervisor()
    monkeypatch.setattr(module, "POLICY", replace(module.POLICY, command_timeout=0.08))

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream" if sse else "application/json")
            self.end_headers()
            try:
                for _ in range(100):
                    self.wfile.write(b": keepalive\n\n" if sse else b" ")
                    self.wfile.flush()
                    time.sleep(0.02)
            except (BrokenPipeError, ConnectionResetError):
                pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        started = time.monotonic()
        assert module.mcp_ready(server.server_port, "/mcp") is False
        assert time.monotonic() - started < 0.5
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_pinned_backend_pointer_fix_preserves_clicks_and_real_drags(tmp_path):
    import runpy
    import textwrap

    patcher = runpy.run_path(str(Path(vm_file).parent / "assets/kiosk-selkies-patch"))
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
    path.write_text(original)
    patcher["patch_file"](path, *patcher["PATCHES"][path.name])
    patched = path.read_text()
    patcher["patch_file"](path, *patcher["PATCHES"][path.name])
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
        patcher["patch_file"](path, *patcher["PATCHES"][path.name])
