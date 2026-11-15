import hashlib
import io
import json
import tarfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from beta9 import mirror
from beta9.abstractions.vm import VM
from beta9.cli import extraclick
from beta9.cli import mirror as mirror_cli
from beta9.cli import setup as setup_cli
from beta9.cli import vm as vm_cli

UTILITY = """#!/bin/sh
echo "$@" >> "$(dirname "$0")/calls"
if [ "$1" = status ]; then
  echo '{"mirror": "laptop", "paused": false, "roots": ["projects/app"], "generation": 3}'
fi
"""


def archive(members):
    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w:gz") as tar:
        for name, content in members.items():
            info = tarfile.TarInfo(name)
            if content is None:
                info.type, info.linkname = tarfile.SYMTYPE, "/bin/sh"
                tar.addfile(info)
            else:
                info.size, info.mode = len(content), 0o755
                tar.addfile(info, io.BytesIO(content))
    return data.getvalue()


@pytest.fixture
def machine(monkeypatch, tmp_path):
    """A signed-in Linux machine whose releases come from a local server."""
    files, requests = {}, []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            body = files.get(self.path.lstrip("/"))
            self.send_response(200 if body is not None else 404)
            self.end_headers()
            self.wfile.write(body or b"")

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setenv("BEAM_MIRROR_RELEASES_URL", f"http://127.0.0.1:{server.server_port}/")
    monkeypatch.setenv("PATH", "/usr/bin:/bin")
    monkeypatch.setattr(mirror.platform, "system", lambda: "Linux")
    monkeypatch.setattr(mirror.platform, "machine", lambda: "x86_64")
    settings = SimpleNamespace(config_path=tmp_path / "beam" / "config.ini", name="Beam")
    monkeypatch.setattr(mirror, "get_settings", lambda: settings)

    def release(members, checksum=None):
        asset = archive(members)
        files[mirror.asset_name()] = asset
        files[mirror.CHECKSUMS] = (
            f"{checksum or hashlib.sha256(asset).hexdigest()}  {mirror.asset_name()}\n"
            f"{'0' * 64}  beam-mirror-aarch64-apple-darwin.tar.gz\n"
        ).encode()

    yield SimpleNamespace(release=release, requests=requests, files=files, root=tmp_path)
    server.shutdown()


def test_install_verifies_the_release_and_rerunning_updates(machine):
    assert mirror.installed() is None
    machine.release({"beam-mirror": UTILITY.encode()})

    path, updated = mirror.install()
    assert updated and path == machine.root / "beam" / "bin" / "beam-mirror"
    assert path.read_text() == UTILITY and path.stat().st_mode & 0o777 == 0o755
    assert mirror.installed() == str(path)

    del machine.requests[:]
    assert mirror.install() == (path, False)
    assert machine.requests == ["/" + mirror.CHECKSUMS]

    machine.release({"./beam-mirror": b"#!/bin/sh\necho newer\n"})
    assert mirror.install() == (path, True)
    assert path.read_text() == "#!/bin/sh\necho newer\n"


@pytest.mark.parametrize(
    "members,checksum,reason",
    [
        ({"beam-mirror": b"tampered"}, "1" * 64, "does not match its checksum"),
        ({"../../evil": b"x", "bin/beam-mirror": b"x"}, None, "does not contain beam-mirror"),
        ({"beam-mirror": None}, None, "does not contain beam-mirror"),
    ],
)
def test_install_takes_nothing_from_a_release_it_cannot_trust(machine, members, checksum, reason):
    machine.release({"beam-mirror": UTILITY.encode()})
    path, _ = mirror.install()

    machine.release(members, checksum)
    with pytest.raises(mirror.MirrorError, match=reason):
        mirror.install()
    assert path.read_text() == UTILITY
    assert sorted(p.name for p in machine.root.rglob("*") if p.is_file()) == [
        "beam-mirror",
        "beam-mirror.sha256",
    ]


def test_install_reports_a_platform_without_a_build(machine, monkeypatch):
    machine.release({"beam-mirror": UTILITY.encode()})
    monkeypatch.setattr(mirror.platform, "system", lambda: "Windows")
    with pytest.raises(mirror.MirrorError, match="no mirror build for Windows"):
        mirror.install()
    monkeypatch.setattr(mirror.platform, "system", lambda: "Linux")
    monkeypatch.setattr(mirror.platform, "machine", lambda: "aarch64")
    with pytest.raises(mirror.MirrorError, match="has no beam-mirror-aarch64-unknown-linux-gnu"):
        mirror.install()
    del machine.files[mirror.CHECKSUMS]
    with pytest.raises(mirror.MirrorError, match="No mirror release was found"):
        mirror.install()


@pytest.mark.parametrize("as_json", [False, True])
def test_setup_mirror_signs_in_installs_and_starts_the_utility(machine, monkeypatch, as_json):
    machine.release({"beam-mirror": UTILITY.encode()})
    signed_in = []
    monkeypatch.setattr(setup_cli, "connect_workspace", signed_in.append)
    monkeypatch.setenv("BETA9_JSON", "1" if as_json else "0")

    result = CliRunner(mix_stderr=False).invoke(setup_cli.setup, ["mirror"])
    assert result.exit_code == 0, result.output + result.stderr
    path = machine.root / "beam" / "bin" / "beam-mirror"
    assert signed_in == ["default"]
    assert (path.parent / "calls").read_text().splitlines() == [
        f"init --config {machine.root / 'beam' / 'config.ini'} --context default",
        "status --json",
    ]
    if as_json:
        assert json.loads(result.stdout) == {
            "path": str(path),
            "updated": True,
            "mirror": "laptop",
            "paused": False,
            "roots": ["projects/app"],
            "generation": 3,
        }
    else:
        assert "Mirror laptop" in result.stdout and "projects/app" in result.stdout
        assert "beta9 mirror launch " in result.stdout
        assert "beta9 mirror open NAME " in result.stdout
        assert "beta9 mirror run " in result.stdout


def test_setup_mirror_guides_an_empty_selection(machine, monkeypatch):
    machine.release({"beam-mirror": UTILITY.replace('["projects/app"]', "[]").encode()})
    monkeypatch.setattr(setup_cli, "connect_workspace", lambda selected: None)
    result = CliRunner().invoke(setup_cli.setup, ["mirror", "--context", "staging"])
    assert result.exit_code == 0, result.output
    assert "beta9 --context staging mirror suggest --add RELATIVE_PATH" in result.output
    assert "beta9 --context staging mirror launch" in result.output


def test_setup_mirror_stops_when_the_utility_cannot_start(machine, monkeypatch):
    machine.release(
        {"beam-mirror": b"#!/bin/sh\necho 'not signed in: run beam login' >&2\nexit 1\n"}
    )
    monkeypatch.setattr(setup_cli, "connect_workspace", lambda selected: None)
    result = CliRunner(mix_stderr=False).invoke(setup_cli.setup, ["mirror"])
    assert result.exit_code == 1
    assert "not signed in: run beam login" in result.stderr


@pytest.fixture
def new_vm(monkeypatch):
    client = MagicMock()
    client.__enter__.return_value = SimpleNamespace(channel=MagicMock(), http=MagicMock())
    monkeypatch.setattr(extraclick, "ServiceClient", lambda config: client)
    monkeypatch.setattr(extraclick, "get_config_context", lambda context: SimpleNamespace())
    factory = MagicMock(return_value=MagicMock(info={"name": "dev", "id": "vm-id", "spec": {}}))
    monkeypatch.setattr(vm_cli, "VM", factory)

    def run(*args):
        result = CliRunner(mix_stderr=False).invoke(vm_cli.management, ["new", *args, "--json"])
        return result, factory

    return run


def test_vm_new_mirror_names_this_machines_mirror(machine, new_vm):
    result, factory = new_vm("dev", "--mirror")
    assert result.exit_code == 1 and not factory.called
    assert "not installed on this machine. Run: beta9 setup mirror" in result.stderr

    status = {
        "mirror": "laptop",
        "context": "default",
        "config_path": str(machine.root / "beam" / "config.ini"),
    }
    machine.release({"beam-mirror": f"#!/bin/sh\necho '{json.dumps(status)}'\n".encode()})
    mirror.install()
    result, factory = new_vm("dev", "--mirror")
    assert result.exit_code == 0, result.stderr
    assert factory.call_args.args == ("dev",) and factory.call_args.kwargs["mirror"] == "laptop"


@pytest.mark.parametrize(
    "binding", [{}, {"context": "staging"}, {"config_path": "/other/beam.ini"}]
)
def test_vm_new_rejects_a_local_mirror_for_a_different_or_unknown_context(
    machine, monkeypatch, new_vm, binding
):
    status = {"mirror": "laptop", **binding}
    if "config_path" in binding:
        status["context"] = "default"
    if "context" in binding:
        status["config_path"] = str(machine.root / "beam" / "config.ini")
    monkeypatch.setattr(mirror, "installed", lambda: "/usr/bin/beam-mirror")
    monkeypatch.setattr(mirror, "status", lambda: status)

    result, factory = new_vm("dev", "--mirror")
    assert result.exit_code == 1 and not factory.called
    assert "not configured for the selected Beam context" in result.stderr


def test_vm_new_checks_the_explicit_context_for_a_local_mirror(machine, monkeypatch, new_vm):
    status = {
        "mirror": "laptop",
        "context": "default",
        "config_path": str(machine.root / "beam" / "config.ini"),
    }
    monkeypatch.setattr(mirror, "installed", lambda: "/usr/bin/beam-mirror")
    monkeypatch.setattr(mirror, "status", lambda: status)

    result, factory = new_vm("dev", "--mirror", "--context", "staging")
    assert result.exit_code == 1 and not factory.called
    assert "beta9 --context staging setup mirror" in result.stderr

    status["context"] = "staging"
    result, factory = new_vm("dev", "--mirror", "--context", "staging")
    assert result.exit_code == 0, result.stderr
    assert factory.call_args.kwargs["mirror"] == "laptop"


def test_vm_new_takes_a_named_mirror_or_none(machine, new_vm):
    result, factory = new_vm("dev", "--mirror", "work-laptop")
    assert result.exit_code == 0, result.stderr
    assert factory.call_args.kwargs["mirror"] == "work-laptop"

    result, factory = new_vm("dev")
    assert result.exit_code == 0, result.stderr
    assert factory.call_args.kwargs["mirror"] is None


def test_vm_new_leaves_exit_status_unset_for_the_cli_wrapper(new_vm):
    result = CliRunner().invoke(vm_cli.management, ["new", "dev", "--json"], standalone_mode=False)
    assert result.exit_code == 0, result.output
    assert result.return_value is None


@pytest.mark.parametrize(
    "roots,paused,push_exit",
    [([], False, 0), (["work"], True, 0), (["work"], False, 9), (["work"], False, 0)],
)
def test_mirror_launch_uploads_before_creating(
    machine, monkeypatch, new_vm, roots, paused, push_exit
):
    monkeypatch.setattr(mirror, "installed", lambda: "/usr/bin/beam-mirror")
    config = str(machine.root / "beam" / "config.ini")
    monkeypatch.setattr(
        mirror,
        "status",
        lambda: {
            "mirror": "laptop",
            "context": "staging",
            "config_path": config,
            "roots": roots,
            "paused": paused,
        },
    )
    push = MagicMock(return_value=push_exit)
    monkeypatch.setattr(mirror, "run", push)
    _, factory = new_vm("existing")
    factory.reset_mock()

    def upload(*args, **kwargs):
        factory.assert_not_called()
        return push_exit

    push.side_effect = upload
    browser = MagicMock()
    monkeypatch.setattr(vm_cli, "_open_browser", browser)

    result = CliRunner(mix_stderr=False).invoke(
        mirror_cli.mirror_group,
        ["--context", "staging", "launch", "dev", "--pool", "cpu", "--json"],
    )
    if not roots or paused or push_exit:
        assert result.exit_code == 1 and not factory.called
        assert push.called == bool(roots and not paused)
        return

    assert result.exit_code == 0, result.stderr
    assert json.loads(result.stdout)["name"] == "dev"
    assert push.call_args.args == ("push",)
    assert push.call_args.kwargs["stdout"] is not None
    assert factory.call_args.args == ("dev",)
    assert factory.call_args.kwargs["mirror"] == "laptop"
    assert factory.call_args.kwargs["pool"] == "cpu"
    assert factory.call_args.kwargs["desktop"] is True
    browser.assert_not_called()


def test_mirror_launch_uses_the_selected_context_before_upload(machine, monkeypatch):
    monkeypatch.setattr(mirror, "installed", lambda: "/usr/bin/beam-mirror")
    monkeypatch.setattr(
        mirror,
        "status",
        lambda: {
            "mirror": "laptop",
            "context": "default",
            "config_path": str(machine.root / "beam" / "config.ini"),
            "roots": ["work"],
        },
    )
    push = MagicMock()
    monkeypatch.setattr(mirror, "run", push)
    result = CliRunner().invoke(mirror_cli.mirror_group, ["--context", "staging", "launch"])
    assert result.exit_code == 1
    assert "beta9 --context staging setup mirror" in result.output
    push.assert_not_called()


def test_mirror_launch_opens_the_created_desktop(monkeypatch, new_vm):
    monkeypatch.setattr(mirror_cli, "_selection", lambda: {"mirror": "laptop", "roots": ["work"]})
    monkeypatch.setattr(mirror, "run", lambda *args, **kwargs: 0)
    _, factory = new_vm("existing")
    vm = factory.return_value
    vm.info["desktop_url"] = "https://dev.example.invalid"
    opener = MagicMock()
    monkeypatch.setattr(vm_cli, "_open_browser", opener)

    result = CliRunner().invoke(mirror_cli.mirror_group, ["launch", "dev"], standalone_mode=False)
    assert result.exit_code == 0, result.output
    assert result.return_value is None
    opener.assert_called_once_with(vm, 8080, vm.info["desktop_url"])


def test_mirror_add_preserves_arguments_and_utility_exit(monkeypatch):
    monkeypatch.setattr(mirror_cli, "_selection", lambda: {})
    run = MagicMock(return_value=7)
    monkeypatch.setattr(mirror, "run", run)
    result = CliRunner().invoke(mirror_cli.mirror_group, ["add", "my files", "--exclude", "*.temp"])
    assert result.exit_code == 7
    run.assert_called_once_with("add", "my files", "--exclude", "*.temp")


def test_mirror_open_resumes_the_existing_vm(monkeypatch):
    callback = MagicMock()
    monkeypatch.setattr(vm_cli.desktop_vm, "callback", callback)
    result = CliRunner().invoke(mirror_cli.mirror_group, ["open", "my-vm"])
    assert result.exit_code == 0, result.output
    callback.assert_called_once_with(
        name="my-vm", resume=True, open_url=True, url_only=False, context=None
    )


def test_vm_requests_a_mirror_by_name_only():
    service = SimpleNamespace(channel=MagicMock(), http=MagicMock())
    assert VM("dev", mirror="laptop", _service=service)._spec["mirror"] == {"name": "laptop"}
    assert "mirror" not in VM("dev", _service=service)._spec


def test_vm_details_show_the_pinned_generation(capsys):
    vm_cli._show(
        {
            "name": "dev",
            "id": "vm-id",
            "status": "running",
            "spec": {"mirror": {"name": "laptop", "id": "m", "generation": 4}},
        },
        False,
    )
    assert "laptop, generation 4" in capsys.readouterr().out
