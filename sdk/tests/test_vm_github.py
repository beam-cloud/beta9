import subprocess
from unittest.mock import Mock

import pytest

from beta9.abstractions.vm.github import connect


def desktop_vm():
    vm = Mock()
    vm.info = {"spec": {"desktop": True, "desktop_transport": "webrtc"}}
    return vm


@pytest.mark.parametrize(
    "spec",
    [
        {},
        {"desktop": True},
        {"desktop": True, "desktop_transport": "webrtc", "kiosk": {"command": ["app"]}},
    ],
)
def test_github_sign_in_requires_current_desktop(spec):
    vm = desktop_vm()
    vm.info["spec"] = spec
    with pytest.raises(ValueError, match="requires a VM with the current desktop"):
        connect(vm)
    vm.refresh.assert_called_once()
    vm.desktop.launch.assert_not_called()


def test_github_sign_in_resumes_the_same_private_desktop():
    vm = desktop_vm()
    vm.info["status"] = "stopped"
    connect(vm)
    vm.start.assert_called_once_with()
    vm.desktop.launch.assert_called_once()


@pytest.mark.parametrize("failure", [None, "login", "setup-git", "missing"])
def test_github_sign_in_runs_in_guest_terminal(tmp_path, failure):
    vm = desktop_vm()
    assert connect(vm) is vm.desktop.launch.return_value
    vm.refresh.assert_called_once()
    terminal, wait, separator, shell, execute, script = vm.desktop.launch.call_args.args
    assert (terminal, wait, separator) == ("/usr/bin/gnome-terminal", "--wait", "--")
    assert (shell, execute) == ("/bin/sh", "-c")

    log = tmp_path / "log"
    if failure != "missing":
        gh = tmp_path / "gh"
        gh.write_text(
            "#!/bin/sh\n"
            '[ -z "${GH_TOKEN+x}${GITHUB_TOKEN+x}${GH_PROMPT_DISABLED+x}${GH_DEBUG+x}" ] || exit 99\n'
            'printf "%s\\n" "$*" >> "$LOG"\n'
            'if [ "$2" = "$FAILURE" ]; then printf "failure\\n" >&2; exit 12; fi\n'
        )
        gh.chmod(0o700)

    run = subprocess.run(
        [shell, execute, script],
        input="\n",
        text=True,
        capture_output=True,
        env={
            "PATH": str(tmp_path),
            "LOG": str(log),
            "FAILURE": failure or "",
            "GH_TOKEN": "ignored-test-value",
            "GITHUB_TOKEN": "ignored-test-value",
            "GH_PROMPT_DISABLED": "1",
            "GH_DEBUG": "api",
        },
        timeout=5,
        check=False,
    )
    assert "Press Enter to close" in run.stdout
    if failure == "missing":
        assert run.returncode == 127
        assert "Install GitHub CLI in this VM" in run.stderr
        assert not log.exists()
        return

    commands = log.read_text().splitlines()
    assert commands[0] == "auth login --hostname github.com --git-protocol https --web"
    if failure == "login":
        assert len(commands) == 1
    else:
        assert commands[1:] == ["auth setup-git --hostname github.com"]
    if failure:
        assert run.returncode == 12
        assert "GitHub setup did not complete" in run.stderr
    else:
        assert run.returncode == 0
        assert "GitHub is connected" in run.stdout
