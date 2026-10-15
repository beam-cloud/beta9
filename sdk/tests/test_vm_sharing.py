"""Sharing should take one command, explain the access, and remain usable in scripts."""

import json
import subprocess
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from beta9.cli import extraclick
from beta9.cli import vm as cli


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
    monkeypatch.setattr(cli, "_copy_share_link", copied)
    monkeypatch.setattr(cli.terminal, "is_interactive", lambda: True)
    opened = MagicMock()
    monkeypatch.setattr(cli.webbrowser, "open", opened)
    return SimpleNamespace(
        service=service, info=info, session=session, copied=copied, opened=opened
    )


def test_share_defaults_to_a_terminal_link_copied_for_one_hour(sharing):
    result = CliRunner().invoke(cli.management, ["share", "dev"])
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
    result = CliRunner().invoke(cli.management, ["share", "dev", *args])
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
    result = CliRunner().invoke(cli.management, ["share", "dev", *args])
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
    result = CliRunner().invoke(cli.management, ["share", "dev", *args])
    assert result.exit_code == 2, result.output
    sharing.service.http.json.assert_not_called()
    sharing.copied.assert_not_called()


@pytest.mark.parametrize("mode", ["--url", "--json", "global-json"])
def test_machine_output_does_not_touch_the_clipboard(sharing, monkeypatch, mode):
    if mode == "global-json":
        monkeypatch.setenv("BETA9_JSON", "1")
    result = CliRunner().invoke(
        cli.management, ["share", "dev", *([] if mode == "global-json" else [mode])]
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
    result = CliRunner().invoke(cli.management, ["share", "dev"])
    assert result.exit_code == 0, result.output
    assert sharing.session["url"] in result.output
    assert "copied" not in result.output


def test_piped_output_does_not_overwrite_the_users_clipboard(sharing, monkeypatch):
    monkeypatch.setattr(cli.terminal, "is_interactive", lambda: False)
    result = CliRunner().invoke(cli.management, ["share", "dev"])
    assert result.exit_code == 0, result.output
    assert sharing.session["url"] in result.output
    sharing.copied.assert_not_called()


def test_public_port_returns_its_existing_url_without_claiming_expiry(sharing):
    sharing.info["spec"]["protected_ports"] = []
    result = CliRunner().invoke(cli.management, ["share", "dev", "--port", "3000"])
    assert result.exit_code == 0, result.output
    assert "https://app.example/" in result.output
    assert "public" in result.output and "does not expire" in result.output
    sharing.service.http.json.assert_called_once()


def test_an_expiring_share_cannot_silently_become_a_public_permanent_link(sharing):
    sharing.info["spec"]["protected_ports"] = []
    result = CliRunner().invoke(
        cli.management, ["share", "dev", "--port", "3000", "--expires-in", "10m"]
    )
    assert result.exit_code == 1, result.output
    assert "public" in result.output and "--protected" in result.output
    sharing.service.http.json.assert_called_once()
    sharing.copied.assert_not_called()


def test_clipboard_failure_falls_back_without_exposing_the_link_to_a_shell(monkeypatch):
    monkeypatch.setattr(cli.sys, "platform", "linux")
    monkeypatch.setattr(cli.shutil, "which", lambda name: "/usr/bin/" + name)
    run = MagicMock(side_effect=[subprocess.CalledProcessError(1, "wl-copy"), None])
    monkeypatch.setattr(cli.subprocess, "run", run)
    link = "https://terminal.example/?beam_vm_session=share"
    assert cli._copy_share_link(link) is True
    assert run.call_count == 2
    assert run.call_args.args == (["/usr/bin/xclip", "-selection", "clipboard"],)
    assert run.call_args.kwargs["input"] == link
    assert "shell" not in run.call_args.kwargs
