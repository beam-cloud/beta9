from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from beta9 import VM
from beta9.cli import extraclick, vm as vm_cli
from beta9.config import get_settings
from beta9.exceptions import SandboxConnectionError


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
    assert result.output == (
        f"Error: VM is not running. Start it with: {start_command}\n"
    )
