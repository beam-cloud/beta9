import datetime
import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import grpc
import pytest
from click.testing import CliRunner

from beta9.cli import extraclick
from beta9.cli import vm as vm_cli


@pytest.fixture
def selected(monkeypatch):
    service = MagicMock()
    service.__enter__.return_value = service
    monkeypatch.setattr(extraclick, "ServiceClient", lambda config: service)
    monkeypatch.setattr(extraclick, "get_config_context", lambda context: SimpleNamespace())
    return service


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
def test_list_uses_current_boot_and_preserves_filters(selected, monkeypatch, width):
    records = [record(), {**record("paused"), "name": "paused-vm", "id": "vm-0000000000000001"}]
    listing = MagicMock(return_value=records)
    monkeypatch.setattr(vm_cli.VM, "list", listing)
    now = datetime.datetime.now(datetime.timezone.utc)
    selected.gateway.list_containers.return_value = SimpleNamespace(
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
        all=True, metadata={"team": "a=b"}, status="running", _service=selected
    )
    selected.gateway.list_containers.assert_called_once()


@pytest.mark.parametrize("records", [[], [record()]])
def test_json_is_unchanged_and_needs_no_container_lookup(selected, monkeypatch, records):
    monkeypatch.setattr(vm_cli.VM, "list", lambda **kwargs: records)
    result = CliRunner().invoke(vm_cli.management, ["list", "--json"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == records
    selected.gateway.list_containers.assert_not_called()


def test_stopped_list_needs_no_container_lookup(selected, monkeypatch):
    monkeypatch.setattr(vm_cli.VM, "list", lambda **kwargs: [record("stopped")])
    result = CliRunner().invoke(vm_cli.management, ["list"], env={"COLUMNS": "120"})
    assert result.exit_code == 0, result.output
    assert "100GiB" in result.output and "1 VM" in result.output
    selected.gateway.list_containers.assert_not_called()


def test_runtime_lookup_failure_does_not_hide_vms(selected, monkeypatch):
    monkeypatch.setattr(vm_cli.VM, "list", lambda **kwargs: [record()])
    selected.gateway.list_containers.side_effect = grpc.RpcError()
    result = CliRunner().invoke(vm_cli.management, ["list"], env={"COLUMNS": "120"})
    assert result.exit_code == 0, result.output
    assert "warm-willow" in result.output and "100GiB" in result.output


@pytest.mark.parametrize("name", ["beta9", "beam"])
def test_empty_state_uses_the_cli_name(selected, monkeypatch, name):
    monkeypatch.setattr(vm_cli.VM, "list", lambda **kwargs: [])
    monkeypatch.setattr(vm_cli.terminal, "cli_name", lambda: name)
    result = CliRunner().invoke(vm_cli.management, ["list"])
    assert result.exit_code == 0, result.output
    assert "No VMs found" in result.output and f"{name} vm new" in result.output
    selected.gateway.list_containers.assert_not_called()
