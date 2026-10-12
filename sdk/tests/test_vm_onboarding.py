import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from beta9.channel import GatewayHTTPError
from beta9.deployment import onboarding


def test_lost_creation_response_resumes_identical_request(tmp_path, monkeypatch):
    selected = SimpleNamespace(http=SimpleNamespace(workspace_id="workspace"))
    vm = MagicMock()
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


def test_setup_vm_emits_one_json_result_after_guest_readiness(tmp_path, monkeypatch):
    from click.testing import CliRunner

    from beta9.cli import setup

    monkeypatch.setenv("BETA9_JSON", "1")
    monkeypatch.setattr("beta9.mcp.server.context_or_none", lambda _: object())
    monkeypatch.setattr("beta9.config.get_config_context", lambda _: object())
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
        setup.common, ["setup", "vm", "--context", "staging", "--name", "first", "--yes"]
    )
    assert result.exit_code == 0, result.output
    report = json.loads(result.stdout)
    assert report == {
        "ready": True,
        "context": "staging",
        "agents": configured,
        "vm": {"name": "first", "id": "vm-ready"},
    }
    prepare.assert_called_once()


def test_setup_vm_does_not_report_success_after_failed_probe(tmp_path, monkeypatch):
    from click.testing import CliRunner

    from beta9.cli import setup

    monkeypatch.setattr("beta9.mcp.server.context_or_none", lambda _: object())
    monkeypatch.setattr("beta9.config.get_config_context", lambda _: object())
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
