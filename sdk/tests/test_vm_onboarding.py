import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from beta9.channel import GatewayHTTPError
from beta9.config import ConfigContext
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
        setup.common, ["setup", "vm", "--context", "staging", "--name", "first", "--yes"]
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


def test_interactive_credit_checkpoint_waits_for_confirmed_funding(monkeypatch):
    import webbrowser

    from beta9.cli import setup

    opening = MagicMock()
    prompt = MagicMock(return_value="")
    monkeypatch.setattr(webbrowser, "open", opening)
    monkeypatch.setattr(setup.click, "prompt", prompt)
    monkeypatch.setattr(setup.sys.stdin, "isatty", lambda: True)
    statuses = iter(
        [
            {
                "workspace_name": "test",
                "credit": {
                    "ok": False,
                    "message": "Add credits at https://account.example/credits",
                },
            },
            {"credit": {"ok": False}},
            {"credit": {"ok": True}},
        ]
    )
    monkeypatch.setattr("beta9.mcp.tools.call_remote", lambda *_: next(statuses))
    monkeypatch.setattr("time.sleep", lambda _: None)
    setup.ensure_credit(object(), unattended=False)
    opening.assert_called_once_with("https://account.example/credits", new=2)
    prompt.assert_called_once_with(
        "Press enter to add credits via Stripe (Ctrl+C to exit)",
        default="",
        show_default=False,
        prompt_suffix=" ",
    )


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
