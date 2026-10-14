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
    removed, replacement = MagicMock(), MagicMock()
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


@pytest.mark.parametrize(("args", "name"), [(["--name", "first"], "first"), ([], "my-first-vm")])
def test_setup_vm_emits_one_json_result_after_guest_readiness(tmp_path, monkeypatch, args, name):
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
