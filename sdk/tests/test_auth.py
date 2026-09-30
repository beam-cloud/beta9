"""
Device-grant sign-in: the poll loop survives what the network does to it, and
every context signs in against its own environment.
"""

import json
from typing import Any, Dict, List, Optional

import pytest
import requests

from beta9 import auth
from beta9.config import (
    ConfigContext,
    SDKSettings,
    context_defaults,
    get_config_context,
    load_config,
    save_config,
    set_settings,
)

STAGING = ConfigContext(
    gateway_host="gw.stage.example",
    gateway_port=443,
    api_url="https://app.stage.example",
    auth_url="https://auth.stage.example/oauth",
)


@pytest.fixture
def settings(monkeypatch, tmp_path):
    config_path = tmp_path / "config.ini"
    monkeypatch.setenv("CONFIG_PATH", str(config_path))
    monkeypatch.delenv("BETA9_TOKEN", raising=False)
    s = SDKSettings(
        name="Beta9",
        config_path=config_path,
        api_token=None,
        gateway_host="gw.example",
        gateway_port=443,
        auth_url="https://auth.example/oauth",
        environments={"staging": STAGING},
    )
    set_settings(s)
    yield s
    set_settings(None)


class FakeResponse:
    """`payload` is served as JSON; `text` alone is a body that is not JSON (an HTML 502)."""

    def __init__(self, status_code: int, payload: Optional[Dict[str, Any]] = None, text: str = ""):
        self.status_code = status_code
        self._payload = payload
        self.content = (json.dumps(payload) if payload is not None else text).encode()

    def json(self):
        if self._payload is None:
            raise ValueError("not json")
        return self._payload


def flow() -> auth.DeviceLogin:
    return auth.DeviceLogin(
        target=context_defaults(),
        device_code="dev-1",
        user_code="ABCD-EFGH",
        verification_uri="https://dash.example/activate",
        verification_uri_complete="https://dash.example/activate?code=ABCD-EFGH",
        expires_at=auth.time.monotonic() + 600,
        interval=0,
    )


def posts(monkeypatch, responses: List[Any]) -> List[str]:
    """Serve `responses` in order; returns the list the posted URLs land in."""
    queue = iter(responses)
    urls: List[str] = []

    def fake_post(url: str, json: Optional[Dict[str, Any]] = None, timeout: Optional[float] = None):
        urls.append(url)
        item = next(queue)
        if isinstance(item, Exception):
            raise item
        return item

    monkeypatch.setattr(auth.requests, "post", fake_post)
    return urls


def test_wait_rides_out_blips_and_bad_gateways(settings, monkeypatch):
    posts(
        monkeypatch,
        [
            requests.ConnectionError("reset by peer"),
            FakeResponse(502, text="<html>Bad Gateway</html>"),
            FakeResponse(400, {"error": "authorization_pending"}),
            FakeResponse(200, {"access_token": "t" * 64, "workspace_name": "acme"}),
        ],
    )

    login = flow()
    context = login.wait()

    assert context.token == "t" * 64
    assert login.workspace_name == "acme"
    assert login.failures == 0  # a good answer resets the count


def test_wait_gives_up_when_the_service_stays_down(settings, monkeypatch):
    posts(monkeypatch, [requests.ConnectionError("down")] * auth.MAX_TRANSIENT_FAILURES)

    with pytest.raises(auth.LoginError) as exc:
        flow().wait()
    assert exc.value.code == "AUTH_UNAVAILABLE"
    assert "Could not reach the sign-in service" in str(exc.value)


def test_poll_reports_unknown_errors_without_a_traceback(settings, monkeypatch):
    posts(monkeypatch, [FakeResponse(400, text="<html>nope</html>")])

    with pytest.raises(auth.LoginError) as exc:
        flow().poll()
    assert str(exc.value) == "Sign-in failed (HTTP 400)"


def test_start_rejects_a_non_json_answer(settings, monkeypatch):
    posts(monkeypatch, [FakeResponse(502, text="<html>Bad Gateway</html>")])

    with pytest.raises(auth.LoginError) as exc:
        auth.DeviceLogin.start()
    assert exc.value.code == "AUTH_UNAVAILABLE"
    assert str(exc.value) == "Could not start sign-in (HTTP 502)"


DEVICE_CODE = {
    "device_code": "dev-1",
    "user_code": "ABCD-EFGH",
    "verification_uri": "https://dash.stage.example/activate",
    "expires_in": 600,
    "interval": 0,
}


def test_login_into_an_environment_saves_a_context_that_points_there(settings, monkeypatch):
    urls = posts(
        monkeypatch,
        [FakeResponse(200, DEVICE_CODE), FakeResponse(200, {"access_token": "t" * 64})],
    )

    login = auth.DeviceLogin.start(context_defaults("staging"))
    context = login.wait()
    auth.save_login(context, name="qa")

    assert urls == [
        "https://auth.stage.example/oauth/device/code",
        "https://auth.stage.example/oauth/token",
    ]
    saved = load_config()["qa"]
    assert (saved.token, saved.gateway_host, saved.api_url, saved.auth_url) == (
        "t" * 64,
        "gw.stage.example",
        "https://app.stage.example",
        "https://auth.stage.example/oauth",
    )
    assert context_defaults("qa") == saved  # signing in again renews it in place


def test_a_context_saved_without_auth_url_borrows_its_gateways(settings):
    """Contexts from before `auth_url` existed, or from `config create`, can still `login`."""
    save_config(
        {
            "default": ConfigContext(token="a" * 64, gateway_host="gw.example", gateway_port=443),
            "stage-qa": ConfigContext(
                token="b" * 64, gateway_host="gw.stage.example", gateway_port=443
            ),
            "elsewhere": ConfigContext(token="c" * 64, gateway_host="gw.other", gateway_port=443),
        }
    )

    assert context_defaults("default").auth_url == "https://auth.example/oauth"
    assert context_defaults("stage-qa").auth_url == "https://auth.stage.example/oauth"
    assert context_defaults("stage-qa").token == "b" * 64
    assert auth.login_configured("elsewhere") is False


def test_default_section_does_not_leak_into_other_contexts(settings):
    settings.config_path.write_text(
        "[default]\ntoken = aaaa\ngateway_host = gw.example\ngateway_port = 443\n"
        "auth_url = https://auth.example/oauth\n\n"
        "[stage-qa]\ntoken = bbbb\ngateway_host = gw.stage.example\ngateway_port = 443\n"
    )

    contexts = load_config()
    assert contexts["stage-qa"].auth_url is None
    assert context_defaults("stage-qa").auth_url == "https://auth.stage.example/oauth"


def test_environment_token_targets_the_environments_gateway(settings, monkeypatch):
    """`BETA9_TOKEN=... cli --context staging` (CI) talks to staging, not the default gateway."""
    monkeypatch.setenv("BETA9_TOKEN", "t" * 64)
    monkeypatch.delenv("BETA9_GATEWAY_HOST", raising=False)  # an explicit host would win
    monkeypatch.delenv("BETA9_GATEWAY_PORT", raising=False)

    context = get_config_context("staging")

    assert (context.token, context.gateway_host, context.auth_url) == (
        "t" * 64,
        "gw.stage.example",
        "https://auth.stage.example/oauth",
    )
    assert get_config_context("default").gateway_host == "gw.example"
