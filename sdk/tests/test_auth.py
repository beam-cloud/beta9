"""Device-grant sign-in: the poll loop survives what the network does to it."""

import json
from typing import Any, Dict, List, Optional

import pytest
import requests

from beta9 import auth
from beta9.config import SDKSettings, set_settings


@pytest.fixture
def settings(monkeypatch, tmp_path):
    config_path = tmp_path / "config.ini"
    monkeypatch.setenv("CONFIG_PATH", str(config_path))
    s = SDKSettings(
        name="Beta9", config_path=config_path, api_token=None, auth_url="https://auth.example/oauth"
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
        device_code="dev-1",
        user_code="ABCD-EFGH",
        verification_uri="https://dash.example/activate",
        verification_uri_complete="https://dash.example/activate?code=ABCD-EFGH",
        expires_at=auth.time.monotonic() + 600,
        interval=0,
    )


def posts(monkeypatch, responses: List[Any]) -> None:
    queue = iter(responses)

    def fake_post(url: str, json: Optional[Dict[str, Any]] = None, timeout: Optional[float] = None):
        item = next(queue)
        if isinstance(item, Exception):
            raise item
        return item

    monkeypatch.setattr(auth.requests, "post", fake_post)


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
