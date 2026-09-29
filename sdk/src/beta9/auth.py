"""
Browser sign-in for the CLI: the OAuth 2.0 device grant (RFC 8628) against the
authorization server in `SDKSettings.auth_url`. The issued token is a
workspace token, so the saved context is the one `config create` would write.
"""

import os
import socket
import sys
import time
import webbrowser
from dataclasses import dataclass
from typing import Dict, Optional

import requests

from .config import DEFAULT_CONTEXT_NAME, ConfigContext, get_settings, load_config, save_config

GRANT_TYPE = "urn:ietf:params:oauth:grant-type:device_code"
REQUEST_TIMEOUT = 15


class LoginError(Exception):
    def __init__(self, message: str, code: str = "LOGIN_FAILED"):
        super().__init__(message)
        self.code: str = code


def login_configured() -> bool:
    return bool(get_settings().auth_url)


def client_name() -> str:
    host = socket.gethostname().split(".")[0] or "this machine"
    return f"{get_settings().name.lower()} CLI on {host}"


def has_browser() -> bool:
    if os.getenv("CI") or os.getenv("SSH_CONNECTION") or os.getenv("SSH_TTY"):
        return False
    if sys.platform.startswith("linux") and not (
        os.getenv("DISPLAY") or os.getenv("WAYLAND_DISPLAY")
    ):
        return False
    return True


def save_login(context: ConfigContext, name: str = DEFAULT_CONTEXT_NAME) -> None:
    contexts = load_config()
    contexts[name] = context
    save_config(contexts)


def _post(path: str, payload: Dict[str, str]) -> requests.Response:
    settings = get_settings()
    if not settings.auth_url:
        raise LoginError(
            "This install has no browser sign-in; create a token in the dashboard and run `config create`.",
            "LOGIN_NOT_CONFIGURED",
        )
    try:
        return requests.post(
            f"{settings.auth_url.rstrip('/')}/{path}", json=payload, timeout=REQUEST_TIMEOUT
        )
    except requests.RequestException as exc:
        raise LoginError(f"Could not reach the sign-in service: {exc}", "AUTH_UNAVAILABLE")


def _message(response: requests.Response, fallback: str) -> str:
    try:
        data = response.json()
    except ValueError:
        return f"{fallback} (HTTP {response.status_code})"
    return data.get("error_description") or data.get("message") or data.get("error") or fallback


@dataclass
class DeviceLogin:
    device_code: str
    user_code: str
    verification_uri: str
    verification_uri_complete: str
    expires_at: float
    interval: int
    workspace_name: str = ""  # set once approved

    @classmethod
    def start(cls) -> "DeviceLogin":
        response = _post("device/code", {"client_name": client_name()})
        if response.status_code >= 400:
            raise LoginError(_message(response, "Could not start sign-in"), "AUTH_UNAVAILABLE")
        data = response.json()
        return cls(
            device_code=data["device_code"],
            user_code=data["user_code"],
            verification_uri=data["verification_uri"],
            verification_uri_complete=data.get("verification_uri_complete")
            or data["verification_uri"],
            expires_at=time.monotonic() + float(data.get("expires_in", 600)),
            interval=int(data.get("interval", 3)),
        )

    def open_browser(self) -> bool:
        if not has_browser():
            return False
        try:
            return webbrowser.open(self.verification_uri_complete, new=2)
        except Exception:
            return False

    def poll(self) -> Optional[ConfigContext]:
        """The context once approved; None while pending."""
        response = _post("token", {"grant_type": GRANT_TYPE, "device_code": self.device_code})
        data = response.json() if response.content else {}
        if response.status_code == 200:
            settings = get_settings()
            self.workspace_name = data.get("workspace_name", "")
            return ConfigContext(
                token=data["access_token"],
                gateway_host=data.get("gateway_host") or settings.gateway_host,
                gateway_port=int(data.get("gateway_port") or settings.gateway_port),
                api_url=data.get("api_url") or settings.api_url,
            )
        error = data.get("error", "")
        if error == "authorization_pending":
            return None
        if error == "slow_down":
            self.interval += 2
            return None
        if error == "expired_token":
            raise LoginError("The sign-in code expired; run login again.", "LOGIN_EXPIRED")
        if error == "access_denied":
            raise LoginError("Sign-in was denied in the browser.", "LOGIN_DENIED")
        raise LoginError(_message(response, "Sign-in failed"))

    def wait(self, timeout: Optional[float] = None) -> ConfigContext:
        deadline = (
            self.expires_at if timeout is None else min(self.expires_at, time.monotonic() + timeout)
        )
        while True:
            context = self.poll()
            if context is not None:
                return context
            if time.monotonic() + self.interval > deadline:
                raise LoginError("Timed out waiting for sign-in; run login again.", "LOGIN_EXPIRED")
            time.sleep(self.interval)
