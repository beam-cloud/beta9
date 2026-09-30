"""
Browser sign-in for the CLI: the OAuth 2.0 device grant (RFC 8628) against the
authorization server of the target context (`ConfigContext.auth_url`; the
settings' for the default one). The issued token is a workspace token, so the
saved context is the one `config create` would write.
"""

import os
import socket
import sys
import time
import webbrowser
from dataclasses import dataclass, replace
from typing import Any, Dict, Optional

import requests

from .config import (
    DEFAULT_CONTEXT_NAME,
    DEFAULT_GATEWAY_PORT,
    ConfigContext,
    context_defaults,
    get_settings,
    load_config,
    save_config,
)

GRANT_TYPE = "urn:ietf:params:oauth:grant-type:device_code"
REQUEST_TIMEOUT = 15
# Consecutive polls that may fail to reach the service (network errors, 5xx)
# before the sign-in is given up; one blip must not end a ten-minute wait.
MAX_TRANSIENT_FAILURES = 5


class LoginError(Exception):
    def __init__(self, message: str, code: str = "LOGIN_FAILED"):
        super().__init__(message)
        self.code: str = code


def login_configured(name: str = DEFAULT_CONTEXT_NAME) -> bool:
    return bool(context_defaults(name).auth_url)


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


def _post(auth_url: Optional[str], path: str, payload: Dict[str, str]) -> requests.Response:
    if not auth_url:
        raise LoginError(
            "This install has no browser sign-in; create a token in the dashboard and run `config create`.",
            "LOGIN_NOT_CONFIGURED",
        )
    try:
        return requests.post(
            f"{auth_url.rstrip('/')}/{path}", json=payload, timeout=REQUEST_TIMEOUT
        )
    except requests.RequestException as exc:
        raise LoginError(f"Could not reach the sign-in service: {exc}", "AUTH_UNAVAILABLE")


def _json(response: requests.Response) -> Dict[str, Any]:
    """The JSON object in a response; {} for an empty or non-JSON body (an HTML 502, say)."""
    try:
        data = response.json()
    except ValueError:
        return {}
    return data if isinstance(data, dict) else {}


def _message(response: requests.Response, fallback: str) -> str:
    data = _json(response)
    if not data:
        return f"{fallback} (HTTP {response.status_code})"
    return data.get("error_description") or data.get("message") or data.get("error") or fallback


@dataclass
class DeviceLogin:
    target: ConfigContext  # where the token will be used; its auth_url issues it
    device_code: str
    user_code: str
    verification_uri: str
    verification_uri_complete: str
    expires_at: float
    interval: int
    workspace_name: str = ""  # set once approved
    failures: int = 0  # consecutive polls that did not reach the service

    @classmethod
    def start(cls, target: Optional[ConfigContext] = None) -> "DeviceLogin":
        target = target or context_defaults()
        response = _post(target.auth_url, "device/code", {"client_name": client_name()})
        data = _json(response)
        if response.status_code >= 400 or not data.get("device_code"):
            raise LoginError(_message(response, "Could not start sign-in"), "AUTH_UNAVAILABLE")
        return cls(
            target=target,
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
        """The context once approved; None while pending or while the service is briefly unreachable."""
        try:
            response = _post(
                self.target.auth_url,
                "token",
                {"grant_type": GRANT_TYPE, "device_code": self.device_code},
            )
        except LoginError as exc:
            if exc.code != "AUTH_UNAVAILABLE":
                raise
            return self._transient(str(exc))
        if response.status_code >= 500:
            return self._transient(_message(response, "The sign-in service is unavailable"))
        self.failures = 0

        data = _json(response)
        if response.status_code == 200 and data.get("access_token"):
            self.workspace_name = data.get("workspace_name", "")
            return replace(
                self.target,
                token=data["access_token"],
                gateway_host=data.get("gateway_host") or self.target.gateway_host,
                gateway_port=int(
                    data.get("gateway_port") or self.target.gateway_port or DEFAULT_GATEWAY_PORT
                ),
                api_url=data.get("api_url") or self.target.api_url,
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

    def _transient(self, message: str) -> None:
        self.failures += 1
        if self.failures >= MAX_TRANSIENT_FAILURES:
            raise LoginError(message, "AUTH_UNAVAILABLE")
        return None

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
