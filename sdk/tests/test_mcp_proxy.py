"""
The stdio MCP proxy: what a client sees before and after sign-in, how remote
calls are forwarded, and the local deploy/login tools.
"""

import io
import json
import os
import stat
import sys
import textwrap
from typing import Any, Dict, Optional
from pathlib import Path

import pytest

from beta9 import auth
from beta9.config import ConfigContext, SDKSettings, load_config, set_settings
from beta9.mcp import server as mcp_server
from beta9.mcp import tools as mcp_tools


@pytest.fixture
def settings(monkeypatch, tmp_path):
    config_path = tmp_path / "config.ini"
    monkeypatch.setenv("CONFIG_PATH", str(config_path))
    monkeypatch.delenv("BETA9_TOKEN", raising=False)
    s = SDKSettings(
        name="Beta9",
        config_path=config_path,
        api_token=None,
        auth_url="https://auth.example/oauth",
    )
    set_settings(s)
    yield s
    set_settings(None)


def run_proxy(proxy: mcp_server.StdioProxy, *messages):
    stdin = io.BytesIO("".join(json.dumps(m) + "\n" for m in messages).encode())
    stdout = io.BytesIO()
    proxy.run(stdin=stdin, stdout=stdout)
    return [json.loads(line) for line in stdout.getvalue().decode().splitlines() if line]


def rpc(method, msg_id=1, **params):
    message = {"jsonrpc": "2.0", "id": msg_id, "method": method}
    if params:
        message["params"] = params
    return message


class FakeRemote:
    """A gateway MCP endpoint: one POST per message, 202 for notifications."""

    def __init__(self, status=200):
        self.calls = []
        self.status = status

    def call(self, message):
        self.calls.append(message)
        if self.status != 200:
            return self.status, None
        if "id" not in message or message["id"] is None:
            return 202, None
        method = message["method"]
        if method == "initialize":
            result = {
                "protocolVersion": "2025-03-26",
                "capabilities": {"tools": {}},
                "instructions": "remote says hi",
            }
        elif method == "tools/list":
            result = {
                "tools": [{"name": "whoami", "description": "", "inputSchema": {"type": "object"}}]
            }
        elif method == "tools/call":
            result = {"content": [{"type": "text", "text": f"called {message['params']['name']}"}]}
        else:
            return 200, {
                "jsonrpc": "2.0",
                "id": message["id"],
                "error": {"code": -32601, "message": "nope"},
            }
        return 200, {"jsonrpc": "2.0", "id": message["id"], "result": result}


def test_unauthenticated_proxy_offers_login_only(settings):
    proxy = mcp_server.StdioProxy(cwd=os.getcwd())
    out = run_proxy(
        proxy,
        rpc("initialize", 1),
        rpc("tools/list", 2),
        rpc("tools/call", 3, name="whoami", arguments={}),
    )

    init, tools, call = out
    assert "Not signed in" in init["result"]["instructions"]
    assert init["result"]["capabilities"]["tools"]["listChanged"] is True
    assert sorted(t["name"] for t in tools["result"]["tools"]) == ["login", "login_status"]
    assert call["result"]["isError"] is True
    assert "login" in call["result"]["content"][0]["text"]


def test_unauthenticated_proxy_without_login_points_at_config_create(settings):
    settings.auth_url = ""
    proxy = mcp_server.StdioProxy(cwd=os.getcwd())
    init, tools = run_proxy(proxy, rpc("initialize", 1), rpc("tools/list", 2))
    assert "config create" in init["result"]["instructions"]
    assert tools["result"]["tools"] == []


def test_authenticated_proxy_merges_remote_and_local_tools(settings, monkeypatch):
    remote = FakeRemote()
    monkeypatch.setattr(mcp_server, "context_or_none", lambda name: object())
    monkeypatch.setattr(mcp_server, "RemoteMCP", lambda context: remote)

    proxy = mcp_server.StdioProxy(cwd=os.getcwd())
    out = run_proxy(
        proxy,
        rpc("initialize", 1),
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        rpc("tools/list", 2),
        rpc("tools/call", 3, name="whoami", arguments={}),
        [rpc("ping", 4), rpc("ping", 5)],
    )

    init, tools, call, batch = out
    assert init["result"]["serverInfo"]["name"] == "beta9"
    assert init["result"]["instructions"].startswith("remote says hi")
    names = [t["name"] for t in tools["result"]["tools"]]
    assert names[0] == "whoami" and "deploy" in names and "login" in names
    assert call["result"]["content"][0]["text"] == "called whoami"
    assert [r["id"] for r in batch] == [4, 5]
    # The notification went to the gateway and produced no output line.
    assert any(m.get("method") == "notifications/initialized" for m in remote.calls)


def test_rejected_token_drops_remote_and_announces_tool_change(settings, monkeypatch):
    remote = FakeRemote(status=401)
    monkeypatch.setattr(mcp_server, "context_or_none", lambda name: object())
    monkeypatch.setattr(mcp_server, "RemoteMCP", lambda context: remote)

    proxy = mcp_server.StdioProxy(cwd=os.getcwd())
    out = run_proxy(proxy, rpc("tools/call", 1, name="whoami", arguments={}), rpc("tools/list", 2))

    notification = next(m for m in out if m.get("method") == "notifications/tools/list_changed")
    assert notification
    call = next(m for m in out if m.get("id") == 1)
    assert call["result"]["isError"] is True
    tools = next(m for m in out if m.get("id") == 2)
    assert "whoami" not in [t["name"] for t in tools["result"]["tools"]]


def test_parse_error_is_reported_not_fatal(settings):
    proxy = mcp_server.StdioProxy(cwd=os.getcwd())
    stdout = io.BytesIO()
    proxy.run(
        stdin=io.BytesIO(b"not json\n" + json.dumps(rpc("ping", 1)).encode() + b"\n"), stdout=stdout
    )
    first, second = [json.loads(line) for line in stdout.getvalue().decode().splitlines()]
    assert first["error"]["code"] == -32700
    assert second["result"] == {}


def fake_cli(tmp_path: Path, script: str) -> Path:
    path = tmp_path / "fake-cli"
    path.write_text("#!/bin/sh\n" + textwrap.dedent(script))
    path.chmod(path.stat().st_mode | stat.S_IEXEC)
    return path


def test_deploy_tool_runs_cli_in_directory_and_reports_url(settings, monkeypatch, tmp_path):
    project = tmp_path / "project"
    project.mkdir()
    cli = fake_cli(
        tmp_path,
        """
        echo "=> Building image"
        echo "args: $*" >&2
        printf '{"deployment_id": "dep-1", "stub_id": "stub-1", "invoke_url": "https://x.example", "version": 3}\\n'
        """,
    )
    monkeypatch.setattr(mcp_tools, "_cli_command", lambda: [str(cli)])

    tools = mcp_tools.LocalTools(cwd=str(tmp_path), on_login=lambda: None, signed_in=lambda: True)
    result = tools.deploy(
        {
            "name": "web",
            "directory": str(project),
            "dockerfile": "Dockerfile",
            "ports": [8000, 9000],
            "env": {"DATABASE_URL": "${{db.web-db.DATABASE_URL}}"},
            "secrets": ["A", "B"],
            "disks": ["data:/data:5Gi"],
            "tcp": True,
            "wait_seconds": 10,
        }
    )

    body = result["structuredContent"]
    assert body["status"] == "deployed"
    assert body["url"] == "https://x.example" and body["deployment_id"] == "dep-1"
    assert "Deployed web at https://x.example" in result["content"][0]["text"]
    job = tools.jobs[body["job_id"]]
    assert job.directory == str(project)
    assert job.command[1:] == [
        "deploy",
        "--json",
        "--name",
        "web",
        "--dockerfile",
        "Dockerfile",
        "--port",
        "8000",
        "--port",
        "9000",
        "--env",
        "DATABASE_URL=${{db.web-db.DATABASE_URL}}",
        "--secrets",
        "A,B",
        "--disk",
        "data:/data:5Gi",
        "--tcp",
    ]


def test_deploy_tool_maps_empty_ports_to_a_worker(settings, monkeypatch, tmp_path):
    cli = fake_cli(tmp_path, 'printf \'{"deployment_id":"d","stub_id":"s","invoke_url":""}\\n\'')
    monkeypatch.setattr(mcp_tools, "_cli_command", lambda: [str(cli)])
    tools = mcp_tools.LocalTools(cwd=str(tmp_path), on_login=lambda: None, signed_in=lambda: True)

    body = tools.deploy(
        {"name": "worker", "entrypoint": ["python", "worker.py"], "ports": [], "wait_seconds": 10}
    )
    job = tools.jobs[body["structuredContent"]["job_id"]]

    assert "--no-ports" in job.command and "--port" not in job.command
    assert "Deployed worker (deployment d)" in body["content"][0]["text"]


def test_deploy_tool_surfaces_cli_failure(settings, monkeypatch, tmp_path):
    # Machine mode prints the cause as a pretty-printed object, then a generic one.
    cli = fake_cli(
        tmp_path,
        """
        echo 'Syncing files...'
        printf '{\\n  "error": "insufficient_credits",\\n  "code": "ERROR",\\n  "hint": "purchase credits at https://p"\\n}\\n'
        printf '{\\n  "error": "Deployment failed",\\n  "code": "ERROR"\\n}\\n'
        exit 1
        """,
    )
    monkeypatch.setattr(mcp_tools, "_cli_command", lambda: [str(cli)])
    tools = mcp_tools.LocalTools(cwd=str(tmp_path), on_login=lambda: None, signed_in=lambda: True)

    result = tools.deploy({"name": "web", "wait_seconds": 10})

    assert result["isError"] is True
    text = result["content"][0]["text"]
    assert text.startswith(
        "Deploy of web failed: insufficient_credits (purchase credits at https://p)"
    )
    assert result["structuredContent"]["logs"] == ["Syncing files..."]  # JSON kept out of the log


def test_deploy_status_returns_new_log_lines_from_cursor(settings, monkeypatch, tmp_path):
    cli = fake_cli(
        tmp_path,
        'echo one; echo two; sleep 1; echo three; printf \'{"deployment_id":"d","stub_id":"s","invoke_url":"u"}\\n\'',
    )
    monkeypatch.setattr(mcp_tools, "_cli_command", lambda: [str(cli)])
    tools = mcp_tools.LocalTools(cwd=str(tmp_path), on_login=lambda: None, signed_in=lambda: True)

    first = tools.deploy({"name": "web", "wait_seconds": 0})["structuredContent"]
    assert first["status"] == "running"
    seen, view = list(first.get("logs", [])), first
    while view["status"] == "running":
        view = tools.deploy_status(
            {"job_id": first["job_id"], "log_cursor": view["log_cursor"], "wait_seconds": 10}
        )["structuredContent"]
        seen += view.get("logs", [])

    assert view["status"] == "deployed" and view["url"] == "u"
    assert "logs" not in view  # the build log is dropped once deployed
    assert seen == sorted(set(seen), key=seen.index)  # each progress line at most once
    assert all(line in {"one", "two", "three"} for line in seen)  # JSON tail excluded


class FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload
        self.content = json.dumps(payload).encode()

    def json(self):
        return self._payload


def test_login_tool_drives_device_flow_and_saves_context(settings, monkeypatch, tmp_path):
    polls = iter(
        [
            FakeResponse(400, {"error": "authorization_pending"}),
            FakeResponse(200, {"access_token": "t" * 64, "token_type": "bearer"}),
        ]
    )

    def fake_post(url: str, json: Optional[Dict[str, Any]] = None, timeout: Optional[float] = None):
        json = json or {}
        assert url.startswith(
            "https://auth.stage.example/oauth/"
        )  # staging's server, not default's
        if url.endswith("/device/code"):
            assert json["client_name"].startswith("beta9 CLI on")
            return FakeResponse(
                200,
                {
                    "device_code": "dev-1",
                    "user_code": "ABCD-EFGH",
                    "verification_uri": "https://dash.example/activate",
                    "verification_uri_complete": "https://dash.example/activate?code=ABCD-EFGH",
                    "expires_in": 600,
                    "interval": 0,
                },
            )
        assert json == {"grant_type": auth.GRANT_TYPE, "device_code": "dev-1"}
        return next(polls)

    monkeypatch.setattr(auth.requests, "post", fake_post)
    monkeypatch.setattr(auth, "has_browser", lambda: False)
    # The proxy serves `mcp --context staging`, a built-in environment nobody has
    # signed in to on this machine yet; the default context belongs to someone else.
    settings.environments["staging"] = ConfigContext(
        gateway_host="gw.stage.example",
        gateway_port=443,
        auth_url="https://auth.stage.example/oauth",
    )
    (tmp_path / "config.ini").write_text("[default]\ntoken = keep-me\n")
    events = []
    tools = mcp_tools.LocalTools(
        cwd=str(tmp_path),
        on_login=lambda: events.append("login"),
        signed_in=lambda: False,
        context_name="staging",
    )

    started = tools.login({})
    assert started["structuredContent"]["user_code"] == "ABCD-EFGH"
    assert "https://dash.example/activate?code=ABCD-EFGH" in started["content"][0]["text"]

    status = tools.login_status({"wait_seconds": 5})
    assert status["structuredContent"]["status"] == "signed_in"
    assert events == ["login"]
    contexts = load_config()
    assert contexts["staging"].token == "t" * 64
    assert contexts["staging"].gateway_host == "gw.stage.example"
    assert contexts["staging"].auth_url == "https://auth.stage.example/oauth"
    assert contexts["default"].token == "keep-me"


def test_login_tool_reports_denial(settings, monkeypatch, tmp_path):
    def fake_post(url, json=None, timeout=None):
        if url.endswith("/device/code"):
            return FakeResponse(
                200,
                {
                    "device_code": "d",
                    "user_code": "X",
                    "verification_uri": "u",
                    "expires_in": 600,
                    "interval": 0,
                },
            )
        return FakeResponse(400, {"error": "access_denied"})

    monkeypatch.setattr(auth.requests, "post", fake_post)
    monkeypatch.setattr(auth, "has_browser", lambda: False)
    tools = mcp_tools.LocalTools(cwd=str(tmp_path), on_login=lambda: None, signed_in=lambda: True)
    tools.login({})
    result = tools.login_status({})
    assert result["isError"] is True and "denied" in result["content"][0]["text"]
    assert tools.login_flow is None


@pytest.mark.skipif(sys.platform == "win32", reason="posix paths")
def test_mcp_install_writes_tokenless_config_for_each_client(settings, monkeypatch, tmp_path):
    from beta9.cli import mcp as mcp_cli

    home = tmp_path / "home"
    (home / ".cursor").mkdir(parents=True)
    (home / ".codex").mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setattr(
        mcp_cli.shutil, "which", lambda name: "/opt/bin/beta9" if name == "beta9" else None
    )

    command = mcp_cli.server_command(None)
    assert command == ["/opt/bin/beta9", "mcp"]
    assert [c.id for c in mcp_cli.detected_clients()] == ["cursor", "codex"]

    for client in mcp_cli.detected_clients():
        mcp_cli.install_client(client, command)
        mcp_cli.install_client(client, command)  # idempotent

    cursor = json.loads((home / ".cursor" / "mcp.json").read_text())
    assert cursor["mcpServers"]["beta9"] == {"command": "/opt/bin/beta9", "args": ["mcp"]}
    assert "Authorization" not in (home / ".cursor" / "mcp.json").read_text()
    codex = (home / ".codex" / "config.toml").read_text()
    assert codex.count("[mcp_servers.beta9]") == 1
    assert 'command = "/opt/bin/beta9"' in codex and 'args = ["mcp"]' in codex
    assert all(mcp_cli.configured(c) for c in mcp_cli.detected_clients())

    # A non-default context is passed through to the server command.
    assert mcp_cli.server_command("staging")[-2:] == ["--context", "staging"]


def test_skill_installs_rendered_for_cli_name(settings, tmp_path):
    from beta9.skills import install_skill

    target = install_skill(tmp_path / "skills")
    assert target.name == "use-beta9"
    rendered = {p.name: p.read_text() for p in target.rglob("*.md")}
    assert rendered["SKILL.md"].startswith("---\nname: use-beta9")
    for text in rendered.values():
        for placeholder in (
            "{{cli}}",
            "{{product}}",
            "{{skill}}",
            "{{login_hint}}",
            "{{dashboard_url}}",
            "{{docs_url}}",
        ):
            assert placeholder not in text
    assert "`beta9 login`" in rendered["SKILL.md"]
    # Platform reference syntax survives rendering untouched.
    assert "${{db.<name>.DATABASE_URL}}" in rendered["SKILL.md"]
    assert {"deploy.md", "databases.md", "operate.md", "setup.md"} <= set(rendered)
