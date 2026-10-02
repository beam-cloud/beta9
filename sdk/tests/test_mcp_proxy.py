"""
The stdio MCP proxy: what a client sees before and after sign-in, how remote
calls are forwarded, and the local deploy/login tools.
"""

import importlib.machinery
import io
import json
import os
import stat
import subprocess
import sys
import textwrap
import time
import types
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


def write_config(path: Path, token: str, gateway_host: str = "gateway.example") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"[default]\ntoken = {token}\ngateway_host = {gateway_host}\ngateway_port = 443\n"
    )


def child_env(**extra: str) -> Dict[str, str]:
    """Environment for a child interpreter that imports this beta9, installed or not."""
    source = str(Path(mcp_tools.__file__).parents[2])
    path = os.pathsep.join(filter(None, [source, os.environ.get("PYTHONPATH")]))
    return {**os.environ, "PYTHONPATH": path, **extra}


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
    context = ConfigContext(token="t")
    monkeypatch.setattr(mcp_server, "context_or_none", lambda name: context)
    monkeypatch.setattr(mcp_server, "RemoteMCP", lambda context: remote)

    proxy = mcp_server.StdioProxy(context_name="prod3", cwd=os.getcwd())
    out = run_proxy(
        proxy,
        rpc("initialize", 1),
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        rpc("tools/list", 2),
        rpc("tools/call", 3, name="whoami", arguments={}),
        [rpc("ping", 4), rpc("ping", 5)],
    )

    init = next(m for m in out if isinstance(m, dict) and m.get("id") == 1)
    tools = next(m for m in out if isinstance(m, dict) and m.get("id") == 2)
    call = next(m for m in out if isinstance(m, dict) and m.get("id") == 3)
    batch = next(m for m in out if isinstance(m, list))
    assert init["result"]["serverInfo"]["name"] == "beta9"
    assert init["result"]["instructions"].startswith("remote says hi")
    # An agent juggling profiles must know which one these tools use.
    assert "acts on context prod3" in init["result"]["instructions"]
    assert "mcp install --context NAME" in init["result"]["instructions"]
    names = [t["name"] for t in tools["result"]["tools"]]
    assert names[0] == "whoami" and "deploy" in names and "login" in names
    assert call["result"]["content"][0]["text"] == "called whoami"
    assert [r["id"] for r in batch] == [4, 5]
    # The notification went to the gateway and produced no output line.
    assert any(m.get("method") == "notifications/initialized" for m in remote.calls)


def test_rejected_token_drops_remote_and_announces_tool_change(settings, monkeypatch):
    remote = FakeRemote(status=401)
    context = ConfigContext(token="t")
    monkeypatch.setattr(mcp_server, "context_or_none", lambda name: context)
    monkeypatch.setattr(mcp_server, "RemoteMCP", lambda context: remote)

    proxy = mcp_server.StdioProxy(cwd=os.getcwd())
    out = run_proxy(proxy, rpc("tools/call", 1, name="whoami", arguments={}), rpc("tools/list", 2))

    notification = next(m for m in out if m.get("method") == "notifications/tools/list_changed")
    assert notification
    call = next(m for m in out if m.get("id") == 1)
    assert call["result"]["isError"] is True
    tools = next(m for m in out if m.get("id") == 2)
    assert "whoami" not in [t["name"] for t in tools["result"]["tools"]]


def test_a_rejected_call_keeps_a_sign_in_saved_while_it_ran(settings, monkeypatch):
    write_config(settings.config_path, "old")
    newer = FakeRemote()

    class Rejected(FakeRemote):
        def call(self, message):
            write_config(settings.config_path, "new")
            proxy._follow_sign_in()  # another request saw the new sign-in meanwhile
            return 401, None

    remotes = {"old": Rejected(), "new": newer}
    monkeypatch.setattr(mcp_server, "RemoteMCP", lambda context: remotes[context.token])
    proxy = mcp_server.StdioProxy(cwd=os.getcwd())
    out = run_proxy(proxy, rpc("tools/call", 1, name="whoami", arguments={}))

    assert next(m for m in out if m.get("id") == 1)["result"]["isError"] is True
    assert proxy.remote is newer


def test_an_unreadable_config_never_fails_the_server(settings, monkeypatch):
    # agents.sh and older CLIs rewrite the file in place; a read can land mid-write.
    monkeypatch.setattr(mcp_server, "RemoteMCP", lambda context: FakeRemote())
    settings.config_path.write_text("[default\ntoken = t")
    proxy = mcp_server.StdioProxy(cwd=os.getcwd())

    def tools():
        [listed] = [m for m in run_proxy(proxy, rpc("tools/list", 1)) if m.get("id") == 1]
        return [t["name"] for t in listed["result"]["tools"]]

    write_config(settings.config_path, "t")
    assert "whoami" in tools()
    settings.config_path.write_text("[default\ntoken = t")
    assert "whoami" in tools()


def test_proxy_acts_as_the_latest_sign_in_saved_anywhere(settings, monkeypatch):
    # `beam login` in a terminal, or another agent's login, while this server runs.
    remotes = []

    def remote(context):
        remotes.append((context.token, FakeRemote()))
        return remotes[-1][1]

    def messages():
        yield rpc("tools/list", 1)
        write_config(settings.config_path, "first")
        yield rpc("tools/list", 2)
        write_config(settings.config_path, "second")
        yield rpc("tools/call", 3, name="whoami", arguments={})

    monkeypatch.setattr(mcp_server, "RemoteMCP", remote)
    proxy = mcp_server.StdioProxy(cwd=os.getcwd())
    stdout = io.BytesIO()
    proxy.run(stdin=(json.dumps(m).encode() + b"\n" for m in messages()), stdout=stdout)
    out = [json.loads(line) for line in stdout.getvalue().decode().splitlines()]

    unsigned, signed = (
        [t["name"] for t in next(m for m in out if m.get("id") == i)["result"]["tools"]]
        for i in (1, 2)
    )
    assert "whoami" not in unsigned and "whoami" in signed
    assert [token for token, _ in remotes] == ["first", "second"]
    assert remotes[1][1].calls[-1]["params"]["name"] == "whoami"
    assert proxy.tools.signed_in_context().token == "second"
    assert [m["method"] for m in out if "method" in m].count(
        "notifications/tools/list_changed"
    ) == 2


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


@pytest.fixture
def local_tools(tmp_path):
    """Signed-in local tools. Request after `settings` or `two_profiles`: the job
    directory follows the settings in force when the tools are built."""
    return mcp_tools.LocalTools(cwd=str(tmp_path), on_login=lambda: None, signed_in=lambda: True)


def test_deploy_tool_runs_cli_in_directory_and_reports_url(
    settings, local_tools, monkeypatch, tmp_path
):
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

    result = local_tools.deploy(
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
    assert body["status"] == "accepted"
    assert body["url"] == "https://x.example" and body["deployment_id"] == "dep-1"
    assert "Deployment accepted for web at https://x.example" in result["content"][0]["text"]
    job = local_tools.jobs[body["job_id"]]
    assert job.directory == str(project)
    assert job.command[1:] == [
        "deploy",
        "--context",
        "default",
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


def test_deploy_tool_maps_empty_ports_to_a_worker(settings, local_tools, monkeypatch, tmp_path):
    cli = fake_cli(tmp_path, 'printf \'{"deployment_id":"d","stub_id":"s","invoke_url":""}\\n\'')
    monkeypatch.setattr(mcp_tools, "_cli_command", lambda: [str(cli)])

    body = local_tools.deploy(
        {"name": "worker", "entrypoint": ["python", "worker.py"], "ports": [], "wait_seconds": 10}
    )
    job = local_tools.jobs[body["structuredContent"]["job_id"]]

    assert "--no-ports" in job.command and "--port" not in job.command
    assert "Deployment accepted for worker (deployment d)" in body["content"][0]["text"]


def test_run_tool_points_at_its_task(settings, local_tools, monkeypatch, tmp_path):
    cli = fake_cli(tmp_path, 'printf \'{"container_id":"c","task_id":"t1","stub_id":"s"}\\n\'')
    monkeypatch.setattr(mcp_tools, "_cli_command", lambda: [str(cli)])

    body = local_tools.run({"name": "once", "entrypoint": ["true"], "wait_seconds": 10})

    assert body["content"][0]["text"].startswith("Task t1 submitted for once. Use get_task")


def test_run_tool_without_a_task_points_at_its_container(
    settings, local_tools, monkeypatch, tmp_path
):
    cli = fake_cli(tmp_path, 'printf \'{"container_id":"c","task_id":"","stub_id":"s"}\\n\'')
    monkeypatch.setattr(mcp_tools, "_cli_command", lambda: [str(cli)])

    body = local_tools.run({"name": "once", "entrypoint": ["true"], "wait_seconds": 10})

    text = body["content"][0]["text"]
    assert text.startswith("Container c submitted for once without a task.")
    assert "logs with container_id" in text and "get_task" not in text


def test_deploy_tool_surfaces_cli_failure(settings, local_tools, monkeypatch, tmp_path):
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

    result = local_tools.deploy({"name": "web", "wait_seconds": 10})

    assert result["isError"] is True
    text = result["content"][0]["text"]
    assert text.startswith(
        "Deploy of web failed: insufficient_credits (purchase credits at https://p)"
    )
    assert result["structuredContent"]["logs"] == ["Syncing files..."]  # JSON kept out of the log
    assert "Syncing files..." not in text  # shown once, with the other fields
    assert json.loads(result["content"][1]["text"]) == result["structuredContent"]


def test_deploy_failure_without_a_json_error_reports_the_last_line(
    settings, local_tools, monkeypatch, tmp_path
):
    # A crash prints a traceback longer than a log page and ends with its cause.
    cli = fake_cli(
        tmp_path,
        """
        for i in $(seq 1 100); do echo "traceback line $i"; done
        echo 'Build failed: build container exited with code 1'
        exit 1
        """,
    )
    monkeypatch.setattr(mcp_tools, "_cli_command", lambda: [str(cli)])

    result = local_tools.deploy({"name": "web", "wait_seconds": 10})

    error = "Build failed: build container exited with code 1"
    assert result["structuredContent"]["error"] == error


@pytest.fixture
def two_profiles(monkeypatch, tmp_path):
    # The serving CLI's settings name one config file (as `beam` does with
    # ~/.beam/config.ini); a fresh interpreter's defaults find another whose
    # `default` context is a different workspace.
    monkeypatch.delenv("CONFIG_PATH", raising=False)
    monkeypatch.delenv("BETA9_TOKEN", raising=False)
    served = tmp_path / "served.ini"
    write_config(served, "served-token", "served.example")
    other = tmp_path / "other.ini"
    write_config(other, "other-token", "other.example")
    set_settings(SDKSettings(name="Beam", config_path=served, api_token=None))
    monkeypatch.setenv("CONFIG_PATH", str(other))
    yield
    set_settings(None)


def test_database_job_hands_its_helper_the_serving_context(two_profiles, local_tools, monkeypatch):
    started = []
    monkeypatch.setattr(mcp_tools.DeployJob, "start", lambda job: started.append(job) or job.save())

    local_tools.create_database_job(
        {"kind": "postgres", "name": "db", "request_key": "k1", "wait_seconds": 0}
    )

    context = json.loads(started[0].env[mcp_tools.JOB_CONTEXT_ENV])
    assert (context["gateway_host"], context["token"]) == ("served.example", "served-token")
    assert "served-token" not in next(local_tools.job_dir.glob("*.json")).read_text()


def test_job_supervisor_passes_its_environment_to_the_helper(two_profiles, local_tools, tmp_path):
    helper = (
        "import json, os; context = json.loads(os.environ['BETA9_MCP_JOB_CONTEXT']);"
        "print(json.dumps({'deployment_id': 'd1', 'token': context['token']}))"
    )
    env = {mcp_tools.JOB_CONTEXT_ENV: json.dumps({"token": "served-token"})}

    view = local_tools.start_command(
        "db", str(tmp_path), [sys.executable, "-c", helper], "k2", 30, env
    )["structuredContent"]

    assert view["status"] == "accepted", view
    assert view["deployment"]["token"] == "served-token"


@pytest.mark.parametrize(
    "kind,check",
    [("postgres", "database_readiness"), ("mysql", "database_credentials and a client connection")],
)
def test_accepted_database_names_its_readiness_check(
    two_profiles, local_tools, tmp_path, kind, check
):
    helper = f"import json; print(json.dumps({{'deployment_id': 'd1', 'kind': '{kind}'}}))"

    result = local_tools.start_command(
        "db", str(tmp_path), [sys.executable, "-c", helper], f"k-{kind}", 30
    )

    assert f"use {check}." in result["content"][0]["text"]


def test_reusing_a_failed_jobs_key_retries_it(two_profiles, local_tools, tmp_path):
    helper = (
        "import json, pathlib, sys; marker = pathlib.Path(sys.argv[1])\n"
        "if not marker.exists(): marker.touch(); print('connection reset'); sys.exit(1)\n"
        "print(json.dumps({'deployment_id': 'd2'}))"
    )
    command = [sys.executable, "-c", helper, str(tmp_path / "failed-once")]

    first = local_tools.start_command("db", str(tmp_path), command, "k-retry", 30)
    second = local_tools.start_command("db", str(tmp_path), command, "k-retry", 30)

    assert first["isError"] is True
    assert second["structuredContent"]["status"] == "accepted"
    assert second["structuredContent"]["job_id"] == first["structuredContent"]["job_id"]
    assert "connection reset" not in second["structuredContent"]["logs"]


def test_reusing_the_key_of_a_failed_job_that_deployed_does_not_redeploy(
    two_profiles, local_tools, tmp_path
):
    runs = tmp_path / "runs"
    helper = (
        "import json, pathlib, sys; runs = pathlib.Path(sys.argv[1])\n"
        "runs.write_text(runs.read_text() + 'x' if runs.exists() else 'x')\n"
        "print(json.dumps({'deployment_id': 'd3'})); print('health check failed'); sys.exit(1)"
    )
    command = [sys.executable, "-c", helper, str(runs)]

    first = local_tools.start_command("app", str(tmp_path), command, "k-deployed", 30)
    second = local_tools.start_command("app", str(tmp_path), command, "k-deployed", 30)

    assert first["isError"] is True and second["isError"] is True
    assert second["structuredContent"]["deployment_id"] == "d3"
    assert runs.read_text() == "x"


def test_listed_jobs_name_their_deployment_without_its_build_log(
    two_profiles, local_tools, tmp_path
):
    helper = "import json; print(json.dumps({'deployment_id': 'd4', 'logs': ['x' * 4096]}))"
    local_tools.start_command("app", str(tmp_path), [sys.executable, "-c", helper], "k-list", 30)

    [job] = local_tools.list_jobs({})["structuredContent"]["items"]

    assert job["deployment_id"] == "d4"
    assert "deployment" not in job


def test_job_results_show_the_same_fields_as_text(two_profiles, local_tools, tmp_path):
    helper = "print('step one'); raise SystemExit(1)"

    result = local_tools.start_command(
        "app", str(tmp_path), [sys.executable, "-c", helper], "k-text", 30
    )

    assert json.loads(result["content"][1]["text"]) == result["structuredContent"]
    assert result["structuredContent"]["logs"] == ["step one"]


def run_database_helper(monkeypatch, refusal: Optional[Dict[str, Any]] = None):
    """`create-database` as a job runner invokes it, in this process: the tokens
    it called the gateway with, and its exit code."""
    calls = []

    def call_remote(context, name, arguments):
        calls.append(context.token)
        if refusal:
            raise mcp_tools.RemoteToolError(refusal)
        return {"deployment_id": "d1"}

    monkeypatch.setattr(mcp_tools, "call_remote", call_remote)
    monkeypatch.setattr(sys, "argv", ["tools", "create-database", "default", '{"name": "db"}'])
    try:
        mcp_tools.main()
    except SystemExit as exc:
        return calls, exc.code
    return calls, 0


def test_database_helper_calls_with_the_handed_context(two_profiles, monkeypatch, capsys):
    refused = {
        "error": "insufficient_credits (workspace 36dc7a, id ws-1)",
        "code": "INSUFFICIENT_CREDITS",
    }
    monkeypatch.setenv(mcp_tools.JOB_CONTEXT_ENV, json.dumps({"token": "served-token"}))

    assert run_database_helper(monkeypatch) == (["served-token"], 0)
    assert json.loads(capsys.readouterr().out) == {"deployment_id": "d1"}
    assert run_database_helper(monkeypatch, refused) == (["served-token"], 1)
    assert json.loads(capsys.readouterr().out) == refused


@pytest.fixture
def home(monkeypatch, tmp_path):
    """What an older server's helper starts with: only a context name, and a HOME
    whose ~/.beam and ~/.beta9 configs are different accounts."""
    write_config(tmp_path / ".beam" / "config.ini", "beam-token")
    write_config(tmp_path / ".beta9" / "config.ini", "beta9-token")
    monkeypatch.setenv("HOME", str(tmp_path))
    for variable in ("CONFIG_PATH", "BEAM_TOKEN", "BETA9_TOKEN", mcp_tools.JOB_CONTEXT_ENV):
        monkeypatch.delenv(variable, raising=False)
    monkeypatch.setattr(sys, "path", list(sys.path))
    monkeypatch.setattr("beta9.config._SETTINGS", None)
    return tmp_path


def test_database_helper_of_an_older_server_uses_the_installed_clis_config(home, monkeypatch):
    beam = types.ModuleType("beam")
    beam.__spec__ = importlib.machinery.ModuleSpec("beam", None)
    monkeypatch.setitem(sys.modules, "beam", beam)

    assert run_database_helper(monkeypatch) == (["beam-token"], 0)


def test_database_helper_does_not_run_a_beam_module_in_the_project(home, monkeypatch):
    # Job runners start the helper in the agent's project; `python -m` puts it on the path.
    project = home / "project"
    project.mkdir()
    (project / "beam.py").write_text("raise SystemExit('the project ran')")
    monkeypatch.chdir(project)
    sys.path.insert(0, str(project))
    # SDKSettings expands its ~/.beta9 default at import, before HOME was replaced.
    monkeypatch.setenv("CONFIG_PATH", str(home / ".beta9" / "config.ini"))

    assert run_database_helper(monkeypatch) == (["beta9-token"], 0)


def test_local_results_show_their_fields_as_text():
    result = mcp_tools.text_result("Review this plan", plan_id="p1")

    texts = [block["text"] for block in result["content"]]
    assert texts[0] == "Review this plan"
    assert json.loads(texts[1]) == {"plan_id": "p1"}
    assert result["structuredContent"] == {"plan_id": "p1"}


def test_database_helper_process_reports_only_its_result(tmp_path):
    # A failed job shows the helper's output; nothing but its JSON belongs there.
    hidden = (mcp_tools.JOB_CONTEXT_ENV, "BETA9_TOKEN", "BEAM_TOKEN")
    env = child_env(HOME=str(tmp_path), CONFIG_PATH=str(tmp_path / "x"))
    helper = subprocess.run(
        [sys.executable, "-m", "beta9.mcp", "create-database", "default", '{"name": "db"}'],
        capture_output=True,
        text=True,
        timeout=60,
        env={k: v for k, v in env.items() if k not in hidden},
    )

    assert helper.returncode == 1
    assert helper.stderr == ""
    assert json.loads(helper.stdout)["code"] == "UNAUTHENTICATED"


def test_jobs_launched_the_way_older_servers_launch_them_still_run(tmp_path):
    # An MCP server started before an upgrade keeps running `python -m beta9.mcp.tools`.
    state = tmp_path / "job.json"
    helper = "import json; print(json.dumps({'deployment_id': 'd1'}))"
    mcp_tools.DeployJob(
        id="j1",
        name="app",
        directory=str(tmp_path),
        command=[sys.executable, "-c", helper],
        state_path=state,
    ).save()

    subprocess.run(
        [sys.executable, "-m", "beta9.mcp.tools", str(state)],
        timeout=60,
        env=child_env(),
        check=True,
    )

    assert mcp_tools.DeployJob.load(state).status == "accepted"


def test_a_job_whose_runner_dies_before_starting_fails_with_its_error(
    settings, local_tools, monkeypatch, tmp_path
):
    runner = fake_cli(tmp_path, "echo 'ModuleNotFoundError: beta9.mcp' >&2; exit 1")
    monkeypatch.setattr(sys, "executable", str(runner))

    result = local_tools.start_command("app", str(tmp_path), ["true"], "k-dead", 10)

    assert result["structuredContent"]["status"] == "failed"
    assert result["structuredContent"]["error"].endswith("ModuleNotFoundError: beta9.mcp")


def test_a_runner_that_never_started_fails_its_job_for_a_server_that_did_not_launch_it(
    settings, local_tools, monkeypatch, tmp_path
):
    command = [sys.executable, "-c", "import json; print(json.dumps({'deployment_id': 'd1'}))"]
    with monkeypatch.context() as launch:
        launch.setattr(mcp_tools.DeployJob, "start", lambda job: job.save())  # never runs
        started = local_tools.start_command("app", str(tmp_path), command, "k-never", 0)
    job_id = started["structuredContent"]["job_id"]
    record = local_tools.job_dir / f"{job_id}.json"

    def restart():  # the server knows only the record, not its supervisor
        local_tools.jobs.clear()
        return local_tools.deploy_status({"job_id": job_id})["structuredContent"]["status"]

    assert restart() == "running"
    launched_long_ago = time.time() - mcp_tools.RUNNER_START_LIMIT - 1
    record.write_text(
        json.dumps({**json.loads(record.read_text()), "started_at": launched_long_ago})
    )
    assert restart() == "failed"
    retried = local_tools.start_command("app", str(tmp_path), command, "k-never", 30)
    assert retried["structuredContent"]["status"] == "accepted"


def test_a_job_stays_visible_after_the_account_changes(settings, local_tools, tmp_path):
    write_config(settings.config_path, "first")
    helper = "import json; print(json.dumps({'deployment_id': 'd1'}))"
    command = [sys.executable, "-c", helper]
    started = local_tools.start_command("app", str(tmp_path), command, "k-switch", 0)
    write_config(settings.config_path, "second")

    job_id = started["structuredContent"]["job_id"]
    view = local_tools.deploy_status({"job_id": job_id, "wait_seconds": 10})
    assert view["structuredContent"]["status"] == "accepted"


def test_deploy_status_returns_new_log_lines_from_cursor(
    settings, local_tools, monkeypatch, tmp_path
):
    cli = fake_cli(
        tmp_path,
        'echo one; echo two; sleep 1; echo three; printf \'{"deployment_id":"d","stub_id":"s","invoke_url":"u"}\\n\'',
    )
    monkeypatch.setattr(mcp_tools, "_cli_command", lambda: [str(cli)])

    first = local_tools.deploy({"name": "web", "wait_seconds": 0})["structuredContent"]
    assert first["status"] == "running"
    seen, view = list(first.get("logs", [])), first
    while view["status"] == "running":
        view = local_tools.deploy_status(
            {"job_id": first["job_id"], "log_cursor": view["log_cursor"], "wait_seconds": 10}
        )["structuredContent"]
        seen += view.get("logs", [])

    assert view["status"] == "accepted" and view["url"] == "u"
    assert "logs" in view  # completion must retain unread log lines
    assert seen == ["one", "two", "three"]
    assert seen == sorted(set(seen), key=seen.index)  # each progress line at most once
    assert all(line in {"one", "two", "three"} for line in seen)  # JSON tail excluded


class FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload
        self.content = json.dumps(payload).encode()

    def json(self):
        return self._payload


def test_remote_retries_only_tools_advertised_as_read_only(monkeypatch):
    remote = object.__new__(mcp_server.RemoteMCP)
    remote.url = "http://gateway.example/api/v1/mcp"
    remote.session = mcp_server.requests.Session()
    remote.read_only_tools = set()
    calls = []

    def post(url, data, timeout):
        message = json.loads(data)
        if message["method"] == "tools/list":
            return FakeResponse(
                200,
                {
                    "result": {
                        "tools": [
                            {"name": "inspect", "annotations": {"readOnlyHint": True}},
                            {"name": "get_or_create", "annotations": {"readOnlyHint": False}},
                        ]
                    }
                },
            )

        calls.append(message["params"]["name"])
        raise mcp_server.requests.ConnectionError("response lost after request")

    monkeypatch.setattr(remote.session, "post", post)
    monkeypatch.setattr(mcp_server.time, "sleep", lambda _: None)
    remote.call(rpc("tools/list"))

    for name, attempts in [("inspect", 3), ("get_or_create", 1), ("get_unknown", 1)]:
        calls.clear()
        with pytest.raises(mcp_server.requests.ConnectionError):
            remote.call(rpc("tools/call", name=name))
        assert calls == [name] * attempts


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


def test_login_tool_reports_denial(settings, local_tools, monkeypatch):
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
    local_tools.login({})
    result = local_tools.login_status({})
    assert result["isError"] is True and "denied" in result["content"][0]["text"]
    assert local_tools.login_flow is None


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


def test_mcp_install_accepts_jsonc_and_blank_configs(settings, tmp_path):
    from beta9.cli import mcp as mcp_cli

    path = tmp_path / "mcp.json"
    path.write_text(
        textwrap.dedent(
            """
            // servers I use, see https://example.com/docs
            {
              "mcpServers": {
                /* keep this one */
                "github": {"command": "npx", "args": ["-y", "gh-mcp", "--flag=//not-a-comment"],},
              },
            }
            """
        )
    )
    client = mcp_cli.AgentClient(id="cursor", label="Cursor", markers=[], config=str(path))
    mcp_cli.install_client(client, ["/opt/bin/beta9", "mcp"])
    data = json.loads(path.read_text())
    assert data["mcpServers"]["github"]["args"] == ["-y", "gh-mcp", "--flag=//not-a-comment"]
    assert data["mcpServers"]["beta9"] == {"command": "/opt/bin/beta9", "args": ["mcp"]}
    assert mcp_cli.configured(client)

    path.write_text("\n\n")
    mcp_cli.install_client(client, ["/opt/bin/beta9", "mcp"])
    assert "beta9" in json.loads(path.read_text())["mcpServers"]


def test_mcp_install_isolates_a_broken_config_to_its_client(settings, tmp_path):
    from beta9.cli import mcp as mcp_cli

    broken = tmp_path / "mcp.json"
    broken.write_text('{\n  "mcpServers": {\n    "x": {"command": }\n  }\n}\n')
    cursor = mcp_cli.AgentClient(id="cursor", label="Cursor", markers=[], config=str(broken))
    codex = mcp_cli.AgentClient(
        id="codex", label="Codex", markers=[], config=str(tmp_path / "config.toml")
    )

    written, failed = mcp_cli.install_clients([cursor, codex], ["/opt/bin/beta9", "mcp"])

    assert list(written) == ["codex"]
    assert "[mcp_servers.beta9]" in (tmp_path / "config.toml").read_text()
    assert list(failed) == ["cursor"]
    assert failed["cursor"].startswith(f"{broken} is not valid JSON (line 3, column ")
    assert "Expecting value" in failed["cursor"]
    assert broken.read_text().count("\n") == 5
    assert not mcp_cli.configured(cursor)


def test_cli_path_prefers_the_running_entry_point(settings, monkeypatch, tmp_path):
    from beta9 import config

    own = tmp_path / "bin" / "beta9"
    own.parent.mkdir()
    own.write_text("#!/usr/bin/env python3\n")
    monkeypatch.setattr(config.shutil, "which", lambda name: "/old/bin/beta9")

    monkeypatch.setattr(sys, "argv", [str(own), "setup", "agent"])
    assert config.cli_path() == str(own)

    monkeypatch.setattr(sys, "argv", ["/usr/bin/python3", "-m", "beta9"])
    assert config.cli_path() == "/old/bin/beta9"


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
