import datetime
import sys
from pathlib import Path
from types import SimpleNamespace
from types import ModuleType

import click
import pytest
from click.testing import CliRunner

from beta9.cli import database as database_cli
from beta9.cli import container as container_cli
from beta9.cli import extraclick
from beta9.cli import run as run_cli
from beta9.cli.main import load_cli
from beta9.config import SDKSettings


def test_disk_management_commands_registered():
    cli = load_cli(check_config=False)

    disk = cli.management_group.get_command(None, "disk")
    assert disk is not None
    assert sorted(disk.commands) == ["create", "delete", "list", "snapshots"]

    create_options = {param.name for param in disk.commands["create"].params}
    assert {"name", "size", "filesystem", "mount_path", "format"} <= create_options
    assert "format" in {param.name for param in disk.commands["list"].params}
    assert "format" in {param.name for param in disk.commands["snapshots"].params}
    assert "yes" in {param.name for param in disk.commands["delete"].params}


def test_container_uptime_ignores_unset_timestamp():
    now = datetime.datetime(2026, 7, 18, tzinfo=datetime.timezone.utc)
    epoch = datetime.datetime.fromtimestamp(0, tz=datetime.timezone.utc)

    assert container_cli._format_uptime(None, now) == "N/A"
    assert container_cli._format_uptime(epoch, now) == "N/A"
    assert container_cli._format_uptime(now - datetime.timedelta(seconds=5), now) == "5 seconds"


def test_database_commands_are_nested_under_db():
    cli = load_cli(check_config=False)

    assert cli.common_group.get_command(None, "postgres") is None
    assert cli.common_group.get_command(None, "redis") is None

    db = cli.common_group.get_command(None, "db")
    assert db is not None
    assert sorted(db.commands) == ["list", "mongo", "mysql", "postgres", "redis"]
    assert "list" in db.commands
    assert "create" in db.commands["postgres"].commands
    assert "create" in db.commands["redis"].commands


def test_database_commands_do_not_expose_unenforced_disk_size():
    cli = load_cli(check_config=False)
    db = cli.common_group.get_command(None, "db")

    for product in ("postgres", "redis"):
        for command in ("create", "scale"):
            options = {param.name for param in db.commands[product].commands[command].params}
            assert "size" not in options


def test_reserved_hardware_dx_commands_are_registered():
    cli = load_cli(check_config=False)

    run = cli.common_group.get_command(None, "run")
    assert {"detach", "machine_id"} <= {param.name for param in run.params}

    deploy = cli.common_group.get_command(None, "deploy")
    assert "replicas" in {param.name for param in deploy.params}

    machine = cli.management_group.get_command(None, "machine")
    assert "ssh" in machine.commands

    container = cli.management_group.get_command(None, "container")
    assert "machine_id" in {param.name for param in container.commands["list"].params}


def test_beam_settings_honor_config_path(monkeypatch, tmp_path):
    config_path = tmp_path / "beam-config.ini"
    monkeypatch.setenv("CONFIG_PATH", str(config_path))
    monkeypatch.setitem(sys.modules, "beam", ModuleType("beam"))

    settings = SDKSettings()

    assert settings.config_path == Path(config_path)


def test_run_runtime_prepare_failure_exits_nonzero(monkeypatch):
    class FakeServiceClient:
        def __init__(self, _config):
            self.channel = object()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class FakePod:
        def __init__(self, entrypoint=None):
            self.entrypoint = entrypoint

        def create(self, machine_id=""):
            return SimpleNamespace(ok=False, error_msg="Failed to prepare runtime")

    monkeypatch.setattr(extraclick, "ServiceClient", FakeServiceClient)
    monkeypatch.setattr(extraclick, "get_config_context", lambda _context: SimpleNamespace())
    monkeypatch.setattr(run_cli, "Pod", FakePod)

    result = CliRunner().invoke(run_cli.common, ["run", "--entrypoint", "echo hi"])

    assert result.exit_code == 1


def test_database_kinds_share_one_command_set():
    cli = load_cli(check_config=False)
    db = cli.common_group.get_command(None, "db")

    verbs = {"create", "credentials", "secrets", "status", "rotate", "delete", "scale"}
    for kind in ("postgres", "redis", "mysql", "mongo"):
        assert verbs <= set(db.commands[kind].commands), kind
    assert "connect" in db.commands["postgres"].commands
    assert "connect" in db.commands["redis"].commands


def test_database_create_sends_gateway_request(monkeypatch):
    calls = []
    monkeypatch.setattr(
        database_cli,
        "_api",
        lambda service, method, path="", **kwargs: (
            calls.append((method, path, kwargs))
            or {"name": "app-db", "kind": "postgres", "host": "h:443"}
        ),
    )

    class FakeServiceClient:
        def __init__(self, _config):
            self.channel = object()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(extraclick, "ServiceClient", FakeServiceClient)
    monkeypatch.setattr(extraclick, "get_config_context", lambda _context: SimpleNamespace())

    result = CliRunner().invoke(
        database_cli.common,
        [
            "db",
            "postgres",
            "create",
            "app-db",
            "--memory",
            "2Gi",
            "--cpu",
            "0.5",
            "--min-replicas",
            "1",
        ],
    )

    assert result.exit_code == 0, result.output
    method, path, kwargs = calls[0]
    assert (method, path) == ("POST", "")
    assert kwargs["json"] == {
        "kind": "postgres",
        "name": "app-db",
        "username": "",
        "password": "",
        "database": "",
        "pool": "",
        "always_on": True,
        "cpu": 500,
        "memory": 2048,
    }
    assert "h:443" in result.output


def test_database_scale_with_resources_redeploys_the_stub(monkeypatch):
    monkeypatch.setattr(
        database_cli,
        "_service_info",
        lambda service, kind, name: {"stub_id": "stub-1", "deployment_id": "dep-1"},
    )
    captured = {}

    def fake_redeploy(service, name, stub_id, mutate):
        config = {"runtime": {"cpu": 1000, "memory": 512}, "autoscaler": {"min_containers": 0}}
        mutate(config)
        captured.update(stub_id=stub_id, config=config)
        return {"deployment_id": "dep-2", "version": 2}

    monkeypatch.setattr(database_cli.stubconfig, "redeploy_with_config", fake_redeploy)

    database_cli._scale(None, "redis", "cache", True, False, None, "1Gi", "private", "json")

    assert captured["stub_id"] == "stub-1"
    assert captured["config"]["runtime"] == {"cpu": 1000, "memory": 1024}
    assert captured["config"]["pool"] == {"name": "private"}
    assert captured["config"]["autoscaler"]["min_containers"] == 1


def test_database_password_sources_are_exclusive(monkeypatch):
    monkeypatch.setenv("DB_PASS", "from-env")
    assert database_cli._password("", "DB_PASS", False) == "from-env"
    assert database_cli._password("", "", False) == ""
    with pytest.raises(click.ClickException):
        database_cli._password("x", "DB_PASS", False)
    assert database_cli._memory_mb("2Gi") == 2048
    assert database_cli._memory_mb("512") == 512
    assert database_cli._memory_mb(None) == 0


def test_stub_request_round_trips_bindings_and_entrypoint():
    from beta9.cli.stubconfig import stub_request_from_config

    stub = {
        "type": "pod/deployment",
        "name": "pod",
        "object": {"external_id": "obj"},
        "app": {"name": "api"},
    }
    config = {
        "entry_point": ["sh", "-lc", "exec app"],
        "env": ["PORT=8080"],
        "secrets": [
            {"name": "BETA9_POSTGRES_DB_URL", "env_name": "DATABASE_URL"},
            {"name": "HF_TOKEN"},
        ],
    }

    request = stub_request_from_config(stub, config)

    assert request["entrypoint"] == ["sh", "-lc", "exec app"]
    assert request["env"] == ["DATABASE_URL=${{secret.BETA9_POSTGRES_DB_URL}}", "PORT=8080"]
    assert request["secrets"] == [{"name": "HF_TOKEN"}]
