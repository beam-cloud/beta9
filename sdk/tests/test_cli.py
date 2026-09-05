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
    assert sorted(db.commands) == ["list", "postgres", "redis"]
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


def test_database_exists_error_uses_active_cli_name(monkeypatch):
    monkeypatch.setattr(database_cli, "_deployment_by_name", lambda service, name: object())

    with click.Context(click.Command("create"), info_name="beta9"):
        with pytest.raises(click.ClickException) as exc:
            database_cli._ensure_database_name_available(None, database_cli.REDIS, "myredis")

    message = str(exc.value)
    assert "beta9 db redis credentials myredis" in message
    assert "beta9 db redis status myredis" in message
    assert "beam redis" not in message


def test_database_scale_redeploys_when_pool_changes(monkeypatch):
    scaled = []
    deployed = []

    monkeypatch.setattr(
        database_cli,
        "_scale_database_to",
        lambda *args, **kwargs: scaled.append((args, kwargs)),
    )
    monkeypatch.setattr(
        database_cli,
        "_deploy_database_service",
        lambda **kwargs: deployed.append(kwargs),
    )
    monkeypatch.setattr(database_cli, "_get_secret_value", lambda service, name: f"value-{name}")

    database_cli._scale_database(
        service=None,
        product=database_cli.REDIS,
        name="myredis",
        always_on=True,
        serverless=False,
        cpu=None,
        memory=None,
        pool="private-pool",
        format="table",
    )

    assert scaled[0][0][3] == 0
    assert scaled[0][1]["all_deployments"] is True
    assert deployed[0]["pool"] == "private-pool"
    assert deployed[0]["min_replicas"] == 1


def test_database_registry_images_skip_python_runtime(monkeypatch):
    class FakeImageClient:
        verify_requests = []
        build_requests = []

        def __init__(self, channel):
            pass

        def verify_image_build(self, request):
            self.verify_requests.append(request)
            return SimpleNamespace(exists=False, image_id="")

        def build_image(self, request):
            self.build_requests.append(request)
            yield SimpleNamespace(
                done=True,
                success=True,
                msg="",
                image_id="redis-image-id",
                python_version="",
            )

    monkeypatch.setattr(database_cli, "ImageServiceStub", FakeImageClient)

    db_service = database_cli._database_service(
        product=database_cli.REDIS,
        name="myredis",
        size=database_cli.REDIS.default_size,
        pool=None,
        min_replicas=0,
        cpu=None,
        memory=None,
    )

    assert db_service.image.ignore_python is True

    image_id, _ = database_cli._registry_image_id(
        SimpleNamespace(channel=object()),
        db_service.image,
    )

    assert image_id == "redis-image-id"
    assert FakeImageClient.verify_requests[0].ignore_python is True
    assert FakeImageClient.build_requests[0].ignore_python is True


def test_database_services_are_serverless_without_pool():
    db_service = database_cli._database_service(
        product=database_cli.REDIS,
        name="myredis",
        size=database_cli.REDIS.default_size,
        pool=None,
        min_replicas=0,
        cpu=None,
        memory=None,
    )

    assert db_service.pool_config is None
