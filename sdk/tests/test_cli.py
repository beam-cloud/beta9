from types import SimpleNamespace

import click
import pytest

from beta9.cli import database as database_cli
from beta9.cli.main import load_cli


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
