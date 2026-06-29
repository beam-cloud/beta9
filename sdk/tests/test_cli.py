import click
import pytest

from beta9.cli import database as database_cli
from beta9.cli.main import load_cli


def test_database_commands_are_nested_under_db():
    cli = load_cli(check_config=False)

    assert cli.common_group.get_command(None, "postgres") is None
    assert cli.common_group.get_command(None, "redis") is None

    db = cli.common_group.get_command(None, "db")
    assert db is not None
    assert sorted(db.commands) == ["postgres", "redis"]
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
