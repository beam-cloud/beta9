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
