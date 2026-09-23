import pytest

from beta9.cli.template import (
    TemplateError,
    _deploy_command,
    import_railway,
    plan_steps,
    validate_manifest,
)

RAILWAY = {
    "template": {"slug": "umami", "name": "Umami", "description": "Analytics"},
    "services": [
        {
            "name": "umami",
            "source": {"image": "umamisoftware/umami:postgresql-latest"},
            "http": True,
        },
        {
            "name": "Postgres",
            "source": {"image": "ghcr.io/railwayapp-templates/postgres-ssl:16"},
            "needs_volume": True,
        },
        {"name": "Valkey", "source": {"image": "valkey/valkey:latest"}, "needs_volume": True},
        {
            "name": "worker",
            "source": {"repo": "https://github.com/org/worker"},
            "needs_volume": True,
            "volume_mount_path": "/data",
        },
    ],
    "required_inputs": [
        {"key": "PORT", "service": "umami", "strategy": "default", "default": "3000"},
        {
            "key": "HOST_URL",
            "service": "umami",
            "strategy": "railway_provided",
            "railway_source": "railway_domain",
        },
        {
            "key": "APP_SECRET",
            "service": "umami",
            "strategy": "generate",
            "generate": "random_base64_32",
        },
        {
            "key": "DATABASE_URL",
            "service": "umami",
            "strategy": "railway_provided",
            "railway_source": "reference_variable",
        },
        {
            "key": "REDIS_URL",
            "service": "umami",
            "strategy": "railway_provided",
            "railway_source": "railway_private_domain",
        },
        {
            "key": "PGHOST",
            "service": "Postgres",
            "strategy": "railway_provided",
            "railway_source": "railway_private_domain",
        },
        {"key": "ADMIN_USER", "service": "worker", "strategy": "ask_user"},
        {
            "key": "ADMIN_PASSWORD",
            "service": "worker",
            "strategy": "generate",
            "generate": "strong_password",
        },
    ],
}


def test_import_railway_maps_databases_inputs_and_disks():
    manifest = import_railway(RAILWAY)
    validate_manifest(manifest)
    services = manifest["services"]
    assert services["umami-postgres"] == {"kind": "database", "engine": "postgres"}
    assert services["umami-redis"] == {"kind": "database", "engine": "redis"}
    umami = services["umami"]
    assert umami["ports"] == [3000]
    assert umami["env"]["HOST_URL"] == "${{app.umami.URL}}"
    assert umami["env"]["APP_SECRET"] == "${{secret(32)}}"
    assert umami["env"]["DATABASE_URL"] == "${{db.umami-postgres.DATABASE_URL}}"
    assert umami["env"]["REDIS_URL"] == "${{db.umami-redis.REDIS_URL}}"
    worker = services["worker"]
    assert worker["kind"] == "repo" and worker["disks"] == {"data": "/data"}
    assert worker["secrets"] == ["ADMIN_USER"]
    assert worker["env"]["ADMIN_PASSWORD"] == "${{secret(24)}}"
    # databases come first, then the apps that reference them
    assert [s["service"] for s in plan_steps(manifest)][:2] == ["umami-postgres", "umami-redis"]


def test_deploy_command_carries_disks():
    args = _deploy_command("app", {"kind": "image", "image": "x", "disks": {"data": "/data"}}, "")
    assert args[args.index("--disk") + 1] == "data:/data"
    args = _deploy_command(
        "app",
        {
            "kind": "image",
            "image": "x",
            "disks": [{"name": "d", "mount_path": "/d", "size": "20Gi"}],
        },
        "",
    )
    assert args[args.index("--disk") + 1] == "d:/d:20Gi"


def test_validate_rejects_relative_disk_mounts():
    with pytest.raises(TemplateError, match="absolute mount path"):
        validate_manifest(
            {"services": {"app": {"kind": "image", "image": "x", "disks": {"data": "data"}}}}
        )
