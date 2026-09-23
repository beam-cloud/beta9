import pytest

from beta9.cli.template import (
    TemplateError,
    _deploy_command,
    validate_manifest,
)


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
