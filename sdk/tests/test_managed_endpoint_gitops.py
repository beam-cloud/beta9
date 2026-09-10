import importlib.util
import shutil
import subprocess
from pathlib import Path
from unittest.mock import Mock

import pytest


@pytest.fixture
def deployer(tmp_path, monkeypatch):
    monkeypatch.setenv("ENDPOINTS_REPO_PATH", "endpoints")
    monkeypatch.setenv("ENDPOINTS_LAST_SHA", "old")
    monkeypatch.setenv("ENDPOINTS_REPO_SHA", "new")
    path = (
        Path(__file__).resolve().parents[2] / "pkg/abstractions/managedendpoint/gitops_deployer.py"
    )
    spec = importlib.util.spec_from_file_location("test_gitops_deployer", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    shutil.rmtree(module.WORKDIR)
    module.REPO = tmp_path
    (tmp_path / ".gitopsignore").write_text(
        "# pinned image sources\nREADME.md\nimages/**\n.github/**\nendpoints/**\n"
    )
    return module


def test_docs_and_image_sources_do_not_redeploy_pinned_models(deployer):
    deployer.git = Mock(
        return_value=Mock(
            stdout="README.md\nimages/vllm/Dockerfile\n.github/workflows/build.yml\n.gitopsignore\n"
        )
    )
    assert deployer.changed_paths() == set()
    assert not deployer.app_changed("qwen/model", deployer.changed_paths(), ["qwen/model"])


def test_endpoint_files_cannot_be_ignored_and_shared_code_still_redeploys(deployer):
    deployer.git = Mock(
        return_value=Mock(stdout="endpoints/qwen/model/app.py\ncommon.py\nconfig.yaml\n")
    )
    assert deployer.changed_paths() == {"endpoints/qwen/model/app.py", "common.py", "config.yaml"}
    assert deployer.app_changed("qwen/model", {"common.py"}, ["qwen/model"])
    assert not deployer.app_changed("qwen/model", {"config.yaml"}, ["qwen/model"])


def test_config_is_required_and_old_filename_is_not_used(deployer):
    (deployer.REPO / "fleet.yaml").write_text("qwen/model: {}\n")
    with pytest.raises(RuntimeError, match="config.yaml is required"):
        deployer.load_config()
    config = "qwen/model:\n  enabled: true\n  gpus:\n    H100: {priority: 1, minReplicas: 1, maxReplicas: 2, preemption: false}\n"
    (deployer.REPO / "config.yaml").write_text(config)
    assert deployer.load_config() == config


def test_removing_endpoint_skips_unchanged_survivor(deployer, monkeypatch):
    root = deployer.REPO / "endpoints"
    app = root / "qwen/model/app.py"
    app.parent.mkdir(parents=True)
    app.write_text("")
    (deployer.REPO / "config.yaml").write_text("{}")
    monkeypatch.chdir(deployer.REPO)
    monkeypatch.setattr(deployer, "checkout", lambda: None)
    monkeypatch.setattr(
        deployer, "changed_paths", lambda: {"config.yaml", "endpoints/codex/donor/app.py"}
    )
    monkeypatch.setattr(
        deployer,
        "git",
        Mock(
            return_value=Mock(stdout="endpoints/qwen/model/app.py\0endpoints/codex/donor/app.py\0")
        ),
    )

    def deploy(app, root, changed, app_dirs, report):
        rel = str(app.parent.relative_to(root))
        report["results"].append(
            {
                "id": rel,
                "path": rel,
                "ok": True,
                "skipped": not deployer.app_changed(rel, changed, app_dirs),
            }
        )

    monkeypatch.setattr(deployer, "deploy_app", deploy)
    reports = []
    monkeypatch.setattr(deployer, "post_report", lambda report: reports.append(report) or True)
    with pytest.raises(SystemExit) as exc:
        deployer.main()
    assert exc.value.code == 0
    assert reports[0]["error"] == ""
    # Omitting the deleted app retires it; the surviving app must keep its version.
    assert reports[0]["results"] == [
        {"id": "qwen/model", "path": "qwen/model", "ok": True, "skipped": True}
    ]


def test_old_tree_scopes_removed_apps_and_preserves_shared_changes(deployer):
    root = deployer.REPO / "endpoints"
    files = [
        "qwen/model/app.py",
        "codex/donor/app.py",
        "codex/donor/settings.py",
        "family/app.py",
        "family/nested/app.py",
        "shared/common.py",
        ".hidden/app.py",
        "node_modules/ignored/app.py",
    ]
    for name in files:
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("")
    deployer.git("init", "-q")
    deployer.git("add", ".")
    deployer.LAST_SHA = deployer.git("write-tree").stdout.strip()
    removed = [
        "codex/donor/app.py",
        "codex/donor/settings.py",
        "family/nested/app.py",
        "shared/common.py",
    ]
    for name in removed:
        (root / name).unlink()
    changed = {"endpoints/" + name for name in removed}
    apps = deployer.discover(root)
    dirs = deployer.app_directories(root, apps, changed)
    assert dirs == {"qwen/model", "codex/donor", "family", "family/nested"}
    private_changes = changed - {"endpoints/shared/common.py"}
    assert not deployer.app_changed("qwen/model", private_changes, dirs)
    assert not deployer.app_changed("family", private_changes, dirs)
    # Removing a real shared helper must still redeploy all remaining apps.
    assert deployer.app_changed("qwen/model", changed, dirs)
    assert deployer.app_changed("family", changed, dirs)


@pytest.mark.parametrize(
    "rel,expected", [("family", False), ("family/nested", True), ("qwen/model", False)]
)
def test_nested_app_changes_only_redeploy_the_deepest_owner(deployer, rel, expected):
    assert (
        deployer.app_changed(
            rel, {"endpoints/family/nested/helper.py"}, {"family", "family/nested", "qwen/model"}
        )
        is expected
    )


def test_root_app_and_first_or_retry_runs(deployer, monkeypatch):
    monkeypatch.setattr(deployer, "REPO_PATH", "")
    app = deployer.REPO / "app.py"
    app.write_text("")
    deployer.git = Mock(
        side_effect=AssertionError("unchanged/full/retry runs need no old tree read")
    )
    for changed in (None, set()):
        assert deployer.app_directories(deployer.REPO, [app], changed) == {""}
    deployer.LAST_SHA = deployer.SHA
    assert deployer.app_directories(deployer.REPO, [app], {"helper.py"}) == {""}
    assert deployer.app_changed("", {"helper.py"}, {""})
    assert not deployer.app_changed("", {"nested/helper.py"}, {"", "nested"})


def test_unreadable_old_tree_aborts_instead_of_guessing_shared_changes(deployer):
    deployer.git = Mock(side_effect=subprocess.CalledProcessError(128, ["git", "ls-tree"]))
    with pytest.raises(subprocess.CalledProcessError):
        deployer.app_directories(deployer.REPO / "endpoints", [], {"endpoints/deleted/app.py"})
