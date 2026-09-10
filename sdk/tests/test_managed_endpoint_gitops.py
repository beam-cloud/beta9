import importlib.util
import shutil
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
