import json
from pathlib import Path
from unittest import mock

import pytest

from beta9 import Gpu, Image, ManagedEndpoint
from beta9.abstractions.image import ImageBuildResult
from beta9.cli import endpoints
from beta9.clients.managedendpoint import ApplyRepoResponse
from beta9.clients.gateway import DeployStubResponse


def test_endpoint_spec_is_runtime_only():
    ep = ManagedEndpoint(
        id="zai-org/glm-4.5-air",
        kind="llm",
        engine="vllm",
        image=Image(base_image="vllm/vllm-openai:latest"),
        entrypoint=["vllm", "serve", "x"],
        gpu={
            "H100": Gpu(count=2, engine_args=["--tp", "2"], config={"max_num_seqs": 256}),
            "A100-80": None,
        },
        drain_seconds=30,
    )
    spec = ep.spec()
    assert spec["id"] == "zai-org/glm-4.5-air"
    assert spec["gpu"] == {
        "H100": {"count": 2, "engine_args": ["--tp", "2"], "config": {"max_num_seqs": 256}},
        "A100-80": {"count": 1},
    }
    assert spec["drain_seconds"] == 30
    assert set(spec) <= {
        "id",
        "kind",
        "engine",
        "entrypoint",
        "port",
        "health",
        "metrics",
        "rollout",
        "drain_seconds",
        "gpu",
    }, "publication (catalog, access, pricing) lives in config.yaml"
    assert ep.ports == [8000]


def test_policy_fields_survive_pruning():
    assert ManagedEndpoint(id="a/b", drain_seconds=0).spec()["drain_seconds"] == 0


@pytest.mark.parametrize(
    "kwargs",
    [
        {"pricing": {"request": "0.05"}},
        {"catalog": {"name": "x"}},
        {"public": True},
        {"allowed_workspaces": ["ws"]},
        {"routes": ["chat/completions"]},
        {"harness": True},
    ],
)
def test_publication_options_are_not_app_settings(kwargs):
    with pytest.raises(TypeError, match="unexpected keyword"):
        ManagedEndpoint(id="a/b", **kwargs)


def test_gpu_list_and_cpu_default():
    assert list(ManagedEndpoint(id="a/b", gpu=["H100", "A10G"]).gpus) == ["H100", "A10G"]
    assert ManagedEndpoint(id="a/b").gpus == {}
    assert "gpu" not in ManagedEndpoint(id="a/b").spec()


@pytest.mark.parametrize("gpu", ["H100", ["H100", "A10G"], {"H100": None}, {"H100": Gpu()}])
def test_plain_gpu_declarations_survive_serialization(gpu):
    """A GPU with empty settings is still a supported GPU; config.yaml can only place what the spec declares."""
    assert ManagedEndpoint(id="audit/model", gpu=gpu).spec()["gpu"]["H100"] == {"count": 1}


def test_deploy_sets_managed_endpoint_json():
    ep = ManagedEndpoint(id="acme/echo", kind="custom", entrypoint=["python", "app.py"])
    with mock.patch.object(ep, "prepare_runtime", return_value=False) as prepare:
        result, ok = ep.deploy()
    assert not ok and result == {}
    prepare.assert_called_once()
    assert prepare.call_args.kwargs["stub_type"] == "managed_endpoint/deployment"
    assert not prepare.call_args.kwargs.get("force_create_stub"), "an unchanged app reuses its stub"
    assert json.loads(ep.managed_endpoint) == {"endpoint": ep.spec()}
    assert ep.entrypoint[0] == "sh"


def test_deploy_rejects_mismatched_name():
    ep = ManagedEndpoint(id="acme/echo", kind="custom", entrypoint=["python", "app.py"])
    _, ok = ep.deploy(name="other")
    assert not ok


def test_deploy_cached_private_image_skips_build():
    image = Image.from_registry(
        "ghcr.io/acme/model:pinned", credentials=["GITHUB_USERNAME", "GITHUB_TOKEN"]
    ).add_commands(["python -c 'import engine'"])
    ep = ManagedEndpoint(id="acme/model", image=image, entrypoint=["engine", "serve"])
    cached = ImageBuildResult(success=True, image_id="cached-model", python_version="python3.12")
    with (
        mock.patch.object(image, "_prepare_context"),
        mock.patch.object(image, "_cached_build_result", return_value=None),
        mock.patch.object(image, "_exists", return_value=(True, cached)) as exists,
        mock.patch.object(ep, "prepare_runtime", side_effect=lambda **_: image.build().success),
        mock.patch.object(ep.gateway_stub, "deploy_stub", return_value=DeployStubResponse(ok=True)),
    ):
        _, ok = ep.deploy()
    assert ok
    exists.assert_called_once()


def test_rollout_policy_is_explicit_and_validated():
    assert ManagedEndpoint(id="a/b").spec()["rollout"] == "wait_for_capacity"
    assert ManagedEndpoint(id="a/b", rollout="replace").spec()["rollout"] == "replace"
    with pytest.raises(ValueError, match="rollout"):
        ManagedEndpoint(id="a/b", rollout="typo")


def _repo(tmp_path: Path, apps: dict) -> Path:
    for path, source in apps.items():
        app = tmp_path / "endpoints" / path / "app.py"
        app.parent.mkdir(parents=True)
        app.write_text(source)
    (tmp_path / "config.yaml").write_text("acme/model: {enabled: true}\n")
    return tmp_path


def test_repo_loads_model_servers_and_ordinary_apps(tmp_path):
    repo = _repo(
        tmp_path,
        {
            "acme/model": (
                "from beta9 import ManagedEndpoint\n"
                "ep = ManagedEndpoint(id='acme/model', entrypoint=['vllm', 'serve'])\n"
            ),
            "acme/video": (
                "from beta9 import task_queue\n"
                "@task_queue(cpu=1, memory='1Gi', gpu='A10G')\n"
                "def render(prompt: str):\n"
                "    return prompt\n"
            ),
            "acme/wrong": (
                "from beta9 import ManagedEndpoint\n"
                "ep = ManagedEndpoint(id='acme/other', entrypoint=['x'])\n"
            ),
            "acme/broken": "raise SystemExit(3)\n",
        },
    )
    apps = {app.path: app for app in endpoints._apps(repo)}
    assert set(apps) == {"acme/model", "acme/video", "acme/wrong", "acme/broken"}

    model = apps["acme/model"].to_proto()
    assert model.id == "acme/model"
    assert json.loads(model.spec_json)["entrypoint"] == ["vllm", "serve"]
    assert model.error == ""

    video = apps["acme/video"]
    assert json.loads(video.runner.managed_endpoint) == {"endpoint": {"id": "acme/video"}}, (
        "an ordinary task queue is hosted by marking its stub, not by a new abstraction"
    )
    assert json.loads(video.to_proto().spec_json) == {"id": "acme/video"}

    assert "must equal the path" in apps["acme/wrong"].error
    assert apps["acme/broken"].error.startswith("import failed")


def test_apply_sends_repo_and_reports_errors(tmp_path):
    repo = _repo(tmp_path, {})
    response = ApplyRepoResponse(
        ok=False, err_msg="1 problem(s) found", errors=["acme/model: pricing: declare request"]
    )
    stub = mock.Mock()
    stub.apply_repo.return_value = response
    with (
        mock.patch.object(endpoints, "get_channel"),
        mock.patch.object(endpoints, "EndpointAdminServiceStub", return_value=stub),
    ):
        result = endpoints._apply(mock.Mock(), repo, [], dry_run=True, commit={"sha": "abc"})
    request = stub.apply_repo.call_args.args[0]
    assert request.dry_run is True and request.sha == "abc"
    assert request.config_yaml.startswith("acme/model:")
    assert result.errors == ["acme/model: pricing: declare request"]
    with pytest.raises(SystemExit):
        endpoints._report(result)
