import json
from unittest import mock

import pytest

from beta9 import Catalog, Gpu, Image, ManagedEndpoint, Pricing
from beta9.abstractions.image import ImageBuildResult
from beta9.clients.gateway import DeployStubResponse


def test_endpoint_spec_serializes():
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
        pricing=Pricing(prompt_tokens="0.0000002"),
        catalog=Catalog(name="GLM", context_length=131072),
        public=True,
        drain_seconds=30,
    )
    spec = ep.spec()
    assert spec["id"] == "zai-org/glm-4.5-air"
    assert spec["gpu"] == {
        "H100": {"count": 2, "engine_args": ["--tp", "2"], "config": {"max_num_seqs": 256}},
        "A100-80": {"count": 1},
    }
    assert spec["pricing"] == {"prompt_tokens": "0.0000002"}
    assert spec["public"] is True
    assert "harness" not in spec
    assert spec["drain_seconds"] == 30
    assert "protected" not in spec
    assert "routes" not in spec
    assert ep.ports == [8000]


def test_policy_fields_survive_pruning():
    spec = ManagedEndpoint(id="a/b", drain_seconds=0).spec()
    assert spec["drain_seconds"] == 0


def test_access_is_private_by_default_and_separate_from_catalog():
    spec = ManagedEndpoint(id="a/b", allowed_workspaces=["workspace-id"]).spec()
    assert spec["public"] is False
    assert spec["allowed_workspaces"] == ["workspace-id"]
    assert set(Catalog().to_dict()) == {"name", "description", "context_length"}


@pytest.mark.parametrize(
    "factory,kwargs",
    [
        (Catalog, {"public": True}),
        (Catalog, {"free": True}),
        (Gpu, {"harness": {}}),
        (ManagedEndpoint, {"id": "a/b", "harness": True}),
        (ManagedEndpoint, {"id": "a/b", "preemptible": False}),
    ],
)
def test_removed_options_are_rejected(factory, kwargs):
    with pytest.raises(TypeError, match="unexpected keyword"):
        factory(**kwargs)


def test_gpu_list_and_cpu_default():
    assert list(ManagedEndpoint(id="a/b", gpu=["H100", "A10G"]).gpus) == ["H100", "A10G"]
    assert ManagedEndpoint(id="a/b").gpus == {}
    assert "gpu" not in ManagedEndpoint(id="a/b").spec()


@pytest.mark.parametrize("gpu", ["H100", ["H100", "A10G"], {"H100": None}, {"H100": Gpu()}])
def test_plain_gpu_declarations_survive_serialization(gpu):
    """A GPU with empty settings is still a supported GPU; config.yaml can only place what the spec declares."""
    spec = ManagedEndpoint(id="audit/model", gpu=gpu).spec()
    assert spec["gpu"]["H100"] == {"count": 1}


def test_deploy_sets_managed_endpoint_json():
    ep = ManagedEndpoint(id="acme/echo", kind="custom", entrypoint=["python", "app.py"])
    with mock.patch.object(ep, "prepare_runtime", return_value=False) as prepare:
        result, ok = ep.deploy(git_sha="abc")
    assert not ok and result == {}
    prepare.assert_called_once()
    assert prepare.call_args.kwargs["stub_type"] == "managed_endpoint/deployment"
    cfg = json.loads(ep.managed_endpoint)
    assert cfg["endpoint"]["kind"] == "custom"
    assert cfg["git_sha"] == "abc"
    assert ep.entrypoint[0] == "sh"


def test_deploy_rejects_mismatched_name():
    ep = ManagedEndpoint(id="acme/echo", kind="custom", entrypoint=["python", "app.py"])
    result, ok = ep.deploy(name="other")
    assert not ok


def test_deploy_cached_private_image_without_registry_credentials(monkeypatch):
    monkeypatch.delenv("GITHUB_USERNAME", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    image = Image.from_registry(
        "ghcr.io/acme/model:pinned", credentials=["GITHUB_USERNAME", "GITHUB_TOKEN"]
    ).add_commands(["python -c 'import engine'"])
    ep = ManagedEndpoint(id="acme/model", image=image, entrypoint=["engine", "serve"])
    cached = ImageBuildResult(success=True, image_id="cached-model", python_version="python3.12")
    with (
        mock.patch.object(image, "_prepare_context"),
        mock.patch.object(image, "_cached_build_result", return_value=None),
        mock.patch.object(image, "_exists", return_value=(True, cached)) as exists,
        mock.patch.object(image, "get_credentials_from_env") as credentials,
        mock.patch.object(ep, "prepare_runtime", side_effect=lambda **_: image.build().success),
        mock.patch.object(ep.gateway_stub, "deploy_stub", return_value=DeployStubResponse(ok=True)),
    ):
        _, ok = ep.deploy()
    assert ok
    exists.assert_called_once()
    credentials.assert_not_called()


def test_rollout_policy_is_explicit_and_validated():
    assert ManagedEndpoint(id="a/b").spec()["rollout"] == "wait_for_capacity"
    assert ManagedEndpoint(id="a/b", rollout="replace").spec()["rollout"] == "replace"
    with pytest.raises(ValueError, match="rollout"):
        ManagedEndpoint(id="a/b", rollout="typo")
