import json
from unittest import mock

from beta9 import Catalog, Gpu, Image, ManagedEndpoint, Pricing


def test_endpoint_spec_serializes():
    ep = ManagedEndpoint(
        id="zai-org/glm-4.5-air",
        kind="llm",
        engine="vllm",
        image=Image(base_image="vllm/vllm-openai:latest"),
        entrypoint=["vllm", "serve", "x"],
        gpu={"H100": Gpu(count=2, engine_args=["--tp", "2"], harness={"max_num_seqs": 256}), "A100-80": None},
        pricing=Pricing(prompt_tokens="0.0000002"),
        catalog=Catalog(name="GLM", context_length=131072, public=True),
        harness=True,
        drain_seconds=30,
    )
    spec = ep.spec()
    assert spec["id"] == "zai-org/glm-4.5-air"
    assert spec["gpu"] == {
        "H100": {"count": 2, "engine_args": ["--tp", "2"], "harness": {"max_num_seqs": 256}},
        "A100-80": {"count": 1},
    }
    assert spec["pricing"] == {"prompt_tokens": "0.0000002"}
    assert spec["catalog"]["public"] is True
    assert spec["harness"] is True
    assert spec["drain_seconds"] == 30
    assert "routes" not in spec
    assert ep.ports == [8000]


def test_gpu_list_and_cpu_default():
    assert list(ManagedEndpoint(id="a/b", gpu=["H100", "A10G"]).gpus) == ["H100", "A10G"]
    assert ManagedEndpoint(id="a/b").gpus == {}


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
