import json
from unittest import mock

from beta9 import (
    Catalog,
    GpuTarget,
    Image,
    KVCache,
    ManagedEndpoint,
    ManagedService,
    Pricing,
)


def test_endpoint_spec_serializes():
    ep = ManagedEndpoint(
        id="zai-org/glm-4.5-air",
        kind="llm",
        engine="vllm",
        image=Image(base_image="vllm/vllm-openai:latest"),
        entrypoint=["vllm", "serve", "x"],
        gpu=[
            GpuTarget(
                "H100",
                count=2,
                min_replicas=1,
                max_replicas=4,
                share=0.3,
                harness={"max_num_seqs": 256},
            ),
            "A100-80",
        ],
        pricing=Pricing(prompt_tokens="0.0000002"),
        catalog=Catalog(name="GLM", context_length=131072, public=True),
        harness=True,
        kv_cache=KVCache(connector="mooncake", service="mooncake-master"),
        topology={"prefill": [GpuTarget("H100")], "decode": [GpuTarget("H100", count=2)]},
    )
    spec = ep.spec()
    assert spec["id"] == "zai-org/glm-4.5-air"
    assert spec["gpu"][0]["type"] == "H100"
    assert spec["gpu"][0]["harness"] == {"max_num_seqs": 256}
    assert spec["gpu"][1]["type"] == "A100-80"
    assert spec["pricing"] == {"prompt_tokens": "0.0000002"}
    assert spec["catalog"]["public"] is True
    assert spec["harness"] is True
    assert spec["kv_cache"]["service"] == "mooncake-master"
    assert spec["topology"]["decode"][0]["count"] == 2
    assert "routes" not in spec
    cfg = json.loads(json.dumps(ep.stub_config(git_sha="abc")))
    assert cfg["git_sha"] == "abc"
    assert cfg["endpoint"]["id"] == "zai-org/glm-4.5-air"
    assert ep.ports == [8000]
    assert ep.deployment_stub_type == "managed_endpoint/deployment"


def test_service_spec():
    svc = ManagedService(name="mooncake-master", entrypoint=["mooncake_master"], replicas=2)
    spec = svc.spec()
    assert spec == {"name": "mooncake-master", "port": 8000, "replicas": 2, "per_locality": False}
    assert svc.stub_config()["service"]["name"] == "mooncake-master"


def test_deploy_sets_managed_endpoint_json():
    ep = ManagedEndpoint(id="acme/echo", kind="custom", entrypoint=["python", "app.py"])
    with mock.patch.object(ep, "prepare_runtime", return_value=False) as prepare:
        result, ok = ep.deploy()
    assert not ok and result == {}
    prepare.assert_called_once()
    assert prepare.call_args.kwargs["stub_type"] == "managed_endpoint/deployment"
    cfg = json.loads(ep.managed_endpoint)
    assert cfg["endpoint"]["kind"] == "custom"
    assert ep.entrypoint[0] == "sh"


def test_deploy_rejects_mismatched_name():
    ep = ManagedEndpoint(id="acme/echo", kind="custom", entrypoint=["python", "app.py"])
    result, ok = ep.deploy(name="other")
    assert not ok
