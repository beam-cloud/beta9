"""
StubConfigV1 JSON → GetOrCreateStub request, for recreating a stub with an
edited config. Mirrors the dashboard's services/gateway/deploy.ts.
"""

import json
from typing import Any, Callable, Dict

from ..channel import ServiceClient


def stub_request_from_config(stub: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    """Same code object; every other field from config."""
    runtime = config.get("runtime") or {}
    gpus = runtime.get("gpus") or ([runtime["gpu"]] if runtime.get("gpu") else [])
    secrets = config.get("secrets") or []
    # A secret bound under another name was a `${{secret.X}}` reference before the
    # gateway expanded it; send it back as one so the binding survives. Bindings go
    # first: an env entry for the same variable (a re-pointed reference) wins.
    env = [
        f"{s['env_name']}=${{{{secret.{s['name']}}}}}" for s in secrets if s.get("env_name")
    ] + list(config.get("env") or [])
    return {
        "object_id": (stub.get("object") or {}).get("external_id", ""),
        "image_id": runtime.get("image_id", ""),
        "stub_type": stub["type"],
        "name": stub["name"],
        "python_version": config.get("python_version", ""),
        "cpu": runtime.get("cpu", 0),
        "memory": runtime.get("memory", 0),
        "gpu": ",".join(gpus),
        "gpu_count": runtime.get("gpu_count", 0),
        "handler": config.get("handler", ""),
        "on_start": config.get("on_start", ""),
        "on_deploy": config.get("on_deploy", ""),
        "on_deploy_stub_id": config.get("on_deploy_stub_id", ""),
        "callback_url": config.get("callback_url", ""),
        "keep_warm_seconds": config.get("keep_warm_seconds", 0),
        "workers": config.get("workers", 1),
        "concurrent_requests": config.get("concurrent_requests", 1),
        "max_pending_tasks": config.get("max_pending_tasks", 100),
        "volumes": config.get("volumes") or [],
        "secrets": [{"name": s["name"]} for s in secrets if not s.get("env_name")],
        "authorized": config.get("authorized", True),
        "autoscaler": config.get("autoscaler"),
        "task_policy": config.get("task_policy"),
        "extra": config["extra"]
        if isinstance(config.get("extra"), str)
        else json.dumps(config.get("extra") or {}),
        "checkpoint_enabled": config.get("checkpoint_enabled", False),
        "checkpoint_trigger": config.get("checkpoint_trigger"),
        "entrypoint": config.get("entry_point") or [],
        "ports": config.get("ports") or [],
        "env": env,
        "inputs": config.get("inputs"),
        "outputs": config.get("outputs"),
        "app_name": (stub.get("app") or {}).get("name", ""),
        "tcp": config.get("tcp", False),
        "block_network": config.get("block_network", False),
        "allow_list": config.get("allow_list") or [],
        "docker_enabled": config.get("docker_enabled", False),
        "hostname": config.get("hostname", ""),
        "is_service": config.get("is_service", False),
        "serving": config.get("serving"),
        "disks": config.get("disks") or [],
        "pool": config.get("pool"),
        "managed_endpoint": json.dumps((config.get("managed_endpoint") or {}).get("endpoint"))
        if (config.get("managed_endpoint") or {}).get("endpoint")
        else "",
        "force_create": True,
    }


def stub_config(stub: Dict[str, Any]) -> Dict[str, Any]:
    try:
        return json.loads(stub.get("config") or "{}")
    except ValueError:
        return {}


def redeploy_with_config(
    service: ServiceClient,
    name: str,
    stub_id: str,
    mutate: Callable[[Dict[str, Any]], None],
) -> Dict[str, Any]:
    """Create a new version of deployment `name` from `stub_id` with an edited config."""
    from ..clients.gateway import DeployStubRequest

    stub = service.http.json("GET", f"/api/v1/stub/{{ws}}/{stub_id}")
    config = stub_config(stub)
    mutate(config)
    created = service.http.json(
        "POST", "/api/v1/gateway/stubs", json=stub_request_from_config(stub, config), timeout=600
    )
    if not created.get("ok"):
        raise RuntimeError(created.get("errMsg") or "stub creation failed")
    deployed = service.gateway.deploy_stub(DeployStubRequest(stub_id=created["stubId"], name=name))
    if not deployed.ok:
        raise RuntimeError(deployed.err_msg or "deploy failed")
    return {"deployment_id": deployed.deployment_id, "version": deployed.version}
