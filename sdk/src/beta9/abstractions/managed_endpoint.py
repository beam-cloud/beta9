"""
Managed endpoints: platform-owned inference endpoints declared in a git repo,
deployed as stubs in the cluster admin workspace and served through ``/v1``.
An endpoint app exports a ``ManagedEndpoint`` (or a ``ManagedService`` for
shared infrastructure such as a KV store master) and is deployed with
``beta9 deploy app.py:endpoint``, by hand or by the GitOps reconciler.
"""

import json
import shlex
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from .. import terminal
from ..abstractions.base.runner import RunnerAbstraction
from ..abstractions.image import Image
from ..abstractions.volume import CloudBucket, Volume
from ..clients.gateway import DeployStubRequest, DeployStubResponse
from ..config import ConfigContext
from ..runner.common import USER_CODE_DIR
from ..type import GpuType, GpuTypeAlias

MANAGED_ENDPOINT_STUB_TYPE = "managed_endpoint"
MANAGED_ENDPOINT_DEPLOYMENT_STUB_TYPE = "managed_endpoint/deployment"
MANAGED_SERVICE_STUB_TYPE = "managed_service"
MANAGED_SERVICE_DEPLOYMENT_STUB_TYPE = "managed_service/deployment"

GpuArg = Union["GpuTarget", List["GpuTarget"], GpuTypeAlias, List[GpuTypeAlias], None]


def _drop_empty(value: Any) -> Any:
    """Recursively drop None / empty containers so specs serialize minimally."""
    if isinstance(value, dict):
        cleaned = {k: _drop_empty(v) for k, v in value.items()}
        return {k: v for k, v in cleaned.items() if v not in (None, {}, [], "")}
    if isinstance(value, list):
        return [_drop_empty(v) for v in value if v is not None]
    return value


@dataclass
class GpuTarget:
    """One hardware shape an endpoint may run on (empty ``type`` = CPU-only)."""

    type: Union[GpuTypeAlias, str] = ""
    count: int = 1
    min_replicas: int = 0
    max_replicas: int = 0
    share: float = 0.0
    engine_args: List[str] = field(default_factory=list)
    harness: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        gpu_type = self.type.value if isinstance(self.type, GpuType) else self.type
        data = asdict(self)
        data.update(type=str(gpu_type or ""), count=int(self.count), share=float(self.share))
        data.update(min_replicas=int(self.min_replicas), max_replicas=int(self.max_replicas))
        return data


@dataclass
class Pricing:
    """USD decimal strings per unit. Empty dimensions are free."""

    prompt_tokens: str = ""
    completion_tokens: str = ""
    cached_prompt_tokens: str = ""
    request: str = ""
    image: str = ""

    def to_dict(self) -> Dict[str, str]:
        return {k: v for k, v in asdict(self).items() if v}


@dataclass
class Catalog:
    """Public listing metadata for ``GET /v1/models`` (OpenRouter shape)."""

    name: str = ""
    description: str = ""
    hf_id: str = ""
    context_length: int = 0
    max_completion_tokens: int = 0
    tokenizer: str = ""
    instruct_type: str = ""
    modalities: List[str] = field(default_factory=list)
    supported_parameters: List[str] = field(default_factory=list)
    public: bool = False
    allowed_workspaces: List[str] = field(default_factory=list)
    free: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {**asdict(self), "public": bool(self.public)}


@dataclass
class ReplicaPolicy:
    """Preemption behaviour for replicas above ``min_replicas``."""

    evictable: bool = True
    drain_seconds: int = 5
    spare_share: float = 0.2

    to_dict = asdict


@dataclass
class KVCache:
    """Shared KV store participation (e.g. Mooncake) within a locality."""

    connector: str = ""
    service: str = ""
    min_replicas: int = 0
    extra: Dict[str, Any] = field(default_factory=dict)

    to_dict = asdict


def _normalize_targets(gpu: GpuArg) -> List[GpuTarget]:
    items = [] if gpu is None else gpu if isinstance(gpu, (list, tuple)) else [gpu]
    out: List[GpuTarget] = []
    for item in items:
        if isinstance(item, (str, GpuType)):
            item = GpuTarget(type=item)
        elif not isinstance(item, GpuTarget):
            raise TypeError(f"gpu entries must be GpuTarget or a GPU type name, got {type(item)!r}")
        out.append(item)
    return out


class _ManagedStub(RunnerAbstraction):
    """Shared runtime plumbing for ManagedEndpoint and ManagedService."""

    deployment_stub_type: str = ""
    spec_key: str = ""

    def __init__(
        self,
        name: str,
        image: Optional[Image],
        entrypoint: Optional[List[str]],
        port: int,
        cpu: Union[int, float, str],
        memory: Union[int, str],
        volumes: Optional[List[Union[Volume, CloudBucket]]],
        secrets: Optional[List[str]],
        env: Optional[Dict[str, str]],
    ) -> None:
        # A fresh Image per stub: the deployer loads many apps in one process,
        # and Image.build() mutates the instance it was given.
        super().__init__(
            cpu=cpu,
            memory=memory,
            image=image if image is not None else Image(),
            volumes=volumes,
            secrets=secrets,
            env=env,
            entrypoint=list(entrypoint or []),
            ports=[int(port)],
            name=name,
            app=name,
            keep_warm_seconds=-1,
        )
        self.parent = self
        self.func = None
        self.spec_name = name
        self.port = int(port)
        if not self.image.override_python_version:
            self.image.ignore_python = True

    def stub_config(self, git_sha: str = "") -> Dict[str, Any]:
        return _drop_empty({self.spec_key: self.spec(), "git_sha": git_sha})

    def deploy(
        self,
        name: Optional[str] = None,
        context: Optional[ConfigContext] = None,
        invocation_details_func: Optional[Callable[..., None]] = None,
        rollout: str = "auto",
        git_sha: str = "",
        **_: Any,
    ) -> Tuple[Dict[str, Any], bool]:
        """Deploy this stub; ``name`` must match the declared id when given."""
        if name and name != self.spec_name:
            msg = f"Deployment name {name!r} must match the declared id {self.spec_name!r}."
            terminal.error(msg, exit=False)
            return {}, False
        self.name = self.spec_name
        if context is not None:
            self.config_context = context
        if not self.entrypoint:
            terminal.error("You must specify an entrypoint.", exit=False)
            return {}, False

        image = self.image
        # Only an image the user supplied (base image, Dockerfile or explicit id)
        # skips code sync; an id produced by an earlier Image.build() does not.
        custom_image = bool(image.base_image or image.dockerfile or image._explicit_image_id)
        if not custom_image:
            # exec so SIGTERM from an eviction or drain reaches the engine, not a wrapper shell.
            cmd = f"cd {USER_CODE_DIR} && exec {shlex.join(self.entrypoint)}"
            self.entrypoint = ["sh", "-c", cmd]
        self.managed_endpoint = json.dumps(self.stub_config(git_sha=git_sha))

        if not self.prepare_runtime(
            stub_type=self.deployment_stub_type,
            force_create_stub=True,
            ignore_patterns=["**"] if custom_image else [],
        ):
            return {}, False

        terminal.header("Deploying")
        resp: DeployStubResponse = self.gateway_stub.deploy_stub(
            DeployStubRequest(stub_id=self.stub_id, name=self.name, rollout=rollout)
        )
        self.deployment_id = resp.deployment_id
        if resp.ok:
            terminal.done("Deployed 🎉")
        elif resp.err_msg:
            terminal.error(resp.err_msg, exit=False)
        result = {"deployment_id": resp.deployment_id, "version": resp.version}
        return {**result, "id": self.spec_name}, resp.ok


class ManagedEndpoint(_ManagedStub):
    """A platform-hosted inference endpoint; ``id`` (``vendor/slug``) is also the app name."""

    deployment_stub_type = MANAGED_ENDPOINT_DEPLOYMENT_STUB_TYPE
    spec_key = "endpoint"

    def __init__(
        self,
        id: str,
        kind: str = "llm",
        image: Optional[Image] = None,
        entrypoint: Optional[List[str]] = None,
        engine: str = "",
        port: int = 8000,
        health: str = "/health",
        metrics: str = "",
        gpu: GpuArg = None,
        routes: Optional[List[str]] = None,
        pricing: Optional[Pricing] = None,
        catalog: Optional[Catalog] = None,
        policy: Optional[ReplicaPolicy] = None,
        harness: bool = False,
        kv_cache: Optional[KVCache] = None,
        topology: Optional[Dict[str, GpuArg]] = None,
        services: Optional[List[str]] = None,
        locality: Optional[List[str]] = None,
        cpu: Union[int, float, str] = 4.0,
        memory: Union[int, str] = "16Gi",
        volumes: Optional[List[Union[Volume, CloudBucket]]] = None,
        secrets: Optional[List[str]] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> None:
        self.id = id
        self.kind = kind
        self.engine = engine
        self.health = health
        self.metrics = metrics
        self.gpu_targets = _normalize_targets(gpu)
        self.routes = list(routes or [])
        self.pricing = pricing or Pricing()
        self.catalog = catalog or Catalog()
        self.policy = policy or ReplicaPolicy()
        self.harness = bool(harness)
        self.kv_cache = kv_cache
        # Prefill/decode disaggregation: role -> GPU targets that role may run on.
        self.topology = {role: _normalize_targets(targets) for role, targets in (topology or {}).items()}
        self.services = list(services or [])
        self.locality = list(locality or [])
        super().__init__(id, image, entrypoint, port, cpu, memory, volumes, secrets, env)

    def spec(self) -> Dict[str, Any]:
        plain = "id kind engine port health metrics routes services locality".split()
        spec: Dict[str, Any] = {k: getattr(self, k) for k in plain}
        spec.update(
            gpu=[t.to_dict() for t in self.gpu_targets],
            pricing=self.pricing.to_dict(),
            catalog=self.catalog.to_dict(),
            policy=self.policy.to_dict(),
            harness=self.harness,
            kv_cache=self.kv_cache.to_dict() if self.kv_cache else None,
            topology={role: [t.to_dict() for t in targets] for role, targets in self.topology.items()},
        )
        return _drop_empty(spec)


class ManagedService(_ManagedStub):
    """Protected shared infrastructure (e.g. a Mooncake master): never evicted, no public route."""

    deployment_stub_type = MANAGED_SERVICE_DEPLOYMENT_STUB_TYPE
    spec_key = "service"

    def __init__(
        self,
        name: str,
        image: Optional[Image] = None,
        entrypoint: Optional[List[str]] = None,
        port: int = 8000,
        health: str = "",
        gpu: GpuArg = None,
        replicas: int = 1,
        per_locality: bool = False,
        cpu: Union[int, float, str] = 2.0,
        memory: Union[int, str] = "8Gi",
        volumes: Optional[List[Union[Volume, CloudBucket]]] = None,
        secrets: Optional[List[str]] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> None:
        self.service_name = name
        self.health = health
        self.gpu_targets = _normalize_targets(gpu)
        self.replicas = int(replicas)
        self.per_locality = bool(per_locality)
        super().__init__(name, image, entrypoint, port, cpu, memory, volumes, secrets, env)

    def spec(self) -> Dict[str, Any]:
        gpu = [t.to_dict() for t in self.gpu_targets]
        spec = {"name": self.service_name, "port": self.port, "health": self.health, "gpu": gpu}
        return _drop_empty({**spec, "replicas": self.replicas, "per_locality": self.per_locality})
