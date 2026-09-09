"""
Managed endpoints: platform-owned inference endpoints declared in a git
repository, deployed as stubs in the cluster admin workspace, filled onto spare
GPU capacity and served through the OpenRouter-compatible ``/v1`` route.

An endpoint app is a module that exports a ``ManagedEndpoint`` (or a
``ManagedService`` for shared infrastructure such as a KV store master)::

    from beta9 import Catalog, GpuTarget, Image, ManagedEndpoint, Pricing

    endpoint = ManagedEndpoint(
        id="zai-org/glm-4.5-air",
        kind="llm",
        engine="vllm",
        image=Image(base_image="vllm/vllm-openai:latest"),
        entrypoint=["vllm", "serve", "zai-org/GLM-4.5-Air-FP8", "--port", "8000"],
        gpu=[
            GpuTarget("H100", count=2, min_replicas=1, max_replicas=8, share=0.3,
                      engine_args=["--max-num-seqs", "256"],
                      harness={"max_num_seqs": 256}),
        ],
        pricing=Pricing(prompt_tokens="0.0000002", completion_tokens="0.0000011"),
        catalog=Catalog(name="GLM 4.5 Air", context_length=131072, public=True),
        harness=True,
    )

The GitOps reconciler runs ``beta9 deploy app.py:endpoint`` for every changed
directory; the same command works by hand with a cluster admin token.
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

ENDPOINT_KINDS = ("llm", "embedding", "image", "custom")


def _drop_empty(value: Any) -> Any:
    """Recursively drop None / empty containers so specs serialize minimally."""
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            cleaned = _drop_empty(v)
            if cleaned is None or cleaned == {} or cleaned == [] or cleaned == "":
                continue
            out[k] = cleaned
        return out
    if isinstance(value, list):
        return [_drop_empty(v) for v in value if v is not None]
    return value


@dataclass
class GpuTarget:
    """
    One hardware shape an endpoint may run on.

    Parameters:
        type: GPU type (``"H100"``, ``"A100-80"``, ...). Empty for CPU-only.
        count: GPUs per replica.
        min_replicas: Protected replicas that are never evicted for serverless work.
        max_replicas: Upper bound including opportunistic replicas.
        share: Fraction of this GPU type's spare capacity the endpoint may fill (0-1).
        engine_args: Extra engine CLI args for this shape (restart-class knobs).
        harness: Initial live harness config for this shape (applied through the
            harness on start; tunable at runtime via the admin RPCs).
    """

    type: Union[GpuTypeAlias, str] = ""
    count: int = 1
    min_replicas: int = 0
    max_replicas: int = 0
    share: float = 0.0
    engine_args: List[str] = field(default_factory=list)
    harness: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        gpu_type = self.type
        if isinstance(gpu_type, GpuType):
            gpu_type = gpu_type.value
        return {
            "type": str(gpu_type or ""),
            "count": int(self.count),
            "min_replicas": int(self.min_replicas),
            "max_replicas": int(self.max_replicas),
            "share": float(self.share),
            "engine_args": list(self.engine_args),
            "harness": dict(self.harness),
        }


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
        data = asdict(self)
        data["public"] = bool(self.public)
        return data


@dataclass
class ReplicaPolicy:
    """Preemption behaviour for replicas above ``min_replicas``."""

    evictable: bool = True
    drain_seconds: int = 5
    keep_warm_seconds: int = 0
    spare_share: float = 0.2

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class KVCache:
    """Shared KV store participation (e.g. Mooncake) within a locality."""

    connector: str = ""
    service: str = ""
    min_replicas: int = 0
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class Topology:
    """
    Prefill/decode disaggregation. ``roles`` maps ``"prefill"`` / ``"decode"``
    to the GPU targets that role may run on.
    """

    mode: str = "monolithic"
    roles: Dict[str, List[GpuTarget]] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "mode": self.mode,
            "roles": {role: [t.to_dict() for t in targets] for role, targets in self.roles.items()},
        }


def _normalize_targets(gpu: Any) -> List[GpuTarget]:
    if gpu is None:
        return []
    if isinstance(gpu, GpuTarget):
        return [gpu]
    if isinstance(gpu, (str, GpuType)):
        return [GpuTarget(type=gpu)]
    out: List[GpuTarget] = []
    for item in gpu:
        if isinstance(item, GpuTarget):
            out.append(item)
        elif isinstance(item, (str, GpuType)):
            out.append(GpuTarget(type=item))
        else:
            raise TypeError(f"gpu entries must be GpuTarget or a GPU type name, got {type(item)!r}")
    return out


class _ManagedStub(RunnerAbstraction):
    """Shared runtime plumbing for ManagedEndpoint and ManagedService."""

    deployment_stub_type: str = ""

    def __init__(
        self,
        *,
        name: str,
        image: Image,
        entrypoint: List[str],
        port: int,
        cpu: Union[int, float, str],
        memory: Union[int, str],
        volumes: Optional[List[Union[Volume, CloudBucket]]],
        secrets: Optional[List[str]],
        env: Optional[Dict[str, str]],
    ) -> None:
        super().__init__(
            cpu=cpu,
            memory=memory,
            gpu=GpuType.NoGPU,
            gpu_count=0,
            image=image,
            volumes=volumes,
            secrets=secrets,
            env=env or {},
            entrypoint=list(entrypoint),
            ports=[int(port)],
            name=name,
            app=name,
            authorized=True,
            keep_warm_seconds=-1,
        )
        self.parent = self
        self.func = None
        self.spec_name = name
        if not self.image.override_python_version:
            self.image.ignore_python = True

    def parse_image(self, image: Image) -> Image:
        image.ignore_python = True
        return image

    def _uses_custom_image_entrypoint(self) -> bool:
        return (
            self.image.base_image != "" or self.image.dockerfile != "" or self.image.image_id != ""
        )

    def spec(self) -> Dict[str, Any]:  # pragma: no cover - overridden
        raise NotImplementedError

    def stub_config(self, git_sha: str = "") -> Dict[str, Any]:  # pragma: no cover - overridden
        raise NotImplementedError

    def deploy(
        self,
        name: Optional[str] = None,
        context: Optional[ConfigContext] = None,
        invocation_details_func: Optional[Callable[..., None]] = None,
        rollout: str = "auto",
        git_sha: str = "",
        **_: Any,
    ) -> Tuple[Dict[str, Any], bool]:
        """
        Deploy this managed stub. ``name`` must match the endpoint id / service
        name when given; the GitOps deployer passes ``git_sha`` so the platform
        can record which revision each version came from.
        """
        if name and name != self.spec_name:
            terminal.error(
                f"Deployment name {name!r} must match the declared id {self.spec_name!r}.",
                exit=False,
            )
            return {}, False
        self.name = self.spec_name

        if context is not None:
            self.config_context = context

        if not self.entrypoint:
            terminal.error("You must specify an entrypoint.", exit=False)
            return {}, False

        is_custom_image = self._uses_custom_image_entrypoint()
        ignore_patterns = ["**"] if is_custom_image else []
        if not is_custom_image:
            self.entrypoint = ["sh", "-c", f"cd {USER_CODE_DIR} && {shlex.join(self.entrypoint)}"]

        self.managed_endpoint = json.dumps(self.stub_config(git_sha=git_sha))

        if not self.prepare_runtime(
            stub_type=self.deployment_stub_type,
            force_create_stub=True,
            ignore_patterns=ignore_patterns,
        ):
            return {}, False

        terminal.header("Deploying")
        deploy_response: DeployStubResponse = self.gateway_stub.deploy_stub(
            DeployStubRequest(stub_id=self.stub_id, name=self.name, rollout=rollout)
        )
        self.deployment_id = deploy_response.deployment_id
        if deploy_response.ok:
            terminal.done("Deployed 🎉")
        elif deploy_response.err_msg:
            terminal.error(deploy_response.err_msg, exit=False)

        return {
            "deployment_id": deploy_response.deployment_id,
            "version": deploy_response.version,
            "id": self.spec_name,
        }, deploy_response.ok


class ManagedEndpoint(_ManagedStub):
    """
    A platform-hosted inference endpoint.

    Parameters:
        id: Catalog id in ``vendor/slug`` form (also the app and deployment name).
        kind: ``"llm"``, ``"embedding"``, ``"image"`` or ``"custom"``.
        image: Container image running the engine.
        entrypoint: Engine command. GPU-shape specific ``engine_args`` are appended
            per replica by the platform.
        engine: Engine name (``"vllm"``, ``"sglang"``, ``"diffusers"``...); used for
            harness selection and cluster allow-lists.
        port: Port the engine listens on.
        health: Readiness path (default ``/health``).
        metrics: Prometheus metrics path (probe-mode engines without a harness).
        gpu: One or more ``GpuTarget`` shapes (or bare GPU type names).
        routes: ``/v1`` routes served; defaults by kind.
        pricing: Per-unit USD pricing.
        catalog: Public listing metadata.
        policy: Preemption behaviour.
        harness: Enable the in-engine harness (live tuning over gRPC).
        kv_cache: Shared KV cache participation.
        topology: Prefill/decode disaggregation.
        services: Managed services this endpoint depends on (addresses are
            injected as ``BEAM_SERVICE_<NAME>_ADDR``).
        locality: Restrict to these localities (pool network domains).
        cpu / memory / volumes / secrets / env: Container resources.
    """

    deployment_stub_type = MANAGED_ENDPOINT_DEPLOYMENT_STUB_TYPE

    def __init__(
        self,
        id: str,
        kind: str = "llm",
        image: Image = Image(),
        entrypoint: Optional[List[str]] = None,
        engine: str = "",
        port: int = 8000,
        health: str = "/health",
        metrics: str = "",
        gpu: Union[GpuTarget, List[GpuTarget], GpuTypeAlias, List[GpuTypeAlias], None] = None,
        routes: Optional[List[str]] = None,
        pricing: Optional[Pricing] = None,
        catalog: Optional[Catalog] = None,
        policy: Optional[ReplicaPolicy] = None,
        harness: bool = False,
        kv_cache: Optional[KVCache] = None,
        topology: Optional[Topology] = None,
        services: Optional[List[str]] = None,
        locality: Optional[List[str]] = None,
        cpu: Union[int, float, str] = 4.0,
        memory: Union[int, str] = "16Gi",
        volumes: Optional[List[Union[Volume, CloudBucket]]] = None,
        secrets: Optional[List[str]] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> None:
        if kind not in ENDPOINT_KINDS:
            raise ValueError(f"kind must be one of {ENDPOINT_KINDS}, got {kind!r}")
        self.id = id
        self.kind = kind
        self.engine = engine
        self.port = int(port)
        self.health = health
        self.metrics = metrics
        self.gpu_targets = _normalize_targets(gpu)
        self.routes = list(routes or [])
        self.pricing = pricing or Pricing()
        self.catalog = catalog or Catalog()
        self.policy = policy or ReplicaPolicy()
        self.harness = bool(harness)
        self.kv_cache = kv_cache
        self.topology = topology
        self.services = list(services or [])
        self.locality = list(locality or [])
        super().__init__(
            name=id,
            image=image,
            entrypoint=list(entrypoint or []),
            port=port,
            cpu=cpu,
            memory=memory,
            volumes=volumes,
            secrets=secrets,
            env=env,
        )

    def spec(self) -> Dict[str, Any]:
        spec: Dict[str, Any] = {
            "id": self.id,
            "kind": self.kind,
            "engine": self.engine,
            "port": self.port,
            "health": self.health,
            "metrics": self.metrics,
            "gpu": [t.to_dict() for t in self.gpu_targets],
            "routes": self.routes,
            "pricing": self.pricing.to_dict(),
            "catalog": self.catalog.to_dict(),
            "policy": self.policy.to_dict(),
            "harness": {"enabled": self.harness},
            "kv_cache": self.kv_cache.to_dict() if self.kv_cache else None,
            "topology": self.topology.to_dict() if self.topology else None,
            "services": self.services,
            "locality": self.locality,
        }
        return _drop_empty(spec)

    def stub_config(self, git_sha: str = "") -> Dict[str, Any]:
        return _drop_empty({"endpoint": self.spec(), "git_sha": git_sha})


class ManagedService(_ManagedStub):
    """
    Protected shared infrastructure for managed endpoints (for example a
    Mooncake store master). Services are never evicted and have no public
    route; endpoints reference them by name.
    """

    deployment_stub_type = MANAGED_SERVICE_DEPLOYMENT_STUB_TYPE

    def __init__(
        self,
        name: str,
        image: Image = Image(),
        entrypoint: Optional[List[str]] = None,
        port: int = 8000,
        health: str = "",
        gpu: Union[GpuTarget, List[GpuTarget], GpuTypeAlias, List[GpuTypeAlias], None] = None,
        replicas: int = 1,
        per_locality: bool = False,
        cpu: Union[int, float, str] = 2.0,
        memory: Union[int, str] = "8Gi",
        volumes: Optional[List[Union[Volume, CloudBucket]]] = None,
        secrets: Optional[List[str]] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> None:
        self.service_name = name
        self.port = int(port)
        self.health = health
        self.gpu_targets = _normalize_targets(gpu)
        self.replicas = int(replicas)
        self.per_locality = bool(per_locality)
        super().__init__(
            name=name,
            image=image,
            entrypoint=list(entrypoint or []),
            port=port,
            cpu=cpu,
            memory=memory,
            volumes=volumes,
            secrets=secrets,
            env=env,
        )

    def spec(self) -> Dict[str, Any]:
        return _drop_empty(
            {
                "name": self.service_name,
                "port": self.port,
                "health": self.health,
                "gpu": [t.to_dict() for t in self.gpu_targets],
                "replicas": self.replicas,
                "per_locality": self.per_locality,
            }
        )

    def stub_config(self, git_sha: str = "") -> Dict[str, Any]:
        return _drop_empty({"service": self.spec(), "git_sha": git_sha})
