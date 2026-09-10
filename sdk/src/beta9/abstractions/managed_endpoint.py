"""
Managed endpoints: platform-owned inference endpoints declared in a git repo
and served through ``/v1``. An ``app.py`` exports one ``ManagedEndpoint`` that
says how the engine runs; ``fleet.yaml`` at the repo root says which endpoints
fill each GPU type, in what order and with what cap. The GitOps reconciler
deploys both.
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


def _drop_empty(value: Any) -> Any:
    """Recursively drop None / empty containers so specs serialize minimally."""
    if isinstance(value, dict):
        cleaned = {k: _drop_empty(v) for k, v in value.items()}
        return {k: v for k, v in cleaned.items() if v not in (None, {}, [], "")}
    if isinstance(value, list):
        return [_drop_empty(v) for v in value if v is not None]
    return value


@dataclass
class Gpu:
    """How the engine runs on one GPU type: GPUs per replica (tensor parallel),
    restart-class args and the live harness seed. Where and how many replicas
    run is fleet.yaml's decision, not the app's."""

    count: int = 1
    engine_args: List[str] = field(default_factory=list)
    harness: Dict[str, Any] = field(default_factory=dict)

    to_dict = asdict


@dataclass
class Pricing:
    """USD decimal strings per unit. Empty dimensions are free."""

    prompt_tokens: str = ""
    completion_tokens: str = ""
    cached_prompt_tokens: str = ""
    request: str = ""
    image: str = ""

    to_dict = asdict


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


GpuArg = Union[
    Dict[Union[GpuTypeAlias, str], Union[Gpu, None]],
    List[Union[GpuTypeAlias, str]],
    GpuTypeAlias,
    str,
    None,
]


def _gpu_key(gpu: Union[GpuTypeAlias, str]) -> str:
    return gpu.value if isinstance(gpu, GpuType) else str(gpu)


def _normalize_gpu(gpu: GpuArg) -> Dict[str, Gpu]:
    """Accept ``"H100"``, ``["H100", "A10G"]`` or ``{"H100": Gpu(...)}``; empty means CPU."""
    if gpu is None:
        return {}
    if isinstance(gpu, dict):
        return {_gpu_key(k): v if v is not None else Gpu() for k, v in gpu.items()}
    items = gpu if isinstance(gpu, (list, tuple)) else [gpu]
    return {_gpu_key(item): Gpu() for item in items}


class ManagedEndpoint(RunnerAbstraction):
    """A platform-hosted inference endpoint; ``id`` (``vendor/slug``) is also the app name.

    Parameters:
        id: ``vendor/slug``; also the model name callers send to ``/v1``.
        kind: ``llm`` | ``embedding`` | ``image`` | ``custom``; picks the routes served.
        image: Container image the engine runs in.
        entrypoint: Engine command; ``Gpu.engine_args`` for the placed GPU type are appended.
        engine: Engine name for validation/observability (``vllm``, ``sglang``...).
        port / health / metrics: Where the engine listens and its readiness / Prometheus paths.
        gpu: GPU types the engine can run on, optionally with per-type ``Gpu`` settings.
            Which of these are actually used, and with what priority and cap, is ``fleet.yaml``'s call.
        routes: Override the default routes for ``kind``.
        pricing / catalog: Billing and ``/v1/models`` metadata.
        harness: Whether the engine runs the beta9 harness (live tuning over RPC).
        preemptible: Whether serverless work may evict replicas (default). ``False`` holds the GPUs.
        drain_seconds: Grace for in-flight requests on eviction or replacement; ``0`` is immediate.
    """

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
        harness: bool = False,
        preemptible: bool = True,
        drain_seconds: int = 5,
        cpu: Union[int, float, str] = 4.0,
        memory: Union[int, str] = "16Gi",
        volumes: Optional[List[Union[Volume, CloudBucket]]] = None,
        secrets: Optional[List[str]] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> None:
        self.id = id
        self.kind = kind
        self.engine = engine
        self.port = int(port)
        self.health = health
        self.metrics = metrics
        self.routes = list(routes or [])
        self.pricing = pricing or Pricing()
        self.catalog = catalog or Catalog()
        self.harness = bool(harness)
        self.preemptible = bool(preemptible)
        self.drain_seconds = int(drain_seconds)
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
            ports=[self.port],
            name=id,
            app=id,
            keep_warm_seconds=-1,
        )
        self.parent = self
        # RunnerAbstraction owns ``self.gpu`` (a single GpuType); the per-type map lives here.
        self.gpus = _normalize_gpu(gpu)
        self.func = None
        if not self.image.override_python_version:
            self.image.ignore_python = True

    def spec(self) -> Dict[str, Any]:
        """The endpoint spec as the gateway validates it (pkg/types ManagedEndpointSpec)."""
        spec = _drop_empty(
            {
                "id": self.id,
                "kind": self.kind,
                "engine": self.engine,
                "port": self.port,
                "health": self.health,
                "metrics": self.metrics,
                "routes": self.routes,
                "pricing": self.pricing.to_dict(),
                "catalog": self.catalog.to_dict(),
                "harness": self.harness,
            }
        )
        # Always sent: an explicit False / 0 must not be pruned.
        spec["protected"] = not self.preemptible
        spec["drain_seconds"] = self.drain_seconds
        # A GPU key with no settings still declares the GPU; it must never be pruned.
        if self.gpus:
            spec["gpu"] = {key: _drop_empty(g.to_dict()) for key, g in self.gpus.items()}
        return spec

    def deploy(
        self,
        name: Optional[str] = None,
        context: Optional[ConfigContext] = None,
        invocation_details_func: Optional[Callable[..., None]] = None,
        git_sha: str = "",
        **_: Any,
    ) -> Tuple[Dict[str, Any], bool]:
        """Deploy this endpoint; ``name`` must match ``id`` when given.

        On failure ``deploy_error`` holds the reason (the deployer reports it).
        """
        self.deploy_error = ""
        if name and name != self.id:
            return self._fail(f"Deployment name {name!r} must match the endpoint id {self.id!r}.")
        self.name = self.id
        if context is not None:
            self.config_context = context
        if not self.entrypoint:
            return self._fail("You must specify an entrypoint.")

        image = self.image
        # Only an image the user supplied (base image, Dockerfile or explicit id)
        # skips code sync; an id produced by an earlier Image.build() does not.
        custom_image = bool(image.base_image or image.dockerfile or image._explicit_image_id)
        if not custom_image:
            # exec so SIGTERM from an eviction or drain reaches the engine, not a wrapper shell.
            self.entrypoint = [
                "sh",
                "-c",
                f"cd {USER_CODE_DIR} && exec {shlex.join(self.entrypoint)}",
            ]
        self.managed_endpoint = json.dumps({"endpoint": self.spec(), "git_sha": git_sha})

        if not self.prepare_runtime(
            stub_type=MANAGED_ENDPOINT_DEPLOYMENT_STUB_TYPE,
            force_create_stub=True,
            ignore_patterns=["**"] if custom_image else [],
        ):
            self.deploy_error = "stub preparation failed (see the deployer log)"
            return {}, False

        terminal.header("Deploying")
        resp: DeployStubResponse = self.gateway_stub.deploy_stub(
            DeployStubRequest(stub_id=self.stub_id, name=self.name, rollout="auto")
        )
        self.deployment_id = resp.deployment_id
        if not resp.ok:
            return self._fail(resp.err_msg or "deploy failed")
        terminal.done("Deployed 🎉")
        return {
            "deployment_id": resp.deployment_id,
            "version": resp.version,
            "id": self.id,
        }, True

    def _fail(self, reason: str) -> Tuple[Dict[str, Any], bool]:
        self.deploy_error = reason
        terminal.error(reason, exit=False)
        return {}, False
