"""
Managed endpoints: platform-hosted model servers declared in a git repo and
served through ``/v1``. An ``app.py`` exports one ``ManagedEndpoint`` that says
only how the engine runs; the repo's ``config.yaml`` publishes it (catalog,
access, pricing) and places it (which GPU types, priority, replica bounds).
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
    restart-class args and the live engine config. Where and how many replicas
    run is config.yaml's decision, not the app's."""

    count: int = 1
    engine_args: List[str] = field(default_factory=list)
    config: Dict[str, Any] = field(default_factory=dict)

    to_dict = asdict


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
    """A platform-hosted model server; ``id`` (``vendor/slug``) is also the app name.

    Parameters:
        id: ``vendor/slug``; also the model name callers send to ``/v1``.
        kind: ``llm`` | ``embedding`` | ``image`` | ``custom``; picks the routes served.
        image: Container image the engine runs in.
        entrypoint: Engine command; ``Gpu.engine_args`` for the placed GPU type are appended.
        engine: Engine name for validation/observability (``vllm``, ``sglang``...).
        port / health / metrics: Where the engine listens and its readiness / Prometheus paths.
        gpu: GPU types the engine can run on, optionally with per-type ``Gpu`` settings.
            ``config.yaml`` sets replica minimums, limits, priority, and preemption.
        drain_seconds: Grace for in-flight requests on eviction or replacement; ``0`` is immediate.
        rollout: ``wait_for_capacity`` preserves the last serving replica. ``replace`` allows
            downtime to release its GPU for a new version when there is no spare capacity.

    Catalog, access and pricing are not app settings: they live in ``config.yaml``.
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
        drain_seconds: int = 5,
        cpu: Union[int, float, str] = 4.0,
        memory: Union[int, str] = "16Gi",
        volumes: Optional[List[Union[Volume, CloudBucket]]] = None,
        secrets: Optional[List[str]] = None,
        env: Optional[Dict[str, str]] = None,
        rollout: str = "wait_for_capacity",
    ) -> None:
        self.id = id
        self.kind = kind
        self.engine = engine
        self.port = int(port)
        self.health = health
        self.metrics = metrics
        self.drain_seconds = int(drain_seconds)
        if rollout not in {"wait_for_capacity", "replace"}:
            raise ValueError("rollout must be wait_for_capacity or replace")
        self.rollout = rollout
        # A fresh Image per stub: `beta9 endpoints deploy` loads many apps in
        # one process, and Image.build() mutates the instance it was given.
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
        """The runtime spec as the gateway validates it (pkg/types ManagedEndpointSpec)."""
        spec = _drop_empty(
            {
                "id": self.id,
                "kind": self.kind,
                "engine": self.engine,
                "entrypoint": self.entrypoint,
                "port": self.port,
                "health": self.health,
                "metrics": self.metrics,
                "rollout": self.rollout,
            }
        )
        # Zero means immediate eviction and must not be pruned.
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
        **_: Any,
    ) -> Tuple[Dict[str, Any], bool]:
        """Deploy this endpoint; ``name`` must match ``id`` when given.

        An unchanged app maps to its existing stub, so redeploying it is a
        no-op rather than a rollout. On failure ``deploy_error`` holds the reason.
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
        self.managed_endpoint = json.dumps({"endpoint": self.spec()})

        if not self.prepare_runtime(
            stub_type=MANAGED_ENDPOINT_DEPLOYMENT_STUB_TYPE,
            ignore_patterns=["**"] if custom_image else [],
        ):
            self.deploy_error = "stub preparation failed (see the build log)"
            return {}, False

        terminal.header("Deploying")
        resp: DeployStubResponse = self.gateway_stub.deploy_stub(
            DeployStubRequest(stub_id=self.stub_id, name=self.name, rollout="auto")
        )
        self.deployment_id = resp.deployment_id
        if not resp.ok:
            return self._fail(resp.err_msg or "deploy failed")
        terminal.done("Unchanged" if resp.rollout_action == "unchanged" else "Deployed 🎉")
        return {
            "deployment_id": resp.deployment_id,
            "version": resp.version,
            "id": self.id,
            "stub_id": self.stub_id,
        }, True

    def _fail(self, reason: str) -> Tuple[Dict[str, Any], bool]:
        self.deploy_error = reason
        terminal.error(reason, exit=False)
        return {}, False
