import os
from typing import Dict, List, Optional, Union

from ..type import GpuType, GpuTypeAlias, Pool, QueueDepthAutoscaler
from .base.container import Container
from .image import Image
from .pod import Pod, PodInstance

DEFAULT_SERVICE_PORT = 8000


def ports_from_dockerfile(image: Optional[Image]) -> List[int]:
    dockerfile = getattr(image, "dockerfile", "") or ""
    ports: List[int] = []

    for raw_line in dockerfile.splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line.upper().startswith("EXPOSE "):
            continue

        for token in line.split()[1:]:
            port_text = token.split("/", 1)[0]
            if not port_text.isdigit():
                continue

            port = int(port_text)
            if port not in ports:
                ports.append(port)

    return ports


def resolve_service_ports(
    *,
    port: Optional[int] = None,
    ports: Optional[List[int]] = None,
    image: Optional[Image] = None,
    default: bool = False,
) -> List[int]:
    service_ports = list(ports or [])
    if port is not None and port not in service_ports:
        service_ports.append(port)
    if service_ports:
        return service_ports

    service_ports = ports_from_dockerfile(image)
    if service_ports:
        return service_ports

    return [DEFAULT_SERVICE_PORT] if default else []


def service_image_implies_default_port(image: Optional[Image]) -> bool:
    return bool(
        getattr(image, "dockerfile", "")
        or getattr(image, "dockerfile_path", "")
        or getattr(image, "base_image", "")
        or getattr(image, "image_id", "")
    )


class Service(Pod):
    """
    Service runs arbitrary long-lived web services and background processes.

    This is a developer-experience wrapper around Pod deployments. It keeps the
    existing pod runtime behavior while exposing a hosting-oriented API for apps
    that already have a Dockerfile, registry image, or process command.
    """

    def __init__(
        self,
        app: str = "",
        command: Optional[Union[str, List[str]]] = None,
        entrypoint: Optional[List[str]] = None,
        port: Optional[int] = None,
        ports: Optional[List[int]] = None,
        name: Optional[str] = None,
        cpu: Union[int, float, str] = 1.0,
        memory: Union[int, str] = 512,
        gpu: Union[GpuTypeAlias, List[GpuTypeAlias]] = GpuType.NoGPU,
        gpu_count: int = 0,
        image: Image = Image(),
        volumes: Optional[List] = None,
        secrets: Optional[List[str]] = None,
        env: Optional[Dict[str, str]] = None,
        keep_warm_seconds: int = 0,
        min_replicas: int = 0,
        max_replicas: Optional[int] = None,
        always_on: bool = False,
        authorized: bool = False,
        tcp: bool = False,
        block_network: bool = False,
        allow_list: Optional[List[str]] = None,
        docker_enabled: bool = False,
        pool: Optional[Union[str, Pool]] = None,
    ) -> None:
        if command is not None and entrypoint:
            raise ValueError("Specify either command or entrypoint, not both.")

        service_ports = resolve_service_ports(
            port=port,
            ports=ports,
            image=image,
            default=service_image_implies_default_port(image),
        )

        service_env = dict(env or {})
        if service_ports and "PORT" not in service_env:
            service_env["PORT"] = str(service_ports[0])

        super().__init__(
            app=app,
            entrypoint=entrypoint or self._command_to_entrypoint(command),
            ports=service_ports,
            name=name,
            cpu=cpu,
            memory=memory,
            gpu=gpu,
            gpu_count=gpu_count,
            image=image,
            volumes=volumes,
            secrets=secrets,
            env=service_env,
            keep_warm_seconds=keep_warm_seconds,
            authorized=authorized,
            tcp=tcp,
            block_network=block_network,
            allow_list=allow_list,
            docker_enabled=docker_enabled,
            pool=pool,
        )
        self.configure_replicas(
            min_replicas=min_replicas,
            max_replicas=max_replicas,
            always_on=always_on,
        )

    def configure_replicas(
        self,
        *,
        min_replicas: Optional[int] = None,
        max_replicas: Optional[int] = None,
        always_on: Optional[bool] = None,
    ) -> None:
        if always_on is False and min_replicas is None:
            min_replicas = 0

        min_replicas = self._replica_value(
            "min_replicas",
            min_replicas,
            default=getattr(self, "min_replicas", 0),
        )
        if always_on:
            min_replicas = max(min_replicas, 1)

        if max_replicas is None:
            max_replicas = getattr(self, "max_replicas", max(1, min_replicas))
            max_replicas = max(max_replicas, min_replicas)
        else:
            max_replicas = self._replica_value(
                "max_replicas",
                max_replicas,
                default=1,
                minimum=1,
            )

        if max_replicas < min_replicas:
            raise ValueError("max_replicas must be greater than or equal to min_replicas")

        self.min_replicas = min_replicas
        self.max_replicas = max_replicas
        self.always_on = min_replicas > 0
        self.autoscaler = QueueDepthAutoscaler(
            min_containers=min_replicas,
            max_containers=max_replicas,
        )

    @staticmethod
    def _replica_value(
        name: str,
        value: Optional[int],
        default: int,
        minimum: int = 0,
    ) -> int:
        if value is None:
            return default
        if value < minimum:
            raise ValueError(f"{name} must be greater than or equal to {minimum}")
        return value

    @staticmethod
    def _command_to_entrypoint(command: Optional[Union[str, List[str]]]) -> List[str]:
        if command is None:
            return []
        if isinstance(command, str):
            return ["sh", "-lc", command]
        return list(command)

    @classmethod
    def from_dockerfile(
        cls,
        dockerfile: str = "Dockerfile",
        context_dir: Optional[str] = None,
        **kwargs,
    ) -> "Service":
        image = Image.from_dockerfile(dockerfile, context_dir=context_dir)
        image.dockerfile_path = dockerfile
        image.ignore_python = True
        return cls(image=image, **kwargs)

    @classmethod
    def from_registry(cls, image_uri: str, **kwargs) -> "Service":
        return cls(image=Image.from_registry(image_uri), **kwargs)

    def serve(self, sync_dir: Optional[str] = None) -> Optional[PodInstance]:
        """
        Start one service container and attach to logs, optionally syncing files.
        """
        result = self.create()
        if not result.ok:
            return result

        Container(container_id=result.container_id).attach(
            container_id=result.container_id,
            sync_dir=sync_dir or os.getcwd(),
        )
        return result

    def generate_deployment_artifacts(self, **kwargs):
        return None

    def cleanup_deployment_artifacts(self):
        return None
