"""Image registry access and Dockerfile build metadata."""

import re
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit

import requests

from .interpolation import interpolate

DOCKER_HUB_HOSTS = ("docker.io", "index.docker.io", "registry-1.docker.io")
SERVICE_REGISTRY_ACCEPT = ", ".join(
    [
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.docker.distribution.manifest.v2+json",
        "application/vnd.oci.image.manifest.v1+json",
    ]
)


def _registry_host(image: str) -> Optional[str]:
    """The registry an image reference names, or None for Docker Hub's short form."""
    first, _, rest = image.partition("/")
    if rest and ("." in first or ":" in first or first == "localhost"):
        return first
    return None


def parse_image(image: str) -> Tuple[str, str, str]:
    """(registry API host, repository, tag or digest) of an image reference."""
    name, _, digest = image.partition("@")
    tag = ""
    if ":" in name.rsplit("/", 1)[-1]:
        name, tag = name.rsplit(":", 1)
    host = _registry_host(name)
    registry, repository = (host, name[len(host) + 1 :]) if host else ("docker.io", name)
    if registry in DOCKER_HUB_HOSTS:
        registry = "registry-1.docker.io"
        if "/" not in repository:
            repository = "library/" + repository
    return registry, repository, digest or tag or "latest"


class Registry:
    """Reads public image configs anonymously; a private or unreachable image yields None."""

    def __init__(self, timeout: float = 8):
        self.session = requests.Session()
        self.timeout = timeout
        self.tokens: Dict[str, str] = {}
        self.configs: Dict[str, Optional[Dict[str, Any]]] = {}
        self.proxies: Dict[str, bool] = {}

    def docker_hub_image(self, image: str) -> Optional[str]:
        """The docker.io reference of an image whose registry only proxies Docker Hub
        (docker.langfuse.com, docker.n8n.io). Such a proxy pulls anonymously from one
        address, so the Docker Hub rate limit its users share is usually spent."""
        host = _registry_host(image)
        if host is None or host in DOCKER_HUB_HOSTS or host.startswith("localhost"):
            return None
        if host not in self.proxies:
            try:
                response = self.session.get(f"https://{host}/v2/", timeout=self.timeout)
                self.proxies[host] = (
                    urlsplit(response.url).hostname in DOCKER_HUB_HOSTS
                    or 'service="registry.docker.io"'
                    in response.headers.get("WWW-Authenticate", "")
                )
            except requests.RequestException:
                self.proxies[host] = False
        return "docker.io/" + image[len(host) + 1 :] if self.proxies[host] else None

    def config(self, image: str) -> Optional[Dict[str, Any]]:
        if image not in self.configs:
            try:
                source = self.docker_hub_image(image) or image
                self.configs[image] = self._config(*parse_image(source))
            except (requests.RequestException, ValueError, KeyError, TypeError):
                self.configs[image] = None
        return self.configs[image]

    def _config(self, registry: str, repository: str, reference: str) -> Optional[Dict[str, Any]]:
        manifest = self._get(registry, repository, f"manifests/{reference}", True).json()
        if "manifests" in manifest:
            chosen = next(
                (
                    entry
                    for entry in manifest["manifests"]
                    if entry.get("platform", {}).get("os") == "linux"
                    and entry.get("platform", {}).get("architecture") == "amd64"
                ),
                None,
            )
            if chosen is None:
                return None
            manifest = self._get(registry, repository, f"manifests/{chosen['digest']}", True).json()
        blob = self._get(registry, repository, f"blobs/{manifest['config']['digest']}").json()
        return blob.get("config") or {}

    def _get(self, registry: str, repository: str, path: str, manifest: bool = False):
        url = f"https://{registry}/v2/{repository}/{path}"
        headers = {"Accept": SERVICE_REGISTRY_ACCEPT} if manifest else {}
        scope = f"{registry}/{repository}"
        if scope in self.tokens:
            headers["Authorization"] = f"Bearer {self.tokens[scope]}"
        response = self.session.get(url, headers=headers, timeout=self.timeout)
        if response.status_code == 401 and scope not in self.tokens:
            token = self._token(response.headers.get("WWW-Authenticate", ""), repository)
            if token:
                self.tokens[scope] = token
                headers["Authorization"] = f"Bearer {token}"
                response = self.session.get(url, headers=headers, timeout=self.timeout)
        response.raise_for_status()
        return response

    def _token(self, challenge: str, repository: str) -> Optional[str]:
        if not challenge.lower().startswith("bearer "):
            return None
        params = dict(re.findall(r'(\w+)="([^"]*)"', challenge))
        realm = params.pop("realm", "")
        if not realm:
            return None
        params.setdefault("scope", f"repository:{repository}:pull")
        response = self.session.get(realm, params=params, timeout=self.timeout)
        response.raise_for_status()
        body = response.json()
        return body.get("token") or body.get("access_token")


def exposed_ports(config: Optional[Dict[str, Any]]) -> List[int]:
    """The TCP ports an image config EXPOSEs, lowest first."""
    return sorted(
        int(port.split("/")[0])
        for port in (config or {}).get("ExposedPorts") or {}
        if port.endswith("/tcp") and port.split("/")[0].isdigit()
    )


def _exec_form(value: str) -> List[str]:
    if value.startswith("["):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [str(item) for item in parsed]
        except ValueError:
            pass
    return ["/bin/sh", "-c", value]


def _dockerfile_instructions(text: str) -> List[Tuple[str, str]]:
    instructions: List[Tuple[str, str]] = []
    pending = ""
    for raw in text.splitlines():
        line = raw.strip()
        if not pending and (not line or line.startswith("#")):
            continue
        if line.startswith("#"):
            continue
        if line.endswith("\\"):
            pending += line[:-1] + " "
            continue
        line = (pending + line).strip()
        pending = ""
        keyword, _, value = line.partition(" ")
        instructions.append((keyword.upper(), value.strip()))
    return instructions


def dockerfile_config(
    path: Path, target: Optional[str], build_args: Dict[str, str], registry: Registry
) -> Tuple[Dict[str, Any], bool]:
    """The image config a Dockerfile build produces, and whether its base images resolved."""
    args = dict(build_args)
    stages: List[Tuple[Optional[str], Dict[str, Any]]] = []
    config: Optional[Dict[str, Any]] = None
    resolved = True
    for keyword, value in _dockerfile_instructions(path.read_text()):
        if keyword == "ARG":
            name, _, default = value.partition("=")
            args.setdefault(name.strip(), default.strip().strip("\"'"))
        elif keyword == "FROM":
            words = [word for word in value.split() if not word.startswith("--")]
            base = interpolate(words[0], args, set()) if words else "scratch"
            name = words[2].lower() if len(words) >= 3 and words[1].lower() == "as" else None
            inherited = next((dict(c) for n, c in stages if n == base.lower()), None)
            if inherited is None and base != "scratch":
                inherited = registry.config(base)
                resolved = resolved and inherited is not None
            config = dict(inherited or {})
            stages.append((name, config))
        elif config is not None:
            value = interpolate(value, args, set())
            if keyword == "ENTRYPOINT":
                config["Entrypoint"], config["Cmd"] = _exec_form(value), None
            elif keyword == "CMD":
                config["Cmd"] = _exec_form(value)
            elif keyword == "EXPOSE":
                exposed = dict(config.get("ExposedPorts") or {})
                exposed.update(
                    {port if "/" in port else f"{port}/tcp": {} for port in value.split()}
                )
                config["ExposedPorts"] = exposed
            elif keyword == "HEALTHCHECK" and " CMD " in f" {value} ":
                config["Healthcheck"] = {"Test": ["CMD-SHELL", value.split("CMD", 1)[1].strip()]}
            elif keyword == "WORKDIR":
                config["WorkingDir"] = value
    if target:
        chosen = next((c for n, c in stages if n == target.lower()), None)
        if chosen is not None:
            return chosen, resolved
    return (stages[-1][1] if stages else {}), resolved


def resolve_image(
    options: Dict[str, Any], directory: str, registry: Registry, infer_ports: bool = True
) -> Tuple[Dict[str, Any], List[str]]:
    """Deploy options completed from the image itself, and notes for the agent: an
    image behind a Docker Hub proxy is pulled from Docker Hub, and omitted ports are
    the EXPOSE of the image or built stage, base images included."""
    options, notes = dict(options), []
    image = options.get("image")
    if image:
        hub = registry.docker_hub_image(str(image))
        if hub:
            notes.append(
                f"Pulling {hub}: {_registry_host(str(image))} only proxies Docker Hub, and its anonymous pulls share one rate limit."
            )
            options["image"] = image = hub
    if not infer_ports or "ports" in options or options.get("handler"):
        return options, notes

    if image:
        source, config = str(image), registry.config(str(image))
    else:
        dockerfile = options.get("dockerfile") or "Dockerfile"
        path = Path(directory, dockerfile)
        if not path.is_file():
            return options, notes
        source = dockerfile
        built, resolved = dockerfile_config(
            path, options.get("target"), options.get("build_args") or {}, registry
        )
        config = built if resolved or exposed_ports(built) else None
    ports = exposed_ports(config)
    if ports:
        options["ports"] = ports
        notes.append(
            f"Ports {ports} come from {source}'s EXPOSE"
            + (f"; the URL and health checks use {ports[0]}." if len(ports) > 1 else ".")
        )
    elif config is not None:
        notes.append(
            f"{source} EXPOSEs no port, so the app listens on 8000 (PORT is set to it); pass ports if it binds another, or ports: [] for a worker."
        )
    else:
        notes.append(
            f"{source}'s EXPOSE is unreadable (a private or unreachable image), so the app gets port 8000 unless you pass ports."
        )
    return options, notes
