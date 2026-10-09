"""Translate a docker-compose project into a draft stack spec for stack_plan.

Compose services share a private network and find each other by service name;
on Beam every app is reached through its public address. The translation
rewrites those addresses into references, maps postgres and redis images onto
managed databases, named volumes onto durable disks, and placeholder
credentials onto generated stack secrets. Whatever it cannot translate
faithfully becomes a warning.
"""

import hashlib
import json
import re
import shlex
import shutil
import tempfile
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

import requests
import yaml

COMPOSE_FILES = ("compose.yaml", "compose.yml", "docker-compose.yaml", "docker-compose.yml")
VARIABLE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
DISK_SIZE = "10Gi"
DEFAULT_MEMORY = "1Gi"
HEX = "0123456789abcdef"
LOCAL_HOSTS = {"localhost", "127.0.0.1", "0.0.0.0", "[::1]", "host.docker.internal"}
DEFAULT_PORTS = {"http": 80, "https": 443, "ws": 80, "wss": 443}
URL = re.compile(
    r"(?P<scheme>[A-Za-z][A-Za-z0-9+.-]*)://(?:(?P<userinfo>[^@/\s?#]*)@)?"
    r"(?P<host>\[[^\]\s]+\]|[A-Za-z0-9_.-]+)(?::(?P<port>\d+))?"
)
HOST_PORT = re.compile(r"(?<![\w.@/:-])(?P<host>[A-Za-z0-9_.-]+):(?P<port>\d{2,5})(?![\w.:-])")
HOST_KEY = re.compile(r"HOST|SERVER|ADDR|ENDPOINT|DOMAIN|BROKER|NODES?$")
SECRET_KEY = re.compile(r"PASSWORD|PASSWD|SECRET|TOKEN(?!S)|SALT|(?<![A-Z])AUTH$|_KEY$|^KEY$")
NOT_SECRET_KEY = re.compile(
    r"(_ID|_FILE|_PATH|_URL|_URI|_HOST|_PORT|_USER|_USERNAME|_ENABLED|_TYPE|_NAME|PUBLIC_KEY"
    r"|_EXPIRY|_TTL|_LENGTH|_HEADER)$"
)
PLACEHOLDERS = re.compile(
    r"change|secret|passw|example|default|placeholder|dummy|test|replace|random|your|xxx|todo"
    r"|fixme|generate|insert|<",
    re.I,
)
CREDENTIAL_WORDS = {"SECRET", "ACCESS", "KEY", "PASSWORD", "PASS", "PWD", "TOKEN", "AUTH"}
TEMPLATE_NAMES = {"base", "common", "default", "defaults", "template", "shared"}
# Credentials a server image refuses to start without.
SERVER_CREDENTIALS = {
    "POSTGRES_PASSWORD",
    "MYSQL_ROOT_PASSWORD",
    "MYSQL_PASSWORD",
    "MARIADB_ROOT_PASSWORD",
    "MARIADB_PASSWORD",
    "MONGO_INITDB_ROOT_PASSWORD",
    "RABBITMQ_DEFAULT_PASS",
    "MINIO_ROOT_PASSWORD",
    "CLICKHOUSE_PASSWORD",
    "ELASTIC_PASSWORD",
}
# A variable with no default: compose requires it, or the app ships without it.
REQUIRED_VARIABLE = re.compile(r"\$(?:\{([A-Za-z_]\w*)(?::?\?[^}]*)?\}|([A-Za-z_]\w*))")
OWN_URL_KEY = re.compile(
    r"(?:^|_)(?:BASE|PUBLIC|SITE|APP|ROOT|EXTERNAL|SERVER|NEXTAUTH|WEBAPP|WEBHOOK|HOST)_?URL$"
)
DATABASE_URL_KEYS = {
    "postgres": re.compile(r"(?:DATABASE|POSTGRES|POSTGRESQL|PG|DB)_UR[LI]"),
    "redis": re.compile(r"(?:REDIS|CACHE|QUEUE)_URL"),
}
SERVICE_REGISTRY_ACCEPT = ", ".join(
    [
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.docker.distribution.manifest.v2+json",
        "application/vnd.oci.image.manifest.v1+json",
    ]
)
# Official images whose clients only need an address and credentials. Builds
# with extensions (pgvector, postgis, timescale) keep running as containers.
MANAGED_IMAGES = {
    "library/postgres": "postgres",
    "bitnami/postgresql": "postgres",
    "library/redis": "redis",
    "bitnami/redis": "redis",
    "valkey/valkey": "redis",
    "bitnami/valkey": "redis",
}
MANAGED_PORTS = {"postgres": 5432, "redis": 6379}
# Health endpoints and sizes compose files rarely state, by image name.
KNOWN_IMAGES: Dict[str, Dict[str, Any]] = {
    "minio": {"health": ("/minio/health/live", 9000)},
    "clickhouse-server": {"health": ("/ping", 8123), "cpu": 2, "memory": "4Gi"},
    "grafana": {"health": ("/api/health", 3000)},
    "prometheus": {"health": ("/-/ready", 9090)},
    "qdrant": {"health": ("/healthz", 6333)},
    "meilisearch": {"health": ("/health", 7700)},
    "typesense": {"health": ("/health", 8108)},
    "elasticsearch": {"cpu": 2, "memory": "4Gi"},
    "opensearch": {"cpu": 2, "memory": "4Gi"},
}
# Compose keys with no Beam equivalent that change how a service behaves.
UNSUPPORTED_KEYS = {
    "privileged": "privileged mode is not available",
    "cap_add": "added capabilities are not available",
    "devices": "host devices are not available",
    "network_mode": "network_mode is ignored; apps reach each other through references",
    "pid": "pid namespaces are not shared",
    "ipc": "ipc namespaces are not shared",
    "user": "runs as the image's default user",
    "sysctls": "sysctls are not applied",
    "ulimits": "ulimits are not applied",
    "shm_size": "shm_size is not applied",
    "extra_hosts": "extra_hosts are not applied",
    "secrets": "compose secrets are files; pass the values with workspace secrets instead",
    "configs": "compose configs are files; bake them into the image instead",
    "extends": "extends is not resolved; merge the base service by hand",
    "security_opt": "security_opt is not applied",
}


class ComposeError(ValueError):
    pass


class _ComposeLoader(yaml.SafeLoader):
    """YAML 1.2 scalars, as compose reads them: `22:22` stays a string, `yes` is not a bool."""


_ComposeLoader.yaml_implicit_resolvers = {
    first: [
        (tag, pattern)
        for tag, pattern in resolvers
        if tag not in ("tag:yaml.org,2002:int", "tag:yaml.org,2002:float", "tag:yaml.org,2002:bool")
    ]
    for first, resolvers in yaml.SafeLoader.yaml_implicit_resolvers.items()
}
_ComposeLoader.add_implicit_resolver(
    "tag:yaml.org,2002:bool", re.compile(r"^(?:true|True|TRUE|false|False|FALSE)$"), list("tTfF")
)
_ComposeLoader.add_implicit_resolver(
    "tag:yaml.org,2002:int", re.compile(r"^[-+]?[0-9]+$"), list("-+0123456789")
)
_ComposeLoader.add_implicit_resolver(
    "tag:yaml.org,2002:float",
    re.compile(r"^[-+]?(?:\.[0-9]+|[0-9]+\.[0-9]*)(?:[eE][-+]?[0-9]+)?$"),
    list("-+0123456789."),
)


class _Override:
    """A value tagged !override replaces what earlier files set instead of merging with it."""

    def __init__(self, value: Any):
        self.value = value


_RESET = object()


def _construct_override(loader: yaml.SafeLoader, node: yaml.Node) -> _Override:
    if isinstance(node, yaml.MappingNode):
        return _Override(loader.construct_mapping(node, deep=True))
    if isinstance(node, yaml.SequenceNode):
        return _Override(loader.construct_sequence(node, deep=True))
    return _Override(loader.construct_scalar(node))


_ComposeLoader.add_constructor("!override", _construct_override)
_ComposeLoader.add_constructor("!reset", lambda loader, node: _RESET)

# Merged as mappings even when written as KEY=VALUE lists.
MAPPING_LISTS = {"environment", "labels", "extra_hosts", "sysctls", "annotations", "args"}
REPLACED = {"command", "entrypoint", "test"}


def _as_mapping(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    mapping: Dict[str, Any] = {}
    for item in value or []:
        key, separator, rest = str(item).partition("=")
        mapping[key] = rest if separator else None
    return mapping


def _plain(value: Any) -> Any:
    if isinstance(value, _Override):
        return _plain(value.value)
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items() if v is not _RESET}
    if isinstance(value, list):
        return [_plain(v) for v in value if v is not _RESET]
    return value


def _volume_target(entry: Any) -> Any:
    if isinstance(entry, dict):
        return entry.get("target")
    parts = str(entry).split(":")
    return parts[1] if len(parts) > 1 else parts[0]


def merge_documents(base: Any, other: Any, key: Optional[str] = None) -> Any:
    """Merge a later compose file over an earlier one, as `docker compose -f a -f b` does."""
    if isinstance(other, _Override):
        return _plain(other.value)
    if key in MAPPING_LISTS and base is not None:
        merged = _as_mapping(base)
        for name, value in _as_mapping(other).items():
            if value is _RESET:
                merged.pop(name, None)
            else:
                merged[name] = _plain(value)
        return merged
    if isinstance(base, dict) and isinstance(other, dict):
        merged = dict(base)
        for name, value in other.items():
            if value is _RESET:
                merged.pop(name, None)
            elif name in merged:
                merged[name] = merge_documents(merged[name], value, name)
            else:
                merged[name] = _plain(value)
        return merged
    if key not in REPLACED and isinstance(base, list) and isinstance(other, list):
        later = [_plain(v) for v in other if v is not _RESET]
        if key == "volumes":
            targets = {_volume_target(v) for v in later}
            return [v for v in base if _volume_target(v) not in targets] + later
        return base + [v for v in later if v not in base]
    return _plain(other)


def find_compose_file(path: str) -> Path:
    candidate = Path(path).expanduser().resolve()
    if candidate.is_file():
        return candidate
    if candidate.is_dir():
        for name in COMPOSE_FILES:
            if (candidate / name).is_file():
                return candidate / name
    raise ComposeError(f"no compose file at {candidate} (looked for {', '.join(COMPOSE_FILES)})")


def read_env_file(path: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    if not path.is_file():
        return values
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = re.sub(r"^export\s+", "", key.strip())
        value = value.strip()
        quoted = re.match(r"""(["'])(.*?)\1(\s+#.*)?$""", value)
        if quoted:
            value = quoted.group(2)
        elif " #" in value:
            value = value.split(" #", 1)[0].rstrip()
        values[key] = value
    return values


def example_env_file(path: Path) -> Optional[Path]:
    """path, or the example a project ships for it (.env.example) when path is missing;
    copying the example is the first step of most self-hosting guides."""
    if path.is_file():
        return path
    stems = [path.name] + ([path.name[1:]] if path.name.startswith(".") else [])
    names = [
        stem + suffix for stem in stems for suffix in (".example", ".sample", ".template", ".dist")
    ]
    if path.name.startswith("."):
        names.append("example" + path.name)
    for name in names:
        if (path.parent / name).is_file():
            return path.parent / name
    return None


def interpolate(text: str, variables: Dict[str, str], missing: Set[str]) -> str:
    """Compose interpolation: $VAR, ${VAR}, ${VAR:-default}, ${VAR-default},
    ${VAR:?error}, ${VAR:+alternative}, nested defaults, and $$ for a literal $."""
    out: List[str] = []
    i = 0
    while i < len(text):
        if text[i] != "$":
            out.append(text[i])
            i += 1
        elif text.startswith("$$", i):
            out.append("$")
            i += 2
        elif text.startswith("${", i):
            end = _closing_brace(text, i + 1)
            if end < 0:
                out.append(text[i:])
                break
            out.append(_expand(text[i + 2 : end], variables, missing))
            i = end + 1
        else:
            match = VARIABLE.match(text, i + 1)
            if match:
                out.append(_lookup(match.group(), variables, missing))
                i = match.end()
            else:
                out.append("$")
                i += 1
    return "".join(out)


def _closing_brace(text: str, start: int) -> int:
    depth = 0
    for index in range(start, len(text)):
        if text[index] == "{":
            depth += 1
        elif text[index] == "}":
            depth -= 1
            if depth == 0:
                return index
    return -1


def _lookup(name: str, variables: Dict[str, str], missing: Set[str]) -> str:
    if name not in variables:
        missing.add(name)
    return variables.get(name, "")


def _expand(expression: str, variables: Dict[str, str], missing: Set[str]) -> str:
    match = VARIABLE.match(expression)
    if not match:
        return ""
    name, rest = match.group(), expression[match.end() :]
    value = variables.get(name)
    for operator in (":-", ":?", ":+", "-", "?", "+"):
        if rest.startswith(operator):
            empty = value is None or (operator.startswith(":") and value == "")
            if operator.endswith("?"):
                if empty:
                    missing.add(name)
                return value or ""
            if empty == operator.endswith("-"):
                return interpolate(rest[len(operator) :], variables, missing)
            return value or ""
    return _lookup(name, variables, missing)


def _interpolate_tree(node: Any, variables: Dict[str, str], missing: Set[str]) -> Any:
    if isinstance(node, str):
        return interpolate(node, variables, missing)
    if isinstance(node, list):
        return [_interpolate_tree(item, variables, missing) for item in node]
    if isinstance(node, dict):
        return {key: _interpolate_tree(value, variables, missing) for key, value in node.items()}
    return node


def parse_image(image: str) -> Tuple[str, str, str]:
    """(registry API host, repository, tag or digest) of an image reference."""
    name, _, digest = image.partition("@")
    tag = ""
    if ":" in name.rsplit("/", 1)[-1]:
        name, tag = name.rsplit(":", 1)
    parts = name.split("/")
    if len(parts) > 1 and ("." in parts[0] or ":" in parts[0] or parts[0] == "localhost"):
        registry, repository = parts[0], "/".join(parts[1:])
    else:
        registry, repository = "docker.io", name
    if registry in ("docker.io", "index.docker.io", "registry-1.docker.io"):
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

    def config(self, image: str) -> Optional[Dict[str, Any]]:
        if image not in self.configs:
            try:
                self.configs[image] = self._config(*parse_image(image))
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


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-") or "app"


def _env_name(value: str) -> str:
    return re.sub(r"[^A-Z0-9]+", "_", value.upper()).strip("_")


def _scalar(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _command(value: Any) -> Optional[List[str]]:
    if value is None:
        return None
    if isinstance(value, list):
        return [str(item) for item in value]
    return shlex.split(str(value))


def _memory(value: Any) -> Optional[str]:
    match = re.fullmatch(r"\s*([0-9.]+)\s*([kmgt]?)i?b?\s*", str(value), re.I)
    if not match:
        return None
    scale = {"": 1 / (1 << 20), "k": 1 / 1024, "m": 1, "g": 1024, "t": 1 << 20}
    mebibytes = max(1, round(float(match.group(1)) * scale[match.group(2).lower()]))
    return f"{mebibytes // 1024}Gi" if mebibytes % 1024 == 0 else f"{mebibytes}Mi"


def _parse_ports(entries: Iterable[Any]) -> Tuple[List[int], Dict[int, int], Set[int], List[str]]:
    """Container ports, published host port -> container port, container ports published
    beyond loopback, and problems."""
    container: List[int] = []
    published: Dict[int, int] = {}
    public: Set[int] = set()
    problems: List[str] = []
    for entry in entries or []:
        if isinstance(entry, dict):
            target, host, protocol, address = (
                entry.get("target"),
                entry.get("published"),
                entry.get("protocol"),
                str(entry.get("host_ip") or ""),
            )
        else:
            text, _, protocol = str(entry).partition("/")
            bracketed = re.match(r"^\[([^\]]*)\]:", text)
            parts = text[bracketed.end() :].split(":") if bracketed else text.split(":")
            target, host = parts[-1], parts[-2] if len(parts) > 1 else None
            address = bracketed.group(1) if bracketed else parts[0] if len(parts) > 2 else ""
        if protocol and protocol != "tcp":
            problems.append(f"port {entry}: only TCP is supported")
            continue
        targets = _port_range(target)
        hosts = _port_range(host) if host not in (None, "") else [None] * len(targets)
        if not targets or len(hosts) != len(targets):
            problems.append(f"port {entry}: not understood")
            continue
        for container_port, host_port in zip(targets, hosts):
            if container_port not in container:
                container.append(container_port)
            if host_port is not None:
                published[host_port] = container_port
                if address not in ("127.0.0.1", "localhost", "::1"):
                    public.add(container_port)
    return container, published, public, problems


def _port_range(value: Any) -> List[Any]:
    text = str(value).strip()
    if re.fullmatch(r"\d+", text):
        return [int(text)]
    match = re.fullmatch(r"(\d+)-(\d+)", text)
    if match and 0 <= int(match.group(2)) - int(match.group(1)) < 32:
        return list(range(int(match.group(1)), int(match.group(2)) + 1))
    return []


def _parse_volume(entry: Any) -> Tuple[str, str, str, bool]:
    """(kind, source, target, read_only) with kind volume, bind, or anonymous."""
    if isinstance(entry, dict):
        kind = entry.get("type", "volume")
        source, target = str(entry.get("source") or ""), str(entry.get("target") or "")
        if kind == "volume" and not source:
            kind = "anonymous"
        return kind, source, target, bool(entry.get("read_only"))
    parts = str(entry).split(":")
    if len(parts) == 1:
        return "anonymous", "", parts[0], False
    source, target = parts[0], parts[1]
    read_only = len(parts) > 2 and "ro" in parts[2].split(",")
    bind = source.startswith((".", "/", "~", "$"))
    return ("bind" if bind else "volume"), source, target, read_only


def _secret_format(key: str, value: str) -> Dict[str, Any]:
    """Length and alphabet for a generated secret replacing value (a placeholder or empty);
    a placeholder often says what it wants, as in "replace-with-a-64-character-hex-string"."""
    text = value.lower()
    stated = next((int(n) for n in re.findall(r"\d+", text) if 16 <= int(n) <= 512), None)
    if len(value) >= 16 and all(char in HEX for char in text):
        return {"length": len(value), "alphabet": HEX}
    if "hex" in text or (not value and "ENCRYPTION" in key.upper()):
        return {"length": stated or 64, "alphabet": HEX}
    if not value:
        return {"length": 64}
    return {"length": stated or max(32, len(value))}


def _extends(child: Any, base: Dict[str, Any]) -> bool:
    return (
        isinstance(child, dict)
        and bool(base)
        and all(key in child for key in base)
        and child.get("image") == base.get("image")
        and child.get("build") == base.get("build")
    )


def _environment_mapping(node: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """A service's `environment` as KEY -> value, None for a pass-through KEY."""
    environment = node.get("environment") or {}
    if isinstance(environment, list):
        environment = dict(
            (entry.split("=", 1) if "=" in entry else (entry, None))
            for entry in map(str, environment)
        )
    return {str(key): _scalar(value) for key, value in environment.items()}


def _health_url(test: Any, aliases: Set[str]) -> Optional[Tuple[str, int]]:
    if isinstance(test, list):
        if not test or test[0] == "NONE":
            return None
        text = " ".join(str(word) for word in test[1:])
    else:
        text = str(test or "")
    hosts = "|".join(re.escape(host) for host in sorted(LOCAL_HOSTS | aliases | {"$$(hostname)"}))
    match = re.search(rf"https?://(?:{hosts})(?::(\d+))?(/[^\s'\"\\|;&)]*)?", text)
    if not match:
        return None
    return match.group(2) or "/", int(match.group(1) or 80)


class Translator:
    def __init__(
        self,
        root: Path,
        document: Dict[str, Any],
        variables: Dict[str, str],
        prefix: Optional[str],
        profiles: Iterable[str],
        registry: Registry,
        build_root: Optional[Path],
        raw: Optional[Dict[str, Any]] = None,
    ):
        self.root = root
        self.document = document
        self.raw_services = (raw or {}).get("services") or {}
        self.variables = variables
        self.registry = registry
        self.build_root = build_root
        self.prefix = _slug(prefix or document.get("name") or root.name)
        self.warnings: List[str] = []
        self.secrets: Dict[str, Dict[str, Any]] = {}
        selected = set(profiles or [])
        services = document.get("services")
        if not isinstance(services, dict) or not services:
            raise ComposeError("the compose file defines no services")
        self.skipped = [
            name
            for name, node in services.items()
            if (node or {}).get("profiles") and not selected & set(node["profiles"])
        ]
        # A proxy or updater driving other containers through the Docker socket cannot run.
        self.dockerized = [
            name
            for name, node in services.items()
            if name not in self.skipped
            and any("docker.sock" in str(entry) for entry in (node or {}).get("volumes") or [])
        ]
        # A `base: &base` service others merge (`<<: *base`) only to share settings.
        self.templates = [
            name
            for name, node in services.items()
            if name in TEMPLATE_NAMES
            and not set(node or {}) & {"ports", "expose", "command", "entrypoint"}
            and any(_extends(other, node or {}) for key, other in services.items() if key != name)
            and not any(
                name in ((other or {}).get("depends_on") or []) for other in services.values()
            )
        ]
        left_out = set(self.skipped) | set(self.dockerized) | set(self.templates)
        self.services = {
            name: node or {} for name, node in services.items() if name not in left_out
        }
        if not self.services:
            raise ComposeError("no service can run without the Docker socket or inactive profiles")
        self.names = {name: self._app_name(name) for name in self.services}
        self.aliases: Dict[str, str] = {}
        for name, node in self.services.items():
            for alias in [
                name,
                node.get("hostname"),
                node.get("container_name"),
            ] + self._network_aliases(node):
                if alias:
                    self.aliases.setdefault(str(alias), name)
        self.ports: Dict[str, List[int]] = {}
        self.public: Dict[str, Set[int]] = {}
        self.published: Dict[int, Tuple[str, int]] = {}
        self.kinds: Dict[str, str] = {}
        self.managed: Dict[str, str] = {}
        self.configs: Dict[str, Optional[Dict[str, Any]]] = {}
        self.needed: Dict[str, Set[int]] = {name: set() for name in self.services}
        self.named_by_host: Set[str] = set()
        self.disk_owners: Dict[str, str] = {}
        # Env keys whose value is an unset variable with no default, by service.
        self.unset: Dict[str, Dict[str, str]] = {name: {} for name in self.services}
        # Env keys declared without a value (pass-through or empty), by service.
        self.declared: Dict[str, Set[str]] = {name: set() for name in self.services}
        self.filled: Set[str] = set()
        self.file_references: Set[Tuple[str, str]] = set()

    def warn(self, service: Optional[str], message: str) -> None:
        self.warnings.append(f"{service}: {message}" if service else message)

    def _app_name(self, service: str) -> str:
        name = _slug(service)
        return (
            name
            if name == self.prefix or name.startswith(self.prefix + "-")
            else f"{self.prefix}-{name}"
        )

    @staticmethod
    def _network_aliases(node: Dict[str, Any]) -> List[str]:
        networks = node.get("networks")
        if not isinstance(networks, dict):
            return []
        return [
            alias for network in networks.values() for alias in (network or {}).get("aliases", [])
        ]

    def translate(self) -> Dict[str, Any]:
        for service, node in self.services.items():
            ports, published, public, problems = _parse_ports(node.get("ports"))
            for port in node.get("expose") or []:
                ports += [p for p in _port_range(str(port).split("/")[0]) if p not in ports]
            self.ports[service] = ports
            labels = node.get("labels") or {}
            routed = any(
                re.match(r"(traefik\.http\.routers\.|caddy)", str(label))
                for label in (labels if isinstance(labels, list) else labels.keys())
            )
            self.public[service] = set(ports) if routed else public
            for host_port, container_port in published.items():
                self.published[host_port] = (service, container_port)
            for problem in problems:
                self.warn(service, problem)
            self.kinds[service] = self._kind(service, node)

        environments = {
            service: self._environment(service, node) for service, node in self.services.items()
        }
        literals = {
            service: self._database_literals(service, environments[service])
            for service in self.services
            if self.kinds[service] == "database"
        }
        for service in self.services:
            if self.kinds[service] != "database":
                environments[service] = self._rewrite_environment(
                    service, environments[service], literals
                )
        self._fill_unset(environments)
        self._share_secrets(environments)
        self._scan_bound_files()
        unreferenced = self._unreferenced_dependencies(environments)
        # The app reaches them at an address built into it, so they must listen somewhere.
        self.named_by_host.update(dependency for _, dependency in unreferenced)

        spec_services = {}
        for service, node in self.services.items():
            spec_services[self.names[service]] = self._service(service, node, environments[service])
        self._check_dependencies(unreferenced, spec_services)
        if self.skipped:
            self.warn(None, f"skipped services in inactive profiles: {', '.join(self.skipped)}")
        if self.templates:
            self.warn(
                None,
                f"left out {', '.join(self.templates)}: other services extend it and it has no command or ports of its own; add it back if it should run",
            )
        if self.dockerized:
            self.warn(
                None,
                f"left out {', '.join(self.dockerized)}: they drive containers through the Docker socket, which is not available; each app gets its own HTTPS URL, so a routing proxy is unnecessary",
            )
        spec: Dict[str, Any] = {"version": 1, "services": spec_services}
        if self.secrets:
            spec["secrets"] = self.secrets
        return {
            "spec": spec,
            "services": {service: self.names[service] for service in self.services},
            "warnings": self.warnings,
        }

    def _kind(self, service: str, node: Dict[str, Any]) -> str:
        image = node.get("image")
        if image and not node.get("build"):
            _, repository, _ = parse_image(str(image))
            if repository in MANAGED_IMAGES:
                self.managed[service] = MANAGED_IMAGES[repository]
                return "database"
        for other in self.services.values():
            depends = other.get("depends_on")
            if isinstance(depends, dict) and (depends.get(service) or {}).get("condition") == (
                "service_completed_successfully"
            ):
                return "job"
        return "application"

    def _environment(self, service: str, node: Dict[str, Any]) -> Dict[str, str]:
        env: Dict[str, str] = {}
        files = node.get("env_file") or []
        for item in [files] if isinstance(files, (str, dict)) else files:
            path, required = (
                (item, True)
                if isinstance(item, str)
                else (item.get("path"), item.get("required", True))
            )
            found = example_env_file(self.root / path)
            if found:
                env.update(read_env_file(found))
                if found != self.root / path:
                    self.warn(service, f"env_file {path} is missing; read {found.name} instead")
            elif required:
                self.warn(service, f"env_file {path} not found")
        raw = _environment_mapping(self.raw_services.get(service) or {})
        for key, value in _environment_mapping(node).items():
            if value is None:
                if key in self.variables:
                    env[key] = self.variables[key]
                else:
                    self.declared[service].add(key)
                continue
            env[key] = value
            if value == "":
                self.declared[service].add(key)
                required = REQUIRED_VARIABLE.fullmatch(raw.get(key) or "")
                if required and (required.group(1) or required.group(2)) not in self.variables:
                    self.unset[service][key] = required.group(1) or required.group(2)
        return env

    def _database_literals(self, service: str, env: Dict[str, str]) -> Dict[str, str]:
        kind = self.managed[service]
        if kind == "postgres":
            user = env.get("POSTGRES_USER") or env.get("POSTGRESQL_USERNAME") or "postgres"
            return {
                "USERNAME": user,
                "PASSWORD": env.get("POSTGRES_PASSWORD") or env.get("POSTGRESQL_PASSWORD") or "",
                "DATABASE": env.get("POSTGRES_DB") or env.get("POSTGRESQL_DATABASE") or user,
                "PORT": "5432",
            }
        command = _command(self.services[service].get("command")) or []
        password = env.get("REDIS_PASSWORD") or env.get("VALKEY_PASSWORD") or ""
        if "--requirepass" in command[:-1]:
            password = command[command.index("--requirepass") + 1]
        return {"PASSWORD": password, "PORT": "6379"}

    def _target(self, host: str, port: Optional[str]) -> Optional[Tuple[str, Optional[int]]]:
        if host in self.aliases:
            return self.aliases[host], int(port) if port else None
        if host in LOCAL_HOSTS and port and int(port) in self.published:
            return self.published[int(port)]
        return None

    def _app_reference(self, service: str, kind: str, port: int) -> str:
        self.needed[service].add(port)
        return "${{app.%s.%s.%d}}" % (self.names[service], kind, port)

    def _rewrite_environment(
        self, service: str, env: Dict[str, str], literals: Dict[str, Dict[str, str]]
    ) -> Dict[str, str]:
        connected: Set[str] = set()
        rewritten = {}
        for key, value in env.items():
            new = self._rewrite(service, key, value, connected)
            if new == value:
                new = self._database_field(key, value, literals) or value
            rewritten[key] = new
        self._address_apps_by_host(service, rewritten)
        self._complete_database_settings(service, rewritten)
        for database in sorted(connected):
            self._require_tls(service, rewritten, database)
        return rewritten

    def _address_apps_by_host(self, service: str, env: Dict[str, str]) -> None:
        """KEY_HOST=postgres naming an app becomes the host half of its TCP gateway address,
        and KEY_PORT the port half; clients must then speak TLS."""
        for key, value in list(env.items()):
            name = self.aliases.get(value)
            if (
                name in (None, service)
                or self.kinds[name] == "database"
                or not HOST_KEY.search(key.upper())
            ):
                continue
            stem = re.fullmatch(r"(.+_)HOST(?:NAME)?", key, re.I)
            prefix = stem.group(1).upper() if stem else None
            port_key = next((k for k in env if prefix and k.upper() == prefix + "PORT"), None)
            port = (
                int(env[port_key])
                if port_key and env[port_key].isdigit()
                else self._default_port(name)
            )
            if port is None:
                self.warn(
                    service,
                    f"{key}={value} names {name} by host; set it to ${{{{app.{self.names[name]}.HOST.<port>}}}} and its port setting to ${{{{app.{self.names[name]}.PORT.<port>}}}}",
                )
                continue
            env[key] = self._app_reference(name, "HOST", port)
            changed = [key]
            if prefix:
                port_key = port_key or stem.group(1) + "PORT"
                env[port_key] = self._app_reference(name, "PORT", port)
                changed.append(port_key)
                for flag in [k for k in env if k.upper().startswith(prefix)]:
                    upper, current = flag.upper(), env[flag].lower()
                    if not re.search(r"(SSL|TLS|SECURE|HTTPS)(_?ENABLED?|_?MODE)?$", upper):
                        continue
                    if "MODE" in upper and current in ("disable", "allow", "prefer"):
                        env[flag] = "require"
                    elif current in ("false", "0", "no", "off"):
                        env[flag] = "1" if current == "0" else "true"
                    else:
                        continue
                    changed.append(f"{flag}={env[flag]}")
            self.warn(
                service,
                f"{', '.join(changed)}: {name} is reached through the TLS TCP gateway, so the client must use TLS (SNI is the host)",
            )

    def _default_port(self, name: str) -> Optional[int]:
        port = self._only_port(name)
        if port is None and not self.ports.get(name):
            config = self._image_config(name, self.services[name]) or {}
            exposed = [
                int(p.split("/")[0]) for p in config.get("ExposedPorts") or {} if p.endswith("/tcp")
            ]
            port = exposed[0] if len(exposed) == 1 else None
        return port

    def _complete_database_settings(self, service: str, env: Dict[str, str]) -> None:
        """An app given a managed database's host often relies on defaults for the rest
        (port 6379, no password), which a managed database does not match."""
        added = []
        for key, value in list(env.items()):
            host = re.fullmatch(r"\$\{\{db\.([^.}]+)\.HOST\}\}", value)
            stem = re.fullmatch(r"(.+_)HOST(?:NAME)?", key, re.I)
            if not (host and stem):
                continue
            database = next(name for name, app in self.names.items() if app == host.group(1))
            fields = {"PORT": "PORT", "PASSWORD": "PASSWORD"}
            if self.managed[database] == "postgres":
                fields["USER"] = "USERNAME"
            family = [other for other in env if other.upper().startswith(stem.group(1).upper())]
            for suffix, field in fields.items():
                sibling = stem.group(1) + suffix
                reference = "${{db.%s.%s}}" % (host.group(1), field)
                if any(
                    other.upper().startswith(sibling.upper()) or env[other] == reference
                    for other in family
                ):
                    continue
                env[sibling] = reference
                added.append(sibling)
        if added:
            self.warn(
                service,
                f"added {', '.join(added)} for the managed database; check the app reads those names",
            )

    def _rewrite(self, service: str, key: str, value: str, connected: Set[str]) -> str:
        if "${{" in value:
            return value
        match = URL.match(value)
        if match and (target := self._target(match["host"], match["port"])):
            name, _ = target
            if self.kinds[name] == "database":
                return self._database_url(service, key, value, name, match)

        def url(match: "re.Match[str]") -> str:
            target = self._target(match["host"], match["port"])
            if not target:
                return match.group(0)
            name, port = target
            if self.kinds[name] == "database":
                self.warn(
                    service,
                    f"{key}: credentials cannot be embedded in a longer value; use ${{{{db.{self.names[name]}.DATABASE_URL}}}} as the whole value",
                )
                return match.group(0)
            scheme = match["scheme"].lower()
            port = port or DEFAULT_PORTS.get(scheme) or self._only_port(name)
            if port is None:
                self.warn(
                    service, f"{key}: cannot tell which port of {name} {match.group(0)} means"
                )
                return match.group(0)
            if scheme in ("http", "https"):
                if match["userinfo"]:
                    self.warn(service, f"{key}: credentials in the URL of {name} were dropped")
                return self._app_reference(name, "URL", port)
            if scheme in ("ws", "wss"):
                self.warn(
                    service,
                    f"{key}: {name} is reached over wss:// at the host of ${{{{app.{self.names[name]}.URL.{port}}}}}; set it by hand",
                )
                return match.group(0)
            self.warn(
                service,
                f"{key}: {scheme}:// to {name} goes through the TLS TCP gateway; enable TLS in the client (SNI is the host)",
            )
            userinfo = f"{match['userinfo']}@" if match["userinfo"] is not None else ""
            return f"{match['scheme']}://{userinfo}{self._app_reference(name, 'TCP', port)}"

        new = URL.sub(url, value)
        if new != value:
            return new
        if value in self.aliases and HOST_KEY.search(key.upper()):
            name = self.aliases[value]
            if self.kinds[name] == "database":
                connected.add(name)
                return "${{db.%s.HOST}}" % self.names[name]
            self.named_by_host.add(name)
            return value

        def pair(match: "re.Match[str]") -> str:
            name = self.aliases.get(match["host"])
            if name is None:
                return match.group(0)
            if self.kinds[name] == "database":
                connected.add(name)
                return "${{db.%s.HOST}}:${{db.%s.PORT}}" % (self.names[name], self.names[name])
            self.warn(
                service,
                f"{key}: {match.group(0)} goes through the TLS TCP gateway; enable TLS in the client (SNI is the host)",
            )
            return self._app_reference(name, "TCP", int(match["port"]))

        return HOST_PORT.sub(pair, value)

    def _database_url(
        self, service: str, key: str, value: str, name: str, match: "re.Match[str]"
    ) -> str:
        kind = self.managed[name]
        scheme = match["scheme"].lower()
        reference = self.names[name]
        if kind == "postgres" and scheme.startswith("postgres"):
            if "+" in scheme:
                self.warn(
                    service,
                    f"{key}: the managed URL is postgresql://...?sslmode=require; {scheme} drivers may need it adapted",
                )
            return "${{db.%s.DATABASE_URL}}" % reference
        if kind == "redis" and scheme in ("redis", "rediss", "valkey"):
            path = value[match.end() :].split("?", 1)[0]
            if path not in ("", "/", "/0"):
                self.warn(
                    service, f"{key}: the managed URL selects database 0, not {path.strip('/')}"
                )
            self.warn(
                service,
                f"{key}: the managed URL is rediss:// (TLS); some clients need TLS options, e.g. Celery's ssl_cert_reqs",
            )
            return "${{db.%s.REDIS_URL}}" % reference
        self.warn(service, f"{key}: {scheme}:// is not a {kind} URL; set it by hand")
        return value

    def _database_field(
        self, key: str, value: str, literals: Dict[str, Dict[str, str]]
    ) -> Optional[str]:
        upper = key.upper()
        if re.search(r"PASS|PWD|SECRET|(?<![A-Z])AUTH", upper):
            field = "PASSWORD"
        elif "USER" in upper:
            field = "USERNAME"
        elif upper.endswith("PORT"):
            field = "PORT"
        elif re.search(r"(^|_)(DB|DATABASE|DBNAME|NAME)$", upper):
            field = "DATABASE"
        else:
            return None
        tags = {
            "postgres": ("PG", "POSTGRES", "DB", "DATABASE", "SQL"),
            "redis": ("REDIS", "VALKEY", "CACHE"),
        }
        for name, known in literals.items():
            if not value or known.get(field) != value:
                continue
            if any(tag in upper for tag in tags[self.managed[name]] + (_env_name(name),)):
                return "${{db.%s.%s}}" % (self.names[name], field)
        return None

    def _require_tls(self, service: str, env: Dict[str, str], database: str) -> None:
        kind = self.managed[database]
        tags = ("REDIS", "VALKEY") if kind == "redis" else ("PG", "POSTGRES", "DB", "DATABASE")
        flags = [
            key
            for key in env
            if any(tag in key.upper() for tag in tags) and re.search(r"TLS|SSL", key.upper())
        ]
        switched = []
        for key in flags:
            upper = key.upper()
            if re.search(r"(CA|CERT|KEY)(_PATH|_FILE)?$", upper) and env[key].startswith("/"):
                env[key] = ""
                switched.append(f"{key} cleared")
            elif re.search(r"SSL_?MODE$", upper):
                env[key] = "require"
                switched.append(f"{key}=require")
            elif env[key].lower() in ("", "false", "0", "no", "off", "disable", "disabled"):
                env[key] = "true"
                switched.append(f"{key}=true")
        if switched:
            self.warn(
                service, f"managed {kind} requires TLS with a public CA: {', '.join(switched)}"
            )
        else:
            self.warn(
                service,
                f"connects to managed {kind} by host; the client must use TLS (no client certificate)",
            )

    def _only_port(self, service: str) -> Optional[int]:
        ports = self.ports.get(service) or []
        return ports[0] if len(ports) == 1 else None

    def _share_secrets(self, environments: Dict[str, Dict[str, str]]) -> None:
        groups: Dict[str, List[Tuple[str, str]]] = {}
        for service, env in environments.items():
            if self.kinds[service] == "database":
                continue
            for key, value in env.items():
                upper = key.upper()
                if (
                    "${{" not in value
                    and SECRET_KEY.search(upper)
                    and not NOT_SECRET_KEY.search(upper)
                    and not re.fullmatch(r"[0-9.]{1,15}|true|false|yes|no", value, re.I)
                    and not value.startswith("/")
                ):
                    groups.setdefault(value, []).append((service, key))
        credentials = [
            (value, uses)
            for value, same in groups.items()
            for uses in self._credentials(same, environments)
        ]
        for value, uses in credentials:
            if not value and not self._required_credential(uses, environments):
                continue
            shared = len(uses) > 1
            weak = len(value) < 16 or len(set(value)) <= 2 or PLACEHOLDERS.search(value)
            if not (shared or weak):
                for service, key in uses:
                    self.warn(
                        service, f"{key} holds a literal credential; move it to a workspace secret"
                    )
                continue
            key = min((key for _, key in uses), key=len)
            name = self._secret_name(key)
            while name in self.secrets:
                name += "_2"
            self.secrets[name] = _secret_format(key, value)
            for service, used in uses:
                environments[service][used] = "${{secret.%s}}" % name
        if self.secrets:
            self.warn(
                None,
                f"generated stack secrets replace placeholder credentials and unset variables: {', '.join(self.secrets)}; check any format the app requires",
            )

    def _credentials(
        self, uses: List[Tuple[str, str]], environments: Dict[str, Dict[str, str]]
    ) -> List[List[Tuple[str, str]]]:
        """Split the uses of one literal into credentials. A placeholder such as `changethis`
        can stand for unrelated secrets, so uses are one credential only when they come from
        the same variable (DB_PASSWORD, and POSTGRES_PASSWORD: ${DB_PASSWORD}), or when a
        client's sibling setting (S3_ENDPOINT beside S3_SECRET_ACCESS_KEY) points at the
        service holding the value (MINIO_ROOT_PASSWORD)."""
        parent = {use: use for use in uses}

        def root(use: Tuple[str, str]) -> Tuple[str, str]:
            while parent[use] != use:
                use = parent[use]
            return use

        for index, (service, key) in enumerate(uses):
            for other, other_key in uses[index + 1 :]:
                linked = self._origin(service, key) == self._origin(other, other_key) or any(
                    self._points_at(environments[a], k, b)
                    for a, k, b in ((service, key, other), (other, other_key, service))
                    if a != b
                )
                if linked:
                    parent[root((other, other_key))] = root((service, key))
        components: Dict[Tuple[str, str], List[Tuple[str, str]]] = {}
        for use in uses:
            components.setdefault(root(use), []).append(use)
        return list(components.values())

    def _origin(self, service: str, key: str) -> str:
        """The variable a setting is read from: VAR for KEY: ${VAR}, else KEY itself, as in
        an env_file or a pass-through."""
        raw = _environment_mapping(self.raw_services.get(service) or {}).get(key)
        match = re.fullmatch(r"\$\{?([A-Za-z_]\w*)(?:[:?+-][^}]*)?\}?", (raw or "").strip())
        return match.group(1) if match else key

    def _required_credential(
        self, uses: List[Tuple[str, str]], environments: Dict[str, Dict[str, str]]
    ) -> bool:
        """An empty credential is an optional integration left unconfigured, unless a server
        in the stack needs it (POSTGRES_PASSWORD) or a client points at the server holding it."""
        return any(
            key.upper() in SERVER_CREDENTIALS
            and key in _environment_mapping(self.raw_services.get(service) or {})
            for service, key in uses
        ) or any(self._points_at(environments[a], k, b) for a, k in uses for b, _ in uses if a != b)

    def _points_at(self, env: Dict[str, str], key: str, target: str) -> bool:
        tokens = key.upper().split("_")
        while tokens and tokens[-1] in CREDENTIAL_WORDS:
            tokens.pop()
        if not tokens:
            return False
        prefix = "_".join(tokens) + "_"
        name = self.names[target]
        return any(
            other.upper().startswith(prefix) and (f"app.{name}." in value or f"db.{name}." in value)
            for other, value in env.items()
        )

    def _secret_name(self, key: str) -> str:
        prefix = _env_name(self.prefix)
        return _env_name(key if key.upper().startswith(prefix + "_") else f"{prefix}_{key}")

    def _fill_unset(self, environments: Dict[str, Dict[str, str]]) -> None:
        """Supply what compose leaves to the operator's .env: a generated secret for an unset
        credential, the app's own URL for an unset public URL, and the one managed database
        the service depends on for an empty connection URL."""
        for service, env in environments.items():
            if self.kinds[service] == "database":
                continue
            for key, variable in self.unset[service].items():
                upper = key.upper()
                if SECRET_KEY.search(upper) and not NOT_SECRET_KEY.search(upper):
                    name = self._secret_name(variable)
                    self.secrets.setdefault(name, _secret_format(key, ""))
                    env[key] = "${{secret.%s}}" % name
                elif OWN_URL_KEY.search(upper):
                    env[key] = "${{app.%s.URL}}" % self.names[service]
                    self.named_by_host.add(service)
                else:
                    continue
                self.filled.add(variable)
            depends = [
                name
                for name in self.services[service].get("depends_on") or []
                if name in self.services
            ]
            for key in sorted(self.declared[service]):
                if env.get(key):
                    continue
                for kind, pattern in DATABASE_URL_KEYS.items():
                    candidates = [name for name in depends if self.managed.get(name) == kind]
                    if pattern.fullmatch(key.upper()) and len(candidates) == 1:
                        field = "DATABASE_URL" if kind == "postgres" else "REDIS_URL"
                        env[key] = "${{db.%s.%s}}" % (self.names[candidates[0]], field)

    def _unreferenced_dependencies(
        self, environments: Dict[str, Dict[str, str]]
    ) -> List[Tuple[str, str]]:
        found = []
        for service, node in self.services.items():
            if self.kinds[service] == "database":
                continue
            values = " ".join(environments[service].values())
            for dependency in node.get("depends_on") or []:
                if dependency not in self.services or self.kinds[dependency] == "job":
                    continue
                name = self.names[dependency]
                if f"db.{name}." in values or f"app.{name}." in values:
                    continue
                if (service, dependency) not in self.file_references:
                    found.append((service, dependency))
        return found

    def _check_dependencies(
        self, unreferenced: List[Tuple[str, str]], spec_services: Dict[str, Any]
    ) -> None:
        """A dependency nothing points at is usually reached through an address built into
        the app (DB_HOSTNAME defaulting to `database`), which does not resolve here."""
        for service, dependency in unreferenced:
            name = self.names[dependency]
            if self.kinds[dependency] == "database":
                url = "DATABASE_URL" if self.managed[dependency] == "postgres" else "REDIS_URL"
                address = (
                    f"its URL setting to ${{{{db.{name}.{url}}}}}, or its host, port and password "
                    f"settings to ${{{{db.{name}.HOST}}}}, ${{{{db.{name}.PORT}}}} and ${{{{db.{name}.PASSWORD}}}}"
                )
            else:
                spec = spec_services[name]
                port = (spec["deploy"].get("ports") or [None])[0]
                if port is None:
                    address = f"an address of {name}, which listens on no known port"
                elif spec.get("health_path"):
                    address = f"its URL setting to ${{{{app.{name}.URL.{port}}}}}"
                else:
                    address = (
                        f"its host and port settings to ${{{{app.{name}.HOST.{port}}}}} and "
                        f"${{{{app.{name}.PORT.{port}}}}} (TLS required)"
                    )
            self.warn(
                service,
                f"depends on {dependency}, but no env value points at it; the app may default to the host {dependency}, which does not resolve here, so set {address}",
            )

    def _image_config(self, service: str, node: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if service in self.configs:
            return self.configs[service]
        build = node.get("build")
        config: Optional[Dict[str, Any]] = None
        if build is not None:
            context, dockerfile, target, args = self._build(node)
            if (context / dockerfile).is_file():
                config, resolved = dockerfile_config(
                    context / dockerfile, target, args, self.registry
                )
                if not resolved:
                    self.warn(
                        service,
                        "a base image's config is unreadable; its ENTRYPOINT and EXPOSE are unknown",
                    )
            else:
                self.warn(service, f"{context / dockerfile} not found")
        elif node.get("image"):
            config = self.registry.config(str(node["image"]))
        self.configs[service] = config
        return config

    def _build(self, node: Dict[str, Any]) -> Tuple[Path, str, Optional[str], Dict[str, str]]:
        build = node["build"]
        if isinstance(build, str):
            build = {"context": build}
        context = (self.root / str(build.get("context") or ".")).resolve()
        args = build.get("args") or {}
        if isinstance(args, list):
            args = dict(
                entry.split("=", 1) if "=" in entry else (entry, "") for entry in map(str, args)
            )
        return (
            context,
            str(build.get("dockerfile") or "Dockerfile"),
            build.get("target"),
            {str(key): _scalar(value) or "" for key, value in args.items()},
        )

    def _service(self, service: str, node: Dict[str, Any], env: Dict[str, str]) -> Dict[str, Any]:
        kind = self.kinds[service]
        depends = node.get("depends_on") or []
        depends_on = sorted(self.names[name] for name in depends if name in self.services)
        if kind == "database":
            self._database_notes(service, node)
            return {
                "type": "database",
                "deploy": {"kind": self.managed[service], "always_on": True},
                "depends_on": depends_on,
            }

        for key, reason in UNSUPPORTED_KEYS.items():
            if node.get(key):
                self.warn(service, f"{key}: {reason}")
        deploy: Dict[str, Any] = {}
        result: Dict[str, Any] = {"type": kind, "deploy": deploy, "depends_on": depends_on}
        config = self._image_config(service, node) or {}
        known = KNOWN_IMAGES.get(self._image_basename(node), {})

        if node.get("build") is not None:
            context, dockerfile, target, args = self._build(node)
            deploy["directory"] = str(context)
            deploy["dockerfile"] = dockerfile
            if (context / dockerfile).resolve().parent != context:
                deploy["context_dir"] = "."
            if target:
                deploy["target"] = target
                stage = re.compile(rf"^\s*FROM\s+.+\s+AS\s+{re.escape(target)}\s*$", re.I | re.M)
                path = context / dockerfile
                if path.is_file() and not stage.search(path.read_text()):
                    self.warn(service, f"build target {target} is not a stage of {dockerfile}")
            if args:
                deploy["build_args"] = {key: str(value) for key, value in args.items()}
        else:
            deploy["image"] = str(node["image"])
            if not config:
                self.warn(
                    service,
                    f"could not read {node['image']}'s config (private or offline); its ENTRYPOINT and EXPOSE are unknown",
                )

        entrypoint = self._entrypoint(service, node, config)
        disks = self._volumes(service, node, deploy)
        if entrypoint:
            deploy["entrypoint"] = entrypoint
            self._scan_command(service, entrypoint)
        if env:
            deploy["env"] = env
        if disks:
            deploy["disks"] = disks
        self._resources(service, node, deploy, known)
        if kind == "job":
            return result

        ports = list(self.ports[service])
        ports += [port for port in sorted(self.needed[service]) if port not in ports]
        if not ports and service in self.named_by_host:
            ports = [
                int(port.split("/")[0])
                for port in config.get("ExposedPorts") or {}
                if port.endswith("/tcp")
            ]
        health = self._health(service, node, config, known, ports)
        if health and health[1] not in ports:
            ports.append(health[1])
        deploy["ports"] = ports
        private = [str(port) for port in ports if port not in self.public[service]]
        own_url = "${{app.%s." % self.names[service]
        if private and not any(own_url in value for value in env.values()):
            self.warn(
                service,
                f"{'ports' if len(private) > 1 else 'port'} {', '.join(private)} {'were' if len(private) > 1 else 'was'} private in compose but public here; anyone with the URL can reach {'them' if len(private) > 1 else 'it'}, so make sure it requires a password or token",
            )
        if health:
            result["health_path"], result["health_port"] = health
            if len(ports) == 1:
                del result["health_port"]
        elif ports:
            self.warn(
                service,
                "no HTTP health check found; readiness only checks that its container runs (set health_path for a real check)",
            )
        return result

    def _scan_command(self, service: str, argv: List[str]) -> None:
        others = sorted((a for a, n in self.aliases.items() if n != service), key=len, reverse=True)
        if not others:
            return
        names = "|".join(map(re.escape, others))
        match = re.search(
            rf"(?:://|/dev/tcp/|(?:-h|--host)[= ])(?:{names})(?![\w.-])|(?<![\w./-])(?:{names}):\d{{2,5}}\b",
            " ".join(argv),
        )
        if match:
            self.warn(
                service,
                f"its command names another service ({match.group(0)}), which does not resolve here; the stack starts it after its dependencies are ready, so drop wait-for loops and read addresses from env",
            )

    def _image_basename(self, node: Dict[str, Any]) -> str:
        if not node.get("image"):
            return ""
        return parse_image(str(node["image"]))[1].rsplit("/", 1)[-1]

    def _entrypoint(
        self, service: str, node: Dict[str, Any], config: Dict[str, Any]
    ) -> Optional[List[str]]:
        entrypoint, command = _command(node.get("entrypoint")), _command(node.get("command"))
        if entrypoint is not None:
            argv = entrypoint + (command or [])
        elif command is not None:
            image_entrypoint = config.get("Entrypoint")
            if image_entrypoint is None and not config:
                self.warn(
                    service,
                    "command replaces the whole argv on Beam and the image's ENTRYPOINT is unknown; prepend it if the image has one",
                )
            argv = list(image_entrypoint or []) + command
        else:
            argv = None
        working_dir = node.get("working_dir")
        if working_dir:
            argv = argv or list(config.get("Entrypoint") or []) + list(config.get("Cmd") or [])
            if argv:
                argv = [
                    "/bin/sh",
                    "-c",
                    f"cd {shlex.quote(str(working_dir))} && exec {shlex.join(argv)}",
                ]
            else:
                self.warn(
                    service, f"working_dir {working_dir} is ignored; the image's command is unknown"
                )
        return argv or None

    def _volumes(self, service: str, node: Dict[str, Any], deploy: Dict[str, Any]) -> List[str]:
        disks = []
        binds: List[Tuple[Path, str]] = []
        declared = self.document.get("volumes") or {}
        for entry in node.get("volumes") or []:
            kind, source, target, read_only = _parse_volume(entry)
            if kind == "volume":
                if (declared.get(source) or {}).get("external"):
                    self.warn(service, f"external volume {source} becomes a new empty disk")
                disks.append(f"{self._disk_name(source)}:{target}:{DISK_SIZE}")
            elif kind == "bind":
                path = (self.root / Path(source).expanduser()).resolve()
                if source.endswith("docker.sock"):
                    self.warn(
                        service, f"{source} cannot be mounted; the Docker daemon is not available"
                    )
                elif source in ("/etc/localtime", "/etc/timezone"):
                    continue
                elif not path.exists() or (path.is_dir() and not any(path.iterdir())):
                    parts = Path(source).parts
                    name = self._disk_name(parts[-1] if parts else "data", service)
                    if any(disk.startswith(name + ":") for disk in disks) and len(parts) > 1:
                        name = self._disk_name("-".join(parts[-2:]), service)
                    disks.append(f"{name}:{target}:{DISK_SIZE}")
                    self.warn(
                        service,
                        f"bind mount {source} has no content to copy; it became a disk at {target}",
                    )
                elif self.root not in path.parents and path != self.root:
                    self.warn(
                        service, f"bind mount {source} is outside the project and is not available"
                    )
                else:
                    binds.append((path, target))
                    if not read_only:
                        self.warn(
                            service,
                            f"files bound from {source} are copied into the image; writes to {target} do not persist",
                        )
        owned = []
        for disk in disks:
            name = disk.split(":", 1)[0]
            owner = self.disk_owners.setdefault(name, service)
            if owner == service:
                owned.append(disk)
            else:
                self.warn(
                    service,
                    f"disk {name} is already attached to {owner}; a durable disk serves one app, so share the data through that app",
                )
        if binds:
            self._bake(service, node, binds, deploy)
        return owned

    def _disk_name(self, volume: str, service: Optional[str] = None) -> str:
        name = _slug(volume)
        if service:
            name = f"{_slug(service)}-{name}"
        return (
            name
            if name == self.prefix or name.startswith(self.prefix + "-")
            else f"{self.prefix}-{name}"
        )

    def _bake(
        self,
        service: str,
        node: Dict[str, Any],
        binds: List[Tuple[Path, str]],
        deploy: Dict[str, Any],
    ) -> None:
        if node.get("build") is not None:
            self.warn(
                service,
                "bind mounts of a built service are dev-only; the image is built from its context",
            )
            return
        if self.build_root is None:
            self.warn(service, "bind mounts are not available; copy the files into an image")
            return
        digest = hashlib.sha256(str(node["image"]).encode())
        for path, target in binds:
            digest.update(f"{path}\0{target}\0".encode())
            for file in sorted(path.rglob("*")) if path.is_dir() else [path]:
                if file.is_file():
                    digest.update(str(file.relative_to(path.parent)).encode() + file.read_bytes())
        destination = self.build_root / digest.hexdigest()[:24]
        if not destination.exists():
            self.build_root.mkdir(parents=True, exist_ok=True)
            with tempfile.TemporaryDirectory(dir=self.build_root) as temporary:
                staging = Path(temporary) / "build"
                staging.mkdir()
                lines = [f"FROM {node['image']}"]
                for index, (path, target) in enumerate(binds):
                    name = f"bind{index}"
                    if path.is_dir():
                        shutil.copytree(path, staging / name, symlinks=False)
                    else:
                        shutil.copy2(path, staging / name)
                    lines.append(f"COPY {name} {target}")
                (staging / "Dockerfile").write_text("\n".join(lines) + "\n")
                staging.rename(destination)
        deploy.pop("image", None)
        deploy["directory"] = str(destination)
        deploy["dockerfile"] = "Dockerfile"
        self.warn(
            service,
            f"bind-mounted files are copied into an image built from {node['image']} at {destination}",
        )

    def _scan_bound_files(self) -> None:
        """Bound config files (an nginx proxy_pass, say) may name other services by host;
        those services must expose the named ports, and the files need new addresses."""
        for service, node in self.services.items():
            if self.kinds[service] == "database" or node.get("build") is not None:
                continue
            others = sorted(
                (a for a, n in self.aliases.items() if n != service), key=len, reverse=True
            )
            if not others:
                continue
            names = "|".join(map(re.escape, others))
            pattern = re.compile(
                rf"://(?P<a>{names})(?::(?P<p>\d{{2,5}}))?(?![\w.-])|(?<![\w./-])(?P<b>{names}):(?P<q>\d{{2,5}})\b"
            )
            for entry in node.get("volumes") or []:
                kind, source, _, _ = _parse_volume(entry)
                path = (self.root / Path(source).expanduser()).resolve()
                if kind != "bind" or not path.exists() or self.root not in [path, *path.parents]:
                    continue
                for file in sorted(path.rglob("*")) if path.is_dir() else [path]:
                    if not file.is_file() or file.stat().st_size >= 1 << 20:
                        continue
                    found = []
                    for match in pattern.finditer(file.read_text(errors="ignore")):
                        target = self.aliases[match.group("a") or match.group("b")]
                        port = match.group("p") or match.group("q")
                        self.file_references.add((service, target))
                        if self.kinds[target] == "application":
                            if port:
                                self.needed[target].add(int(port))
                            else:
                                self.named_by_host.add(target)
                        found.append(match.group(0).lstrip(":/"))
                    if found:
                        self.warn(
                            service,
                            f"{file.relative_to(self.root)} names {', '.join(sorted(set(found)))}, which does not resolve on Beam; rewrite it to the public address (${{{{app.NAME.URL.<port>}}}}), or drop a proxy that only routes to one app",
                        )

    def _resources(
        self, service: str, node: Dict[str, Any], deploy: Dict[str, Any], known: Dict[str, Any]
    ) -> None:
        resources = (node.get("deploy") or {}).get("resources") or {}
        limits = resources.get("limits") or {}
        cpus = limits.get("cpus") or node.get("cpus")
        memory = limits.get("memory") or node.get("mem_limit")
        deploy["cpu"] = float(cpus) if cpus else known.get("cpu", 1)
        deploy["memory"] = (_memory(memory) if memory else None) or known.get(
            "memory", DEFAULT_MEMORY
        )
        devices = (resources.get("reservations") or {}).get("devices") or []
        if node.get("gpus") or any(
            "gpu" in (device.get("capabilities") or []) for device in devices
        ):
            self.warn(service, "reserves a GPU; set deploy.gpu (see capabilities)")

    def _health(
        self,
        service: str,
        node: Dict[str, Any],
        config: Dict[str, Any],
        known: Dict[str, Any],
        ports: List[int],
    ) -> Optional[Tuple[str, int]]:
        aliases = {alias for alias, name in self.aliases.items() if name == service}
        for test in (
            (node.get("healthcheck") or {}).get("test"),
            (config.get("Healthcheck") or {}).get("Test"),
        ):
            found = _health_url(test, aliases) if test else None
            if found and (not ports or found[1] in ports):
                return found
        if known.get("health") and (not ports or known["health"][1] in ports):
            return known["health"]
        return None

    def _database_notes(self, service: str, node: Dict[str, Any]) -> None:
        kind = self.managed[service]
        tag = parse_image(str(node["image"]))[2]
        major = tag.split(".", 1)[0].split("-", 1)[0]
        managed = {"postgres": "16", "redis": "7"}[kind]
        if major.isdigit() and major != managed:
            self.warn(service, f"compose pins {kind} {tag}; the managed {kind} is {managed}")
        if any(_parse_volume(entry)[0] == "bind" for entry in node.get("volumes") or []):
            self.warn(
                service,
                "bind-mounted init scripts do not run on a managed database; run them as a job",
            )
        self.warn(
            service,
            f"became a managed {kind} ({self.names[service]}); compose's credentials are replaced by generated ones",
        )


def load_document(path: Path) -> Dict[str, Any]:
    try:
        document = yaml.load(path.read_text(), Loader=_ComposeLoader) or {}
    except yaml.YAMLError as exc:
        raise ComposeError(f"invalid YAML in {path}: {exc}") from exc
    if not isinstance(document, dict):
        raise ComposeError(f"{path} is not a compose file")
    return document


def translate(
    path: str,
    prefix: Optional[str] = None,
    env: Optional[Dict[str, str]] = None,
    profiles: Optional[List[str]] = None,
    registry: Optional[Registry] = None,
    build_root: Optional[Path] = None,
    files: Optional[List[str]] = None,
) -> Dict[str, Any]:
    compose_file = find_compose_file(path)
    root = compose_file.parent
    env_file = example_env_file(root / ".env")
    variables = {
        **(read_env_file(env_file) if env_file else {}),
        **{k: str(v) for k, v in (env or {}).items()},
    }
    missing: Set[str] = set()
    document = _plain(load_document(compose_file))
    merged = []
    for name in files or []:
        extra = (root / name).resolve()
        if not extra.is_file():
            raise ComposeError(f"no compose file at {extra}")
        if extra != compose_file:
            document = merge_documents(document, load_document(extra))
            merged.append(extra.name)
    translator = Translator(
        root,
        _interpolate_tree(document, variables, missing),
        variables,
        prefix,
        profiles or [],
        registry or Registry(),
        build_root,
        raw=document,
    )
    result = translator.translate()
    document = translator.document
    unresolved = missing - translator.filled
    if unresolved:
        result["warnings"].insert(
            0,
            f"unset variables became empty: {', '.join(sorted(unresolved))}; pass env to set them",
        )
    if env_file and env_file.name != ".env":
        result["warnings"].insert(0, f".env is missing; read {env_file.name} instead")
    for override in (
        "compose.override.yaml",
        "compose.override.yml",
        "docker-compose.override.yaml",
        "docker-compose.override.yml",
    ):
        if (root / override).is_file() and override not in merged:
            result["warnings"].append(
                f"{override} is not merged (it usually holds development settings); "
                "pass it in files to merge it"
            )
    if document.get("include"):
        result["warnings"].append("include: files are not merged; pass them in files")
    return {
        "compose_file": str(compose_file),
        "merged": merged,
        "prefix": translator.prefix,
        **result,
    }


def definition() -> Dict[str, Any]:
    return {
        "name": "stack_from_compose",
        "description": (
            "Translate a docker-compose project into a draft stack spec for stack_plan; "
            "provisions nothing. Compose service names become prefixed app names; postgres "
            "and redis images become managed databases; build: and image: services become "
            "applications (one-shot services that others wait on become jobs); named volumes "
            "become durable disks; bind-mounted files are copied into a derived image; "
            "addresses such as http://minio:9000, redis:6379 or localhost:<published port> "
            "become references; placeholder credentials shared between services become "
            "generated stack secrets. Review every warning before stack_plan: they mark what "
            "compose expresses that Beam does not (private networking, TLS to managed "
            "databases, user, ulimits)."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Absolute path of the compose file or the directory holding it.",
                },
                "prefix": {
                    "type": "string",
                    "description": "App name prefix; app names are workspace-wide. Default: the compose project name.",
                },
                "env": {
                    "type": "object",
                    "additionalProperties": {"type": "string"},
                    "description": "Interpolation variables, overriding the project's .env.",
                },
                "profiles": {"type": "array", "items": {"type": "string"}},
                "files": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "More compose files merged over it in order, relative to its directory, "
                        "as with `docker compose -f a.yml -f b.yml`; use the files the project's "
                        "README deploys with."
                    ),
                },
            },
            "required": ["path"],
            "additionalProperties": False,
        },
        "annotations": {"title": "Translate a compose project", "readOnlyHint": True},
    }


def handler(build_root: Callable[[], Path]) -> Callable[[Dict[str, Any]], Dict[str, Any]]:
    from .tools import text_result

    def run(args: Dict[str, Any]) -> Dict[str, Any]:
        result = translate(
            str(args["path"]),
            prefix=args.get("prefix"),
            env=args.get("env"),
            profiles=args.get("profiles"),
            build_root=build_root(),
            files=args.get("files"),
        )
        return text_result(
            "Draft stack spec. Resolve the warnings, adjust the spec, then call stack_plan "
            "with a name and this spec.",
            **result,
        )

    return run
