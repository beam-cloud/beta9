import configparser
import functools
import inspect
import ipaddress
import os
import shutil
import socket
import sys
import tempfile
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any, Dict, Mapping, MutableMapping, Optional, Tuple, Union

from . import terminal
from .env import is_remote

DEFAULT_CLI_NAME = "Beta9"
DEFAULT_CONTEXT_NAME = "default"
DEFAULT_GATEWAY_HOST = "0.0.0.0"
DEFAULT_GATEWAY_PORT = 1993
DEFAULT_API_HOST = "0.0.0.0"
DEFAULT_API_PORT = 1994
_SETTINGS: Optional["SDKSettings"] = None
DEFAULT_ASCII_LOGO = """
           ,#@@&&&&&&&&&@&/
        @&&&&&&&&&&&&&&&&&&&&@#
         *@&&&&&&&&&&&&&&&&&&&&&@/
   ##      /&&&&&&&&&&&&&@&&&&&&&&@,
  @&&&&&.    (&&&&&&@/    &&&&&&&&&&/
 &&&&&&&&&@*   %&@.      @& ,@&&&&&&&,
.@&&&&&&&&&&&&#        &&*  ,@&&&&&&&&
*&&&&&&&&&&&@,   %&@/@&*    @&&&&&&&&@
.@&&&&&&&&&*      *&@     .@&&&&&&&&&&
 %&&&&&&&&     /@@*     .@&&&&&&&&&&@,
  &&&&&&&/.#@&&.     .&&&    %&&&&&@,
   /&&&&&&&@%*,,*#@&&(         ,@&&
     /&&&&&&&&&&&&&&,
        #@&&&&&&&&&&,
            ,(&@@&&&,
"""


def _http_url(host: str, port: int) -> str:
    return f"{'https' if int(port) == 443 else 'http'}://{host}:{port}"


@dataclass
class SDKSettings:
    name: str = DEFAULT_CLI_NAME
    gateway_host: str = DEFAULT_GATEWAY_HOST
    gateway_port: int = DEFAULT_GATEWAY_PORT
    api_host: str = DEFAULT_API_HOST
    api_port: int = DEFAULT_API_PORT
    config_path: Path = Path("~/.beta9/config.ini").expanduser()
    ascii_logo: str = DEFAULT_ASCII_LOGO
    use_defaults_in_prompt: bool = False
    api_token: Optional[str] = os.getenv("BETA9_TOKEN")
    # Dashboard link template for an app's overview page, e.g.
    # "https://platform.beam.cloud/app/{app_id}/overview". Empty when there is
    # no dashboard to link to (plain beta9 installs without one).
    app_url_template: str = os.getenv("BETA9_APP_URL_TEMPLATE", "")
    # OAuth 2.0 authorization server for `login` (RFC 8628 device grant):
    # POST {auth_url}/device/code and POST {auth_url}/token. Empty means the
    # install has no browser sign-in and tokens are entered by hand.
    auth_url: str = os.getenv("BETA9_AUTH_URL", "")
    # Public documentation, quoted in the agent skill; empty omits the links.
    docs_url: str = os.getenv("BETA9_DOCS_URL", "")
    # Other clusters this CLI is built for, by name (a staging cluster, say):
    # `login --environment X` signs in there and `--context X` uses it.
    # Tokenless; sign-in saves the token. The fields above are the default.
    environments: Dict[str, "ConfigContext"] = field(default_factory=dict)

    @property
    def api_url(self) -> str:
        return _http_url(self.api_host, self.api_port)

    def __post_init__(self, **kwargs):
        config_path = os.getenv("CONFIG_PATH")
        if config_path:
            self.config_path = Path(config_path).expanduser()

        # Handle Beam-specific environment variables if beam module is loaded
        if "beam" in sys.modules:
            self.name = "Beam"
            self.api_host = os.getenv("API_HOST", "app.beam.cloud")
            self.api_port = int(os.getenv("API_PORT", 443))
            self.gateway_host = os.getenv("GATEWAY_HOST", "gateway.beam.cloud")
            self.gateway_port = int(os.getenv("GATEWAY_PORT", 443))
            if not config_path:
                self.config_path = Path("~/.beam/config.ini").expanduser()
            self.use_defaults_in_prompt = True
            self.api_token = os.getenv("BEAM_TOKEN")

            # The dashboard lives at platform.<domain> and the account API at
            # api.<domain>, mirroring the api host at app.<domain>
            # (e.g. app.beam.cloud -> platform.beam.cloud, api.beam.cloud).
            host = self.api_host.split(":")[0]
            if not self.app_url_template and host.startswith("app."):
                self.app_url_template = (
                    f"https://platform.{host[len('app.') :]}/app/{{app_id}}/overview"
                )
            self.auth_url = os.getenv("BEAM_AUTH_URL", self.auth_url)
            if not self.auth_url and host.startswith("app."):
                self.auth_url = f"https://api.{host[len('app.') :]}/v2/oauth"
            self.docs_url = self.docs_url or "https://docs.beam.cloud"


@dataclass
class ConfigContext:
    token: Optional[str] = None
    gateway_host: Optional[str] = None
    gateway_port: Optional[int] = None
    api_url: Optional[str] = None
    # OAuth device-grant server that issues this context's tokens; empty when
    # tokens are created in a dashboard and pasted in.
    auth_url: Optional[str] = None

    @property
    def http_url(self) -> str:
        if self.api_url:
            return self.api_url.rstrip("/")
        port = int(self.gateway_port or DEFAULT_GATEWAY_PORT)
        port = DEFAULT_API_PORT if port == DEFAULT_GATEWAY_PORT else port
        return _http_url(self.gateway_host or DEFAULT_GATEWAY_HOST, port)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ConfigContext":
        return cls(**{k: v for k, v in data.items() if k in inspect.signature(cls).parameters})

    def to_dict(self) -> MutableMapping[str, Any]:
        return {k: ("" if not v else v) for k, v in asdict(self).items()}

    def use_ssl(self) -> bool:
        if self.gateway_port in [443, "443"]:
            return True
        return False

    def is_valid(self) -> bool:
        return all([self.token, self.gateway_host, self.gateway_port])


def set_settings(s: Optional[SDKSettings] = None) -> None:
    if s is None:
        s = SDKSettings()

    global _SETTINGS
    _SETTINGS = s


def get_settings() -> SDKSettings:
    if not _SETTINGS:
        set_settings()

    return _SETTINGS  # type: ignore


def cli_path() -> Optional[str]:
    """This install's executable, not whichever copy is first on PATH."""
    name = get_settings().name.lower()
    entry = Path(sys.argv[0]) if sys.argv and sys.argv[0] else None
    if entry is not None and entry.name == name and entry.is_file():
        return str(entry.absolute())
    return shutil.which(name)


def load_config(path: Optional[Union[str, Path]] = None) -> MutableMapping[str, ConfigContext]:
    if path is None:
        path = get_settings().config_path

    path = Path(path)
    if not path.exists():
        return {}

    # `[default]` is an ordinary context; nothing inherits from it, or a
    # staging context missing a key would silently pick up the default's.
    parser = configparser.ConfigParser()
    parser.read(path)

    return {k: ConfigContext.from_dict(v) for k, v in parser.items() if k != parser.default_section}


def save_config(
    contexts: Mapping[str, ConfigContext], path: Optional[Union[Path, str]] = None
) -> None:
    if not contexts:
        return

    if path is None:
        path = get_settings().config_path

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    parser = configparser.ConfigParser()
    parser.read_dict({k: v.to_dict() for k, v in contexts.items()})

    # Replaced, never rewritten in place: running MCP servers reread this file
    # on every request. Through a symlink, its target is replaced.
    path = path.resolve()
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    try:
        with os.fdopen(fd, "w") as file:
            parser.write(file)
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def is_config_empty(path: Optional[Union[Path, str]] = None) -> bool:
    if path is None:
        path = get_settings().config_path

    path = Path(path)
    if path.exists():
        return False

    parser = configparser.ConfigParser()
    parser.read(path)
    if any(v.get("gateway_host") for v in parser.values()):
        return False

    return True


def settings_context() -> ConfigContext:
    """The default environment: the gateway and sign-in the settings describe."""
    settings = get_settings()
    return ConfigContext(
        gateway_host=settings.gateway_host,
        gateway_port=settings.gateway_port,
        api_url=settings.api_url,
        auth_url=settings.auth_url,
    )


def context_defaults(name: str = DEFAULT_CONTEXT_NAME) -> ConfigContext:
    """
    Where context `name` connects, with or without a token: the saved context,
    else the environment of that name, else the settings. A context saved
    before it recorded an `auth_url` borrows the one of the environment at
    its gateway, so `login` can renew it.
    """
    known = [settings_context(), *get_settings().environments.values()]
    saved = load_config().get(name)
    if saved is None:
        return get_settings().environments.get(name, known[0])
    if not saved.auth_url:
        for environment in known:
            if environment.gateway_host == saved.gateway_host:
                return replace(saved, auth_url=environment.auth_url)
    return saved


def get_config_context(name: str = DEFAULT_CONTEXT_NAME) -> ConfigContext:
    contexts = load_config()
    if name in contexts:
        return contexts[name]

    settings = get_settings()
    defaults = context_defaults(name)

    gateway_host = os.getenv("BETA9_GATEWAY_HOST") or defaults.gateway_host
    gateway_port = int(os.getenv("BETA9_GATEWAY_PORT") or defaults.gateway_port or 0)
    token = os.getenv("BETA9_TOKEN", settings.api_token)

    # Inside a container the gateway address is always injected; a token is
    # not (managed endpoint replicas authenticate with their replica secret
    # instead), so build a tokenless context rather than prompting.
    if gateway_host and gateway_port and (token or is_remote()):
        same_gateway = (gateway_host, gateway_port) == (
            defaults.gateway_host,
            defaults.gateway_port,
        )
        return ConfigContext(
            token=token,
            gateway_host=gateway_host,
            gateway_port=gateway_port,
            api_url=os.getenv("BETA9_API_URL") or (defaults.api_url if same_gateway else None),
            auth_url=defaults.auth_url if same_gateway else None,
        )

    if not sys.stdin.isatty():
        cli = settings.name.lower()
        how = f"{cli} login" if defaults.auth_url else f"{cli} config create"
        if name != DEFAULT_CONTEXT_NAME:
            how += f" --name {name}" if defaults.auth_url else f" {name}"
        terminal.error(
            f"Not signed in: context '{name}' does not exist.",
            hint=f"Run `{how}`, or set {cli.upper()}_TOKEN.",
            code="NOT_AUTHENTICATED",
        )
    terminal.header(f"Context '{name}' does not exist. Let's try setting it up.")
    contexts[name] = prompt_for_config_context(name=name, require_token=True)[1]
    save_config(contexts)
    return contexts[name]


def prompt_for_config_context(
    name: Optional[str] = None,
    token: Optional[str] = None,
    gateway_host: Optional[str] = None,
    gateway_port: Optional[int] = None,
    require_token: bool = False,
) -> Tuple[str, ConfigContext]:
    settings = get_settings()

    prompt_name = functools.partial(
        terminal.prompt, text="Context Name", default=name or DEFAULT_CONTEXT_NAME
    )

    try:
        while not name and not (name := prompt_name()):
            terminal.warn("Name is invalid.")

        defaults = context_defaults(name)
        prompt_gateway_host = functools.partial(
            terminal.prompt, text="Gateway Host", default=gateway_host or defaults.gateway_host
        )
        prompt_gateway_port = functools.partial(
            terminal.prompt, text="Gateway Port", default=gateway_port or defaults.gateway_port
        )

        if settings.use_defaults_in_prompt:
            gateway_host = defaults.gateway_host
            gateway_port = defaults.gateway_port
        else:
            while not (gateway_host := prompt_gateway_host()) or not validate_ip_or_dns(
                gateway_host
            ):
                terminal.warn("Gateway host is invalid or unreachable.")

            while not (gateway_port := prompt_gateway_port()) or not validate_port(gateway_port):
                terminal.warn("Gateway port is invalid.")

        if require_token:
            while not (token := terminal.prompt(text="Token", default=None)) or len(token) < 64:
                terminal.warn("Token is invalid.")
        else:
            token = terminal.prompt(text="Token", default=None)

    except (KeyboardInterrupt, EOFError):
        os._exit(1)

    same_gateway = (gateway_host, int(gateway_port or 0)) == (
        defaults.gateway_host,
        defaults.gateway_port,
    )
    return name, ConfigContext(
        token=token,
        gateway_host=gateway_host,
        gateway_port=gateway_port,
        api_url=defaults.api_url if same_gateway else None,
        auth_url=defaults.auth_url if same_gateway else None,
    )


def validate_ip_or_dns(value) -> bool:
    try:
        ipaddress.ip_address(value)
        return True
    except ValueError:
        pass

    try:
        socket.gethostbyname(value)
        return True
    except socket.error:
        pass

    return False


def validate_port(value: Any) -> bool:
    try:
        if 0 < int(value) <= 65535:
            return True
    except ValueError:
        pass

    return False
