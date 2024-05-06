import configparser
import functools
import inspect
import ipaddress
import os
import socket
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping, MutableMapping, Optional, Tuple, Union

from . import terminal

DEFAULT_CLI_NAME = "Beta9"
DEFAULT_CONTEXT_NAME = "default"
DEFAULT_GATEWAY_HOST = "0.0.0.0"
DEFAULT_API_HOST = "0.0.0.0:1994"
DEFAULT_GATEWAY_PORT = 1993
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


@dataclass
class SDKSettings:
    name: str = DEFAULT_CLI_NAME
    gateway_host: str = DEFAULT_GATEWAY_HOST
    gateway_port: int = DEFAULT_GATEWAY_PORT
    api_host: str = DEFAULT_API_HOST
    config_path: Path = Path("~/.beta9/config.ini").expanduser()
    ascii_logo: str = DEFAULT_ASCII_LOGO

    def __post_init__(self, **kwargs):
        if p := os.getenv("CONFIG_PATH"):
            self.config_path = Path(p).expanduser()


@dataclass
class ConfigContext:
    token: Optional[str] = None
    gateway_host: Optional[str] = None
    gateway_port: Optional[int] = None

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ConfigContext":
        return cls(**{k: v for k, v in data.items() if k in inspect.signature(cls).parameters})

    def to_dict(self) -> MutableMapping[str, Any]:
        return {k: ("" if not v else v) for k, v in asdict(self).items()}

    def use_ssl(self) -> bool:
        if self.gateway_port in [443, "443"]:
            return True
        return False


def set_settings(s: Optional[SDKSettings] = None) -> None:
    if s is None:
        s = SDKSettings()

    global _SETTINGS
    _SETTINGS = s


def get_settings() -> SDKSettings:
    if not _SETTINGS:
        set_settings()

    return _SETTINGS  # type: ignore


def load_config(path: Optional[Union[str, Path]] = None) -> MutableMapping[str, ConfigContext]:
    if path is None:
        path = get_settings().config_path

    path = Path(path)
    if not path.exists():
        return {}

    parser = configparser.ConfigParser(default_section=DEFAULT_CONTEXT_NAME)
    parser.read(path)

    return {k: ConfigContext.from_dict(v) for k, v in parser.items()}  # type:ignore


def save_config(
    contexts: Mapping[str, ConfigContext], path: Optional[Union[Path, str]] = None
) -> None:
    if not contexts:
        return

    if path is None:
        path = get_settings().config_path

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    parser = configparser.ConfigParser(default_section=DEFAULT_CONTEXT_NAME)
    parser.read_dict({k: v.to_dict() for k, v in contexts.items()})

    with open(path, "w") as file:
        parser.write(file)


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


def get_config_context(name: str = DEFAULT_CONTEXT_NAME) -> ConfigContext:
    config = load_config()
    if name in config:
        return config[name]

    gateway_host = os.getenv("BETA9_GATEWAY_HOST", None)
    gateway_port = os.getenv("BETA9_GATEWAY_PORT", None)
    token = os.getenv("BETA9_TOKEN", None)

    if gateway_host and gateway_port and token:
        return ConfigContext(
            token=token,
            gateway_host=gateway_host,
            gateway_port=gateway_port,
        )

    terminal.header(f"Context '{name}' does not exist. Let's try setting it up.")
    _, config = prompt_for_config_context(name=name)
    return config


def prompt_for_config_context(
    name: Optional[str] = None,
    token: Optional[str] = None,
    gateway_host: Optional[str] = None,
    gateway_port: Optional[int] = None,
) -> Tuple[str, ConfigContext]:
    settings = get_settings()

    prompt_name = functools.partial(
        terminal.prompt, text="Context Name", default=name or DEFAULT_CONTEXT_NAME
    )
    # TODO: validate host by talking to gateway?
    prompt_gateway_host = functools.partial(
        terminal.prompt, text="Gateway Host", default=gateway_host or settings.gateway_host
    )
    prompt_gateway_port = functools.partial(
        terminal.prompt, text="Gateway Port", default=gateway_port or settings.gateway_port
    )

    try:
        while not (name := prompt_name()) or not isinstance(name, str):
            pass

        while not (gateway_host := prompt_gateway_host()) or not validate_ip_or_dns(gateway_host):
            pass

        while not (gateway_port := prompt_gateway_port()) or not validate_port(gateway_port):
            pass

        token = terminal.prompt(text="Token", default=None)

    except (KeyboardInterrupt, EOFError):
        os._exit(1)

    return name, ConfigContext(
        token=token,
        gateway_host=gateway_host,
        gateway_port=gateway_port,
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
