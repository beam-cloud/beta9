import configparser
import inspect
import ipaddress
import os
import socket
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Tuple, Union

from . import terminal

DEFAULT_CONTEXT_NAME = "default"
DEFAULT_GATEWAY_HOST = "0.0.0.0"
DEFAULT_GATEWAY_PORT = 1993


@dataclass
class ConfigContext:
    token: Optional[str] = None
    gateway_host: Optional[str] = None
    gateway_port: Optional[int] = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ConfigContext":
        return cls(**{k: v for k, v in data.items() if k in inspect.signature(cls).parameters})

    def to_dict(self) -> dict:
        return {k: ("" if not v else v) for k, v in asdict(self).items()}

    def use_ssl(self) -> bool:
        if self.gateway_port in [443, "443"]:
            return True
        return False


def get_config_path(base_dir: str = "~/.beta9") -> Path:
    """
    Gets the path of the config.

    Can be overridden by setting the CONFIG_PATH environment variable.

    Args:
        base_dir: Base path of the config file. Defaults to "~/.beta9".

    Returns:
        A Path object.
    """
    path = Path(f"{base_dir}/config.ini").expanduser()

    if config_path := os.getenv("CONFIG_PATH"):
        if os.path.exists(config_path):
            return Path(config_path).expanduser()

    return path


def load_config(path: Optional[Union[str, Path]] = None) -> Dict[str, ConfigContext]:
    if path is None:
        path = get_config_path()

    path = Path(path)
    if not path.exists():
        return {}

    parser = configparser.ConfigParser(default_section=DEFAULT_CONTEXT_NAME)
    parser.read(path)

    return {k: ConfigContext.from_dict(v) for k, v in parser.items()}  # type:ignore


def save_config(
    contexts: Mapping[str, ConfigContext], path: Optional[Union[Path, str]] = None
) -> None:
    if path is None:
        path = get_config_path()

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    parser = configparser.ConfigParser(default_section=DEFAULT_CONTEXT_NAME)
    parser.read_dict({k: v.to_dict() for k, v in contexts.items()})

    with open(path, "w") as file:
        parser.write(file)


def is_config_empty(path: Optional[Union[Path, str]] = None) -> bool:
    if path is None:
        path = get_config_path()

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
    contexts = load_config()

    try:
        if name in contexts:
            text = f"Context '{name}' already exists. Overwrite?"
            if terminal.prompt(text=text, default="n").lower() in ["n", "no"]:
                return name, contexts[name]

        while not name:
            name = terminal.prompt(text="Context Name", default=DEFAULT_CONTEXT_NAME)

        while not gateway_host:
            gateway_host = terminal.prompt(text="Gateway Host", default=DEFAULT_GATEWAY_HOST)
            gateway_host = gateway_host if validate_ip_or_dns(gateway_host) else ""

        while not gateway_port:
            gateway_port = terminal.prompt(text="Gateway Port", default=DEFAULT_GATEWAY_PORT)
            gateway_port = gateway_port if validate_port(gateway_port) else 0

        token = terminal.prompt(text="Token", default=None)

    except (KeyboardInterrupt, EOFError):
        os._exit(1)

    contexts[name] = ConfigContext(
        token=token,
        gateway_host=gateway_host,
        gateway_port=gateway_port,
    )

    save_config(contexts)

    return name, contexts[name]


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
        if int(value) > 0:
            return True
    except ValueError:
        pass

    return False
