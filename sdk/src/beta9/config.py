import configparser
import ipaddress
import os
import socket
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping, Optional, Tuple, Union

from . import terminal

DEFAULT_CONTEXT_NAME = "default"
DEFAULT_GATEWAY_HOST = "0.0.0.0"
DEFAULT_GATEWAY_PORT = 1993
DEFAULT_GATEWAY_HTTP_PORT = 1994
DEFAULT_CONFIG_FILE_PATH = Path("~/.beta9/config.ini").expanduser()

if config_path := os.getenv("CONFIG_PATH"):
    if os.path.exists(config_path):
        DEFAULT_CONFIG_FILE_PATH = Path(config_path)


@dataclass
class ConfigContext:
    token: Optional[str] = None
    gateway_host: Optional[str] = None
    gateway_port: Optional[int] = None
    gateway_http_port: Optional[int] = None

    def use_ssl(self) -> bool:
        if self.gateway_port in [443, "443"]:
            return True
        return False

    def to_dict(self) -> dict:
        return {k: ("" if not v else v) for k, v in asdict(self).items()}


def load_config(
    path: Union[str, Path] = DEFAULT_CONFIG_FILE_PATH,
) -> dict[str, ConfigContext]:
    path = Path(path)
    if not path.exists():
        return {}

    parser = configparser.ConfigParser(default_section=DEFAULT_CONTEXT_NAME)
    parser.read(path)

    return {k: ConfigContext(**v) for k, v in parser.items()}  # type:ignore


def save_config(
    contexts: Mapping,
    path: Union[Path, str] = DEFAULT_CONFIG_FILE_PATH,
) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    parser = configparser.ConfigParser(default_section=DEFAULT_CONTEXT_NAME)
    parser.read_dict({k: v.to_dict() for k, v in contexts.items()})

    with open(path, "w") as file:
        parser.write(file)


def is_config_empty(
    path: Union[Path, str] = DEFAULT_CONFIG_FILE_PATH,
) -> bool:
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

    terminal.header(f"Context '{name}' does not exist. Let's try setting it up.")
    _, config = prompt_for_config_context(name=name)
    return config


def prompt_for_config_context(
    name: Optional[str] = None,
    token: Optional[str] = None,
    gateway_host: Optional[str] = None,
    gateway_port: Optional[int] = None,
    gateway_http_port: Optional[int] = None,
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

        while not gateway_http_port:
            gateway_http_port = terminal.prompt(
                text="Gateway HTTP Port", default=DEFAULT_GATEWAY_HTTP_PORT
            )
            gateway_http_port = gateway_http_port if validate_port(gateway_http_port) else 0

        token = terminal.prompt(text="Token", default=None, password=True)

    except (KeyboardInterrupt, EOFError):
        os._exit(1)

    contexts[name] = ConfigContext(
        token=token,
        gateway_host=gateway_host,
        gateway_port=gateway_port,
        gateway_http_port=gateway_http_port,
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
