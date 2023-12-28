import configparser
import functools
import os
import sys
from contextlib import contextmanager
from typing import Any, Callable, NamedTuple, Optional

from grpclib.client import Channel

from beam import terminal

DEFAULT_CONFIG_FILE_PATH = "~/.beam/.config"
DEFAULT_CONTEXT_NAME = "default"


class GatewayConfig(NamedTuple):
    gateway_url: str = "0.0.0.0"
    gateway_port: str = "1993"
    token: Optional[str] = None


def load_config_from_file() -> GatewayConfig:
    config_path = os.path.expanduser(DEFAULT_CONFIG_FILE_PATH)
    config = configparser.ConfigParser()

    if not os.path.exists(config_path):
        return GatewayConfig()

    config.read(config_path)
    gateway_url = config.get(DEFAULT_CONTEXT_NAME, "gateway_url", fallback="0.0.0.0")
    gateway_port = config.get(DEFAULT_CONTEXT_NAME, "gateway_port", fallback="1993")
    token = config.get(DEFAULT_CONTEXT_NAME, "token", fallback=None)

    return GatewayConfig(gateway_url, gateway_port, token)


def save_config_to_file(config: GatewayConfig) -> None:
    config_path = os.path.expanduser(DEFAULT_CONFIG_FILE_PATH)
    os.makedirs(os.path.dirname(config_path), exist_ok=True)

    config_parser = configparser.ConfigParser()
    config_parser[DEFAULT_CONTEXT_NAME] = config._asdict()

    with open(config_path, "w") as file:
        config_parser.write(file)


def get_gateway_config() -> GatewayConfig:
    config = load_config_from_file()

    if config.token is None:
        terminal.prompt("Welcome! Enter your token: ")

        token = input()

        if token.strip() == "":
            terminal.error("Invalid token.")

        config = config._replace(token=token)
        save_config_to_file(config)

    return config


def get_gateway_channel() -> Channel:
    config: GatewayConfig = get_gateway_config()
    return Channel(
        host=config.gateway_url,
        port=int(config.gateway_port),
        ssl=True if config.gateway_port == "443" else False,
    )


@contextmanager
def runner_context():
    exit_code = 0

    try:
        channel: Channel = get_gateway_channel()
        yield channel
    except SystemExit as exc:
        exit_code = exc.code
        raise
    except BaseException:
        exit_code = 1
    finally:
        channel.close()

        if exit_code != 0:
            sys.exit(exit_code)


def with_runner_context(func: Callable) -> Callable:
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        with runner_context() as c:
            return func(*args, **kwargs, channel=c)

    return wrapper
