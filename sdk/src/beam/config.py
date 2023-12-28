import configparser
import functools
import os
import sys
from contextlib import contextmanager
from typing import Any, Callable, NamedTuple, Optional, Type, Union, cast

from grpclib.client import Channel, Stream, _MetadataLike
from grpclib.const import Cardinality
from grpclib.metadata import Deadline
from multidict import MultiDict

from beam import terminal
from beam.aio import run_sync
from beam.clients.gateway import ConfigureResponse, GatewayServiceStub

DEFAULT_CONFIG_FILE_PATH = "~/.beam/.config"
DEFAULT_PROFILE_NAME = "default"
DEFAULT_GATEWAY_HOST = "0.0.0.0"
DEFAULT_GATEWAY_PORT = "1993"


class GatewayConfig(NamedTuple):
    gateway_url: str = DEFAULT_GATEWAY_HOST
    gateway_port: str = DEFAULT_GATEWAY_PORT
    token: Optional[str] = None


class AuthenticatedChannel(Channel):
    def __init__(self, *args, token: Optional[str] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self._token = token

    def request(
        self,
        name: str,
        cardinality: Cardinality,
        request_type: Type,
        reply_type: Type,
        *,
        timeout: Optional[float] = None,
        deadline: Optional[Deadline] = None,
        metadata: Optional[_MetadataLike] = None,
    ) -> Stream:
        if self._token:
            metadata = cast(MultiDict, MultiDict(metadata or ()))
            metadata["authorization"] = f"Bearer {self._token}"

        return super().request(
            name,
            cardinality,
            request_type,
            reply_type,
            timeout=timeout,
            deadline=deadline,
            metadata=metadata,
        )


def load_config_from_file() -> GatewayConfig:
    config_path = os.path.expanduser(DEFAULT_CONFIG_FILE_PATH)
    config = configparser.ConfigParser()

    if not os.path.exists(config_path):
        return GatewayConfig()

    config.read(config_path)

    gateway_url = config.get(DEFAULT_PROFILE_NAME, "gateway_url", fallback=DEFAULT_GATEWAY_HOST)
    gateway_port = config.get(DEFAULT_PROFILE_NAME, "gateway_port", fallback=DEFAULT_GATEWAY_PORT)
    token = config.get(DEFAULT_PROFILE_NAME, "token", fallback=None)

    return GatewayConfig(gateway_url, gateway_port, token)


def save_config_to_file(config: GatewayConfig) -> None:
    config_path = os.path.expanduser(DEFAULT_CONFIG_FILE_PATH)
    os.makedirs(os.path.dirname(config_path), exist_ok=True)

    config_parser = configparser.ConfigParser()
    config_parser[DEFAULT_PROFILE_NAME] = config._asdict()

    with open(config_path, "w") as file:
        config_parser.write(file)


def get_gateway_config() -> GatewayConfig:
    return load_config_from_file()


def get_gateway_channel() -> Channel:
    config: GatewayConfig = get_gateway_config()
    channel: Union[AuthenticatedChannel, None] = None

    if config.token is None:
        channel = AuthenticatedChannel(
            host=config.gateway_url,
            port=int(config.gateway_port),
            ssl=True if config.gateway_port == "443" else False,
            token=None,
        )

        terminal.header("Welcome to Beam! Let's get started 📡")

        gateway_url = terminal.prompt(text="Gateway host", default="0.0.0.0")
        gateway_port = terminal.prompt(text="Gateway port", default="1993")
        token = terminal.prompt(text="Token", default=None)

        config = config._replace(gateway_url=gateway_url, gateway_port=gateway_port, token=token)

        terminal.header("Configuring gateway")
        gateway_stub = GatewayServiceStub(channel=channel)
        config_response: ConfigureResponse = run_sync(gateway_stub.configure(name="test-thing"))
        if not config_response.ok:
            channel.close()
            terminal.error("Unable to configure gateway")

        # save_config_to_file(config)

    else:
        channel = AuthenticatedChannel(
            host=config.gateway_url,
            port=int(config.gateway_port),
            ssl=True if config.gateway_port == "443" else False,
            token=config.token,
        )

    return channel


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
