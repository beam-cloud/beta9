import configparser
import functools
import os
import sys
import traceback
from contextlib import contextmanager
from typing import Any, Callable, NamedTuple, Optional, Type, Union, cast

from grpclib.client import Channel, Stream, _MetadataLike
from grpclib.const import Cardinality
from grpclib.metadata import Deadline
from multidict import MultiDict
from rich import prompt

from . import terminal
from .aio import run_sync
from .clients.gateway import AuthorizeRequest, AuthorizeResponse, GatewayServiceStub
from .exceptions import RunnerException

DEFAULT_CONFIG_FILE_PATH = "~/.beta9/creds"
DEFAULT_PROFILE_NAME = "default"
DEFAULT_GATEWAY_HOST = "0.0.0.0"
DEFAULT_GATEWAY_PORT = "1993"
DEFAULT_HTTP_PORT = "1994"


class GatewayConfig(NamedTuple):
    name: str = DEFAULT_PROFILE_NAME
    gateway_host: str = DEFAULT_GATEWAY_HOST
    gateway_port: str = DEFAULT_GATEWAY_PORT
    http_port: str = DEFAULT_HTTP_PORT
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

    gateway_host = config.get(DEFAULT_PROFILE_NAME, "gateway_host", fallback=DEFAULT_GATEWAY_HOST)
    gateway_port = config.get(DEFAULT_PROFILE_NAME, "gateway_port", fallback=DEFAULT_GATEWAY_PORT)
    http_port = config.get(DEFAULT_PROFILE_NAME, "http_port", fallback=DEFAULT_HTTP_PORT)
    token = config.get(DEFAULT_PROFILE_NAME, "token", fallback=None)

    return GatewayConfig(DEFAULT_PROFILE_NAME, gateway_host, gateway_port, http_port, token)


def save_config_to_file(*, config: GatewayConfig, name: str) -> None:
    config_path = os.path.expanduser(DEFAULT_CONFIG_FILE_PATH)
    os.makedirs(os.path.dirname(config_path), exist_ok=True)

    if not name:
        name = DEFAULT_PROFILE_NAME

    config_parser = configparser.ConfigParser()
    config_parser.read(config_path)

    if config_parser.has_section(name):
        if not prompt.Confirm.ask(f"Configuration for {name} already exists. Overwrite?"):
            return
    else:
        config_parser.add_section(name)

    config_parser.set(name, "gateway_host", config.gateway_host)
    config_parser.set(name, "gateway_port", config.gateway_port)
    config_parser.set(name, "http_port", config.http_port)
    config_parser.set(name, "token", config.token or "")

    with open(config_path, "w") as file:
        config_parser.write(file)


def get_gateway_config() -> GatewayConfig:
    gateway_host = os.getenv("BETA9_GATEWAY_HOST", None)
    gateway_port = os.getenv("BETA9_GATEWAY_PORT", None)
    http_port = os.getenv("BETA9_HTTP_PORT", None)
    token = os.getenv("BETA9_TOKEN", None)

    if gateway_host and gateway_port and token:
        return GatewayConfig(
            gateway_host=gateway_host, gateway_port=gateway_port, http_port=http_port, token=token
        )

    return load_config_from_file()


def configure_gateway_credentials(
    config: GatewayConfig,
    *,
    name: Optional[str] = None,
    gateway_host: Optional[str] = None,
    gateway_port: Optional[str] = None,
    token: Optional[str] = None,
) -> GatewayConfig:
    terminal.header("Welcome to Beta9! Let's get started 📡")

    name = name or terminal.prompt(text="Profile name", default=DEFAULT_PROFILE_NAME)
    gateway_host = gateway_host or terminal.prompt(
        text="Gateway host", default=DEFAULT_GATEWAY_HOST
    )
    gateway_port = gateway_port or terminal.prompt(
        text="Gateway port", default=DEFAULT_GATEWAY_PORT
    )
    http_port = terminal.prompt(text="HTTP port", default=DEFAULT_HTTP_PORT)
    token = token or terminal.prompt(text="Token", default=None)

    config = config._replace(
        name=name,
        gateway_host=gateway_host,
        gateway_port=gateway_port,
        http_port=http_port,
        token=token,
    )
    terminal.header("Configuring gateway")

    return config


def get_gateway_channel() -> Channel:
    if os.getenv("CI"):
        return Channel(host="localhost", port=50051, ssl=False)

    config: GatewayConfig = get_gateway_config()
    channel: Union[AuthenticatedChannel, None] = None

    if not config.token:
        config = configure_gateway_credentials(
            config,
            name=config.name,
            gateway_host=config.gateway_host,
            gateway_port=config.gateway_port,
        )

        channel = AuthenticatedChannel(
            host=config.gateway_host,
            port=int(config.gateway_port),
            ssl=True if config.gateway_port == "443" else False,
            token=config.token,
        )

        terminal.header("Authorizing with gateway")

        gateway_stub = GatewayServiceStub(channel=channel)
        auth_response: AuthorizeResponse = run_sync(gateway_stub.authorize(AuthorizeRequest()))
        if not auth_response.ok:
            channel.close()
            terminal.error(f"Unable to authorize with gateway: {auth_response.error_msg}")

        terminal.header("Authorized 🎉")

        token = config.token
        if not token:
            token = auth_response.new_token

        config = config._replace(
            gateway_host=config.gateway_host, gateway_port=config.gateway_port, token=token
        )
        save_config_to_file(config=config, name=config.name)

        channel.close()  # Close unauthenticated channel

    channel = AuthenticatedChannel(
        host=config.gateway_host,
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
    except RunnerException as exc:
        exit_code = exc.code
        raise
    except SystemExit as exc:
        exit_code = exc.code
        raise
    except BaseException:
        exit_code = 1
    finally:
        if channel := locals().get("channel", None):
            channel.close()

        if exit_code != 0:
            print(traceback.format_exc())
            sys.exit(exit_code)


def with_runner_context(func: Callable) -> Callable:
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        with runner_context() as c:
            return func(*args, **kwargs, channel=c)

    return wrapper
