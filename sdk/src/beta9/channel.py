import functools
import os
import sys
import traceback
from contextlib import contextmanager
from typing import Any, Callable, Optional, Type, cast

from grpclib.client import Channel, Stream
from grpclib.const import Cardinality
from grpclib.metadata import Deadline, _MetadataLike
from multidict import MultiDict

from beta9.config import (
    ConfigContext,
    get_config_context,
    load_config,
    prompt_for_config_context,
    save_config,
)

from . import terminal
from .aio import run_sync
from .clients.gateway import AuthorizeRequest, AuthorizeResponse, GatewayServiceStub
from .clients.volume import VolumeServiceStub
from .config import DEFAULT_CONTEXT_NAME, SDKSettings
from .exceptions import RunnerException


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


def get_channel(context: Optional[ConfigContext] = None) -> Channel:
    if os.getenv("CI"):
        return Channel(host="localhost", port=50051, ssl=False)

    if not context:
        _, context = prompt_for_config_context()

    return AuthenticatedChannel(
        host=context.gateway_host,
        port=context.gateway_port,
        ssl=context.use_ssl(),
        token=context.token,
    )


def prompt_first_auth(settings: SDKSettings) -> None:
    terminal.header(f"Welcome to {settings.name.title()}! Let's get started 📡")
    terminal.print(settings.ascii_logo, highlight=True)

    name, context = prompt_for_config_context(
        name=DEFAULT_CONTEXT_NAME,
        gateway_host=settings.gateway_host,
        gateway_port=settings.gateway_port,
    )

    channel = AuthenticatedChannel(
        host=context.gateway_host,
        port=context.gateway_port,
        ssl=context.use_ssl(),
        token=context.token,
    )

    terminal.header("Authorizing with gateway")
    with ServiceClient.with_channel(channel) as client:
        res: AuthorizeResponse
        res = run_sync(client.gateway.authorize(AuthorizeRequest()))
        if not res.ok:
            terminal.error(f"Unable to authorize with gateway: {res.error_msg}")

        terminal.header("Authorized 🎉")

    # Set new token, if one was returned
    context.token = res.new_token if res.new_token else context.token

    # Load config, add new context
    contexts = load_config(settings.config_path)
    contexts[name] = context
    contexts[DEFAULT_CONTEXT_NAME] = context

    # Write updated contexts to config
    save_config(contexts, settings.config_path)


@contextmanager
def runner_context():
    exit_code = 0

    try:
        config = get_config_context()
        channel: Channel = get_channel(config)
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


class ServiceClient:
    def __init__(self, config: Optional[ConfigContext] = None) -> None:
        self._config: Optional[ConfigContext] = config
        self._channel: Optional[Channel] = None
        self._gateway: Optional[GatewayServiceStub] = None
        self._volume: Optional[VolumeServiceStub] = None

    def __enter__(self) -> "ServiceClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.channel.close()

    @classmethod
    def with_channel(cls, channel: Channel) -> "ServiceClient":
        self = cls()
        self.channel = channel
        return self

    @property
    def channel(self) -> Channel:
        if not self._channel:
            self._channel = get_channel(self._config)
        return self._channel

    @channel.setter
    def channel(self, value) -> None:
        if not value or not isinstance(value, Channel):
            raise ValueError("Invalid channel")
        self._channel = value

    @property
    def gateway(self) -> GatewayServiceStub:
        if not self._gateway:
            self._gateway = GatewayServiceStub(self.channel)
        return self._gateway

    @property
    def volume(self) -> VolumeServiceStub:
        if not self._volume:
            self._volume = VolumeServiceStub(self.channel)
        return self._volume

    def close(self) -> None:
        self.channel.close()
