import functools
import os
import sys
import traceback
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any, Callable, List, NewType, Optional, Tuple, cast

import grpc
from grpc import ChannelCredentials
from grpc._interceptor import _Channel as InterceptorChannel

from . import terminal
from .clients.gateway import AuthorizeRequest, AuthorizeResponse, GatewayServiceStub
from .clients.secret import SecretServiceStub
from .clients.volume import VolumeServiceStub
from .config import (
    DEFAULT_CONTEXT_NAME,
    ConfigContext,
    SDKSettings,
    get_config_context,
    load_config,
    prompt_for_config_context,
    save_config,
)
from .exceptions import RunnerException


class Channel(InterceptorChannel):
    def __init__(
        self,
        addr: str,
        token: Optional[str] = None,
        credentials: Optional[ChannelCredentials] = None,
    ):
        if credentials is not None:
            channel = grpc.secure_channel(addr, credentials)
        elif addr.endswith("443"):
            channel = grpc.secure_channel(addr, grpc.ssl_channel_credentials())
        else:
            channel = grpc.insecure_channel(addr)

        interceptor = AuthTokenInterceptor(token)
        super().__init__(channel=channel, interceptor=interceptor)


MetadataType = NewType("MetadataType", List[Tuple[Any, Any]])


class ClientCallDetails(ABC):
    @property
    @abstractmethod
    def metadata(self) -> MetadataType:
        pass

    @abstractmethod
    def _replace(self, metadata: MetadataType) -> "ClientCallDetails":
        pass


class AuthTokenInterceptor(
    grpc.UnaryUnaryClientInterceptor,
    grpc.UnaryStreamClientInterceptor,
    grpc.StreamUnaryClientInterceptor,
    grpc.StreamStreamClientInterceptor,
):
    """A generic interceptor to add an authentication token to gRPC requests."""

    def __init__(self, token: Optional[str] = None):
        """Initialize the interceptor with an optional authentication token."""
        self._token = token

    def _add_auth_metadata(
        self,
        client_call_details: ClientCallDetails,
    ) -> ClientCallDetails:
        """Add authentication metadata to the client call."""
        if self._token:
            auth_headers = [("authorization", f"Bearer {self._token}")]
            if client_call_details.metadata is not None:
                new_metadata = client_call_details.metadata + auth_headers
            else:
                new_metadata = auth_headers
        else:
            new_metadata = client_call_details.metadata

        return client_call_details._replace(metadata=cast(MetadataType, new_metadata))

    def intercept_call(self, continuation, client_call_details, request):
        """Intercept all types of calls to add auth token."""
        new_details = self._add_auth_metadata(client_call_details)

        return continuation(new_details, request)

    def intercept_call_stream(self, continuation, client_call_details, request_iterator):
        return self.intercept_call(continuation, client_call_details, request=request_iterator)

    # Implement the four necessary interceptor methods using intercept_call
    intercept_unary_unary = intercept_call
    intercept_unary_stream = intercept_call
    intercept_stream_unary = intercept_call_stream
    intercept_stream_stream = intercept_call_stream


def handle_grpc_error(error: grpc.RpcError):
    code = error.code()
    details = error.details()

    if code == grpc.StatusCode.UNAUTHENTICATED:
        terminal.error("Unauthorized: Invalid auth token provided.")
    elif code == grpc.StatusCode.UNAVAILABLE:
        terminal.error("Unable to connect to gateway.")
    elif code == grpc.StatusCode.CANCELLED:
        return
    elif code == grpc.StatusCode.UNKNOWN:
        terminal.error(f"Error {details}")
    else:
        terminal.error(f"Unhandled GRPC error: {code}")


def with_grpc_error_handling(func: Callable) -> Callable:
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except grpc.RpcError as e:
            handle_grpc_error(e)

    return wrapper


def get_channel(context: Optional[ConfigContext] = None) -> Channel:
    if os.getenv("CI"):
        return Channel("localhost:50051")

    if not context:
        _, context = prompt_for_config_context()

    return Channel(
        addr=f"{context.gateway_host}:{context.gateway_port}",
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

    channel = Channel(
        addr=f"{context.gateway_host}:{context.gateway_port}",
        token=context.token,
    )

    terminal.header("Authorizing with gateway")
    with ServiceClient.with_channel(channel) as client:
        res: AuthorizeResponse
        res = client.gateway.authorize(AuthorizeRequest())
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
        if exit_code != 0:
            print(traceback.format_exc())
            sys.exit(exit_code)

        channel.close()


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
        self._secret: Optional[SecretServiceStub] = None

    def __enter__(self) -> "ServiceClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._channel:
            self._channel.close()

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

    @property
    def secret(self) -> SecretServiceStub:
        if not self._secret:
            self._secret = SecretServiceStub(self.channel)
        return self._secret

    def close(self) -> None:
        if self._channel:
            self._channel.close()
