import atexit
import functools
import hashlib
import os
from importlib.metadata import PackageNotFoundError, version
import sys
import time
import traceback
import weakref
from abc import ABC, abstractmethod
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Callable, Generator, List, NewType, Optional, Sequence, Tuple, cast

import grpc
import requests
from grpc import ChannelCredentials, RpcError
from grpc._interceptor import _Channel as InterceptorChannel

from . import terminal
from .clients.disk import DiskServiceStub
from .clients.gateway import (
    AuthorizeRequest,
    AuthorizeResponse,
    ExportWorkspaceConfigRequest,
    GatewayServiceStub,
)
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
from .env import is_remote
from .exceptions import RunnerException

GRPC_MAX_MESSAGE_SIZE = 16 * 1024 * 1024
_channels = weakref.WeakSet()
_deadline = ContextVar("beta9_rpc_deadline", default=None)


@contextmanager
def rpc_timeout(seconds: float):
    """Bound all RPCs in this scope, including time already spent between calls."""
    deadline = time.monotonic() + seconds
    current = _deadline.get()
    token = _deadline.set(min(current, deadline) if current is not None else deadline)
    try:
        yield
    finally:
        _deadline.reset(token)


@atexit.register
def _close_channels() -> None:
    # Close before interpreter teardown, while gRPC's monitor threads can still exit.
    for channel in list(_channels):
        channel.close()


def channel_reconnect_event(connect_status: grpc.ChannelConnectivity) -> None:
    if connect_status not in (
        grpc.ChannelConnectivity.CONNECTING,
        grpc.ChannelConnectivity.IDLE,
        grpc.ChannelConnectivity.READY,
        grpc.ChannelConnectivity.SHUTDOWN,
    ):
        terminal.warn("Connection lost, reconnecting...")


class Channel(InterceptorChannel):
    def __init__(
        self,
        addr: str,
        token: Optional[str] = None,
        credentials: Optional[ChannelCredentials] = None,
        options: Optional[Sequence[Tuple[str, Any]]] = None,
        retry: Tuple[Callable[[grpc.ChannelConnectivity], None], bool] = (
            channel_reconnect_event,
            True,
        ),
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ):
        if options is None:
            options = [
                ("grpc.max_receive_message_length", GRPC_MAX_MESSAGE_SIZE),
                ("grpc.max_send_message_length", GRPC_MAX_MESSAGE_SIZE),
            ]

        if credentials is not None:
            channel = grpc.secure_channel(addr, credentials, options=options)
        elif addr.endswith("443"):
            channel = grpc.secure_channel(addr, grpc.ssl_channel_credentials(), options=options)
        else:
            channel = grpc.insecure_channel(addr, options=options)

        # NOTE: we observed that in a multiprocessing context, this
        # retry mechanism did not work as expected. We're not sure why,
        # but for now, just don't subscribe to these events in containers
        if not is_remote():
            channel.subscribe(*retry)

        interceptor = AuthTokenInterceptor(token, metadata)
        super().__init__(channel=channel, interceptor=interceptor)
        # Per-channel caches are shared across channels to the same gateway and token.
        self.cache_key = hashlib.sha256(f"{addr}\n{token or ''}".encode()).hexdigest()
        _channels.add(self)

    def close(self) -> None:
        super().close()
        _channels.discard(self)


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

    def __init__(
        self, token: Optional[str] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None
    ):
        """Initialize the interceptor with an optional authentication token and
        extra metadata attached to every call."""
        self._token = token
        self._metadata = list(metadata or [])

    def _add_auth_metadata(
        self,
        client_call_details: ClientCallDetails,
    ) -> ClientCallDetails:
        """Add authentication metadata to the client call."""
        headers = list(self._metadata)
        if self._token:
            headers.append(("authorization", f"Bearer {self._token}"))
        if headers:
            new_metadata = list(client_call_details.metadata or []) + headers
        else:
            new_metadata = client_call_details.metadata

        return client_call_details._replace(metadata=cast(MetadataType, new_metadata))

    def intercept_call(self, continuation, client_call_details, request):
        """Intercept all types of calls to add auth token."""
        new_details = self._add_auth_metadata(client_call_details)
        if (deadline := _deadline.get()) is not None:
            remaining = max(0, deadline - time.monotonic())
            timeout = new_details.timeout
            new_details = new_details._replace(
                timeout=min(timeout, remaining) if timeout is not None else remaining
            )

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
        terminal.error(
            "Unauthorized: Invalid auth token provided.",
            code="NOT_AUTHENTICATED",
            hint=f"Run `{terminal.cli_name()} configure` or pass --context.",
        )
    elif code == grpc.StatusCode.UNAVAILABLE:
        terminal.error("Unable to connect to gateway.", code="GATEWAY_UNAVAILABLE")
    elif code == grpc.StatusCode.CANCELLED:
        terminal.error("Request cancelled.", code="CANCELLED")
    elif code == grpc.StatusCode.DEADLINE_EXCEEDED:
        terminal.error("Request timed out.", code="TIMEOUT")
    elif code == grpc.StatusCode.RESOURCE_EXHAUSTED:
        terminal.error(f"Resource limit exceeded: {details}", code="CAPACITY")
    elif code == grpc.StatusCode.NOT_FOUND:
        terminal.error(f"Not found: {details}", code="NOT_FOUND")
    elif code == grpc.StatusCode.INVALID_ARGUMENT:
        terminal.error(f"Invalid request: {details}", code="INVALID_CONFIG")
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


def caller_metadata() -> List[Tuple[str, str]]:
    """Attribution headers: BETA9_CALLER overrides `cli/<version>`, BETA9_AGENT_SESSION groups one agent run."""
    caller = os.getenv("BETA9_CALLER") or f"cli/{sdk_version()}"
    metadata = [("x-beta9-caller", caller)]
    if session := os.getenv("BETA9_AGENT_SESSION"):
        metadata.append(("x-beta9-agent-session", session))
    return metadata


def sdk_version() -> str:
    try:
        return version("beta9")
    except PackageNotFoundError:
        return "unknown"


def get_channel(context: Optional[ConfigContext] = None) -> Channel:
    if not context:
        _, context = prompt_for_config_context()

    channel = Channel(
        addr=f"{context.gateway_host}:{context.gateway_port}",
        token=context.token,
        metadata=caller_metadata(),
    )
    channel.config = context
    return channel


def prompt_first_auth(settings: SDKSettings) -> None:
    if settings.api_token:
        name = DEFAULT_CONTEXT_NAME
        context = ConfigContext(
            token=settings.api_token,
            gateway_host=settings.gateway_host,
            gateway_port=settings.gateway_port,
            api_url=settings.api_url,
        )
    else:
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


def pass_channel(func: Callable) -> Callable:
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        config = get_config_context()
        with get_channel(config) as channel:
            return func(*args, **kwargs, channel=channel)

    return wrapper


@contextmanager
def handle_error():
    exit_code = 0
    try:
        yield
    except RpcError as exc:
        handle_grpc_error(exc)
    except RunnerException as exc:
        exit_code = exc.code
    except SystemExit as exc:
        exit_code = exc.code
        raise
    except BaseException:
        exit_code = 1
    finally:
        if exit_code != 0:
            print(traceback.format_exc())
            sys.exit(exit_code)


@contextmanager
def runner_context() -> Generator[Channel, None, None]:
    with handle_error():
        config = get_config_context()
        channel = get_channel(config)
        yield channel
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
        self._disk: Optional[DiskServiceStub] = None
        self._secret: Optional[SecretServiceStub] = None
        self._http: Optional["GatewayHTTP"] = None

    def __enter__(self) -> "ServiceClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

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
    def http(self) -> "GatewayHTTP":
        """REST access to the same gateway, resolved once per client."""
        if not self._http:
            config = self.gateway.export_workspace_config(ExportWorkspaceConfigRequest())
            scheme = "https" if config.gateway_http_tls else "http"
            base_url = self._config.api_url if self._config and self._config.api_url else None
            self._http = GatewayHTTP(
                base_url=(
                    base_url or f"{scheme}://{config.gateway_http_host}:{config.gateway_http_port}"
                ).rstrip("/"),
                workspace_id=config.workspace_id,
                token=(self._config.token if self._config else "") or self.channel.config.token,
            )
        return self._http

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
    def disk(self) -> DiskServiceStub:
        if not self._disk:
            self._disk = DiskServiceStub(self.channel)
        return self._disk

    @property
    def secret(self) -> SecretServiceStub:
        if not self._secret:
            self._secret = SecretServiceStub(self.channel)
        return self._secret

    def close(self) -> None:
        if self._channel:
            self._channel.close()


class GatewayHTTP:
    """
    The gateway's REST surface (`/api/v1/...`) for one workspace. `{ws}` in a
    path is replaced with the workspace id; the caller headers are attached so
    actions are attributed like gRPC calls.
    """

    def __init__(self, base_url: str, workspace_id: str, token: str):
        self.base_url = base_url
        self.workspace_id = workspace_id
        self.headers = {"Authorization": f"Bearer {token}", **dict(caller_metadata())}

    def url(self, path: str) -> str:
        return self.base_url + path.replace("{ws}", self.workspace_id)

    def request(self, method: str, path: str, timeout: float = 60, **kwargs):
        return requests.request(
            method, self.url(path), headers=self.headers, timeout=timeout, **kwargs
        )

    def json(self, method: str, path: str, **kwargs):
        """Request and decode JSON; GatewayHTTPError on 4xx/5xx or when the gateway is unreachable."""
        try:
            response = self.request(method, path, **kwargs)
        except requests.RequestException as exc:
            raise GatewayHTTPError(0, f"Request failed: {exc}")
        if response.status_code >= 400:
            try:
                message = response.json().get("message") or response.text
            except ValueError:
                message = response.text
            raise GatewayHTTPError(response.status_code, message or f"HTTP {response.status_code}")
        return response.json() if response.content else None


def http_error_code(status: int) -> str:
    """The CLI error code for an HTTP status, for `--json` consumers."""
    return {
        0: "GATEWAY_UNAVAILABLE",
        401: "NOT_AUTHENTICATED",
        404: "NOT_FOUND",
        409: "ALREADY_EXISTS",
    }.get(status, "ERROR")


class GatewayHTTPError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message

    @property
    def code(self) -> str:
        return http_error_code(self.status)
