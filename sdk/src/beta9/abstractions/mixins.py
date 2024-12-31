import traceback
import urllib.parse
from typing import Any, Callable, ClassVar, Optional

from .. import terminal
from ..abstractions.base.runner import SHELL_STUB_TYPE
from ..channel import with_grpc_error_handling
from ..clients.gateway import DeployStubRequest, DeployStubResponse, GetUrlRequest, GetUrlResponse
from ..clients.shell import CreateShellRequest
from ..config import ConfigContext
from .base.runner import RunnerAbstraction
from .shell import SSHShell, create_connect_tunnel


class DeployableMixin:
    func: Callable
    parent: RunnerAbstraction
    deployment_id: Optional[str] = None
    deployment_stub_type: ClassVar[str]

    def _validate(self):
        if not hasattr(self, "func") or not isinstance(self.func, Callable):
            raise AttributeError("func variable not set or is incorrect type")

        if not hasattr(self, "parent") or not isinstance(self.parent, RunnerAbstraction):
            raise AttributeError("parent variable not set or is incorrect type")

        if not hasattr(self, "deployment_stub_type") or not self.deployment_stub_type:
            raise AttributeError("deployment_stub_type variable not set")

    def deploy(
        self,
        name: Optional[str] = None,
        context: Optional[ConfigContext] = None,
        invocation_details_func: Optional[Callable[..., None]] = None,
        **invocation_details_options: Any,
    ) -> bool:
        self._validate()

        self.parent.name = name or self.parent.name
        if not self.parent.name:
            terminal.error(
                "You must specify an app name (either in the decorator or via the --name argument)."
            )

        if context is not None:
            self.parent.config_context = context

        if not self.parent.prepare_runtime(
            func=self.func, stub_type=self.deployment_stub_type, force_create_stub=True
        ):
            return False

        terminal.header("Deploying")
        deploy_response: DeployStubResponse = self.parent.gateway_stub.deploy_stub(
            DeployStubRequest(stub_id=self.parent.stub_id, name=self.parent.name)
        )

        self.parent.deployment_id = deploy_response.deployment_id
        if deploy_response.ok:
            terminal.header("Deployed 🎉")
            if invocation_details_func:
                invocation_details_func(**invocation_details_options)
            else:
                self.parent.print_invocation_snippet(**invocation_details_options)

        return deploy_response.ok

    @with_grpc_error_handling
    def shell(self, timeout: int = 0, url_type: str = ""):
        stub_type = SHELL_STUB_TYPE

        if not self.parent.prepare_runtime(
            func=self.func, stub_type=stub_type, force_create_stub=True
        ):
            return False

        # First, spin up the shell container
        create_shell_response = self.parent.shell_stub.create_shell(
            CreateShellRequest(
                stub_id=self.parent.stub_id,
                timeout=timeout,
            )
        )
        if not create_shell_response.ok:
            return terminal.error("Failed to create shell ❌")

        # Then, we can retrieve the URL and issue a CONNECT request / establish a tunnel
        res: GetUrlResponse = self.parent.gateway_stub.get_url(
            GetUrlRequest(
                stub_id=self.parent.stub_id,
                deployment_id=getattr(self, "deployment_id", ""),
                url_type=url_type,
            )
        )
        if not res.ok:
            return terminal.error("Failed to get shell connection URL")

        # Parse the URL to extract the container_id
        parsed_url = urllib.parse.urlparse(res.url)
        path_segments = parsed_url.path.split("/")
        if len(path_segments) < 3:
            return terminal.error("Invalid URL path")

        container_id = create_shell_response.container_id
        ssh_token = create_shell_response.token

        # Use the container_id to establish a connection
        tunnel_socket = None
        proxy_host, proxy_port = parsed_url.hostname, parsed_url.port
        try:
            tunnel_socket = create_connect_tunnel(
                proxy_host,
                proxy_port,
                self.parent.stub_id,
                container_id,
                self.parent.config_context.token,
            )
        except BaseException:
            terminal.error(f"Failed to establish ssh tunnel: {traceback.format_exc()}")

        import paramiko

        try:
            transport = paramiko.Transport(tunnel_socket)
            transport.start_client()
            transport.auth_password("runc", ssh_token)
            session = transport.open_session()

            with SSHShell(
                channel=session,
            ) as _:
                pass

        except paramiko.SSHException as e:
            print(f"SSH error: {e}")
        except EOFError:
            print("Connection closed by the server.")
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            if session:
                session.close()

            transport.close()
