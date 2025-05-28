import inspect
import threading
import urllib.parse
from typing import Any, Callable, ClassVar, Optional

from .. import terminal
from ..abstractions.base.container import Container
from ..abstractions.base.runner import SHELL_STUB_TYPE
from ..channel import with_grpc_error_handling
from ..clients.gateway import DeployStubRequest, DeployStubResponse, GetUrlRequest, GetUrlResponse
from ..clients.shell import CreateShellInExistingContainerRequest, CreateStandaloneShellRequest
from ..config import ConfigContext
from .base.runner import RunnerAbstraction
from .shell import SSHShell


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

    def _is_abstraction_callable_wrapper(self, func: Callable, ab_name: str) -> bool:
        return (
            hasattr(func, "parent")
            and inspect.isclass(type(func.parent))
            and func.parent.__class__.__name__ == ab_name
        )

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

        if self.parent.on_deploy and self._is_abstraction_callable_wrapper(
            self.parent.on_deploy, "Function"
        ):
            terminal.header("Running on_deploy hook")
            self.parent.on_deploy()

        if not self.parent.prepare_runtime(
            func=self.func, stub_type=self.deployment_stub_type, force_create_stub=True
        ):
            return False

        terminal.header("Deploying")
        deploy_response: DeployStubResponse = self.parent.gateway_stub.deploy_stub(
            DeployStubRequest(
                stub_id=self.parent.stub_id,
                name=self.parent.name,
            )
        )

        self.parent.deployment_id = deploy_response.deployment_id
        if deploy_response.ok:
            terminal.header("Deployed 🎉")
            if invocation_details_func:
                invocation_details_func(**invocation_details_options)
            else:
                self.parent.print_invocation_snippet(**invocation_details_options)

        return deploy_response.ok

    def _attach_and_sync(self, container_id: str, sync_dir: str):
        try:
            container = Container(
                container_id=container_id,
            )
            container.attach(container_id=container_id, sync_dir=sync_dir, hide_logs=True)
        except BaseException:
            terminal.header(f"Stopped syncing directory '{sync_dir}'")

    @with_grpc_error_handling
    def shell(
        self, url_type: str = "", sync_dir: Optional[str] = None, container_id: Optional[str] = None
    ):
        # First, spin up the shell container
        username = "root"
        password = ""

        if container_id:
            with terminal.progress("Creating shell..."):
                create_shell_response = self.parent.shell_stub.create_shell_in_existing_container(
                    CreateShellInExistingContainerRequest(
                        container_id=container_id,
                    )
                )

                if not create_shell_response.ok:
                    return terminal.error(
                        f"Failed to create shell: {create_shell_response.err_msg} ❌"
                    )

                username = create_shell_response.username
                password = create_shell_response.password
                self.parent.stub_id = create_shell_response.stub_id
        else:
            stub_type = SHELL_STUB_TYPE

            if not self.parent.prepare_runtime(
                func=self.func, stub_type=stub_type, force_create_stub=True
            ):
                return False

            create_shell_response = self.parent.shell_stub.create_standalone_shell(
                CreateStandaloneShellRequest(
                    stub_id=self.parent.stub_id,
                )
            )
            if not create_shell_response.ok:
                return terminal.error(f"Failed to create shell: {create_shell_response.err_msg} ❌")

            container_id = create_shell_response.container_id
            username = create_shell_response.username
            password = create_shell_response.password

        # Then, we can retrieve the URL and establish a tunnel
        res: GetUrlResponse = self.parent.gateway_stub.get_url(
            GetUrlRequest(
                stub_id=self.parent.stub_id,
                deployment_id=getattr(self, "deployment_id", ""),
                url_type="/path",
                is_shell=True,
            )
        )
        if not res.ok:
            return terminal.error(f"Failed to get shell connection URL: {res.err_msg} ❌")

        # Parse the URL to extract the container_id
        parsed_url = urllib.parse.urlparse(res.url)
        proxy_host, proxy_port = parsed_url.hostname, parsed_url.port

        if not proxy_port:
            proxy_port = 443 if parsed_url.scheme == "https" else 80

        if sync_dir:
            threading.Thread(
                target=self._attach_and_sync,
                args=(container_id, sync_dir),
                daemon=True,
            ).start()

        with SSHShell(
            host=proxy_host,
            port=proxy_port,
            path=parsed_url.path,
            container_id=container_id,
            stub_id=self.parent.stub_id,
            auth_token=self.parent.config_context.token,
            username=username,
            password=password,
        ) as shell:
            shell.start()
