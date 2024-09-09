import os
import threading
from typing import Any, Callable, List, Optional, Union

from .. import terminal
from ..abstractions.base.runner import (
    APP_DEPLOYMENT_STUB_TYPE,
    APP_SERVE_STUB_TYPE,
    APP_STUB_TYPE,
    CONTAINER_STUB_TYPE,
    BaseAbstraction,
    RunnerAbstraction,
)
from ..abstractions.image import Image
from ..abstractions.volume import Volume
from ..channel import with_grpc_error_handling
from ..clients.container import (
    CommandExecutionRequest,
    CommandExecutionResponse,
    ContainerServiceStub,
    CreateTunnelRequest,
    CreateTunnelResponse,
)
from ..env import is_local
from ..sync import FileSyncer
from ..type import GpuType, GpuTypeAlias
from .mixins import DeployableMixin


class TunnelCannotRunLocallyError(Exception):
    pass


class Tunnel(BaseAbstraction):
    def __init__(self) -> None:
        super().__init__()
        self._container_stub: Optional[ContainerServiceStub] = None

    @property
    def container_stub(self) -> ContainerServiceStub:
        if not self._container_stub:
            self._container_stub = ContainerServiceStub(self.channel)
        return self._container_stub

    @container_stub.setter
    def container_stub(self, value: ContainerServiceStub) -> None:
        self._container_stub = value

    def open(self, *, port: int) -> str:
        if is_local():
            raise TunnelCannotRunLocallyError

        container_id = os.environ["CONTAINER_ID"]

        r: CreateTunnelResponse = self.container_stub.create_tunnel(
            CreateTunnelRequest(container_id=container_id, port=port)
        )

        print(r.ok)


class Command(RunnerAbstraction):
    """
    Command allows you to run arbitrary commands in a remote container.

    Parameters:
        cpu (Union[int, float, str]):
            The number of CPU cores allocated to the container. Default is 1.0.
        memory (Union[int, str]):
            The amount of memory allocated to the container. It should be specified in
            MiB, or as a string with units (e.g. "1Gi"). Default is 128 MiB.
        gpu (GpuTypeAlias):
            The type or name of the GPU device to be used for GPU-accelerated tasks. If not
            applicable or no GPU required, leave it empty. Default is [GpuType.NoGPU](#gputype).
        image (Union[Image, dict]):
            The container image used for the task execution. Default is [Image](#image).
        volumes (Optional[List[Volume]]):
            A list of volumes to be mounted to the container. Default is None.
        secrets (Optional[List[str]):
            A list of secrets that are injected into the container as environment variables. Default is [].
        name (Optional[str]):
            A name for the container. Default is None.
        callback_url (Optional[str]):
            An optional URL to send a callback to when a task is completed, timed out, or cancelled.

    Example usage:
        ```
        from beta9 import Image, container


        def run_command():
            image = Image()
            cmd = container.Command(cpu=2, memory=512, image=image)
            exit_code = cmd.run((["python", "-c", "\"print('Hello, World!')\""]))
            print(exit_code)

        ```
    """

    def __init__(
        self,
        cpu: Union[int, float, str] = 1.0,
        memory: Union[int, str] = 128,
        gpu: GpuTypeAlias = GpuType.NoGPU,
        image: Image = Image(),
        volumes: Optional[List[Volume]] = None,
        secrets: Optional[List[str]] = None,
        callback_url: Optional[str] = None,
    ) -> None:
        super().__init__(
            cpu=cpu,
            memory=memory,
            gpu=gpu,
            image=image,
            volumes=volumes,
            secrets=secrets,
            callback_url=callback_url,
        )

        self.task_id = ""
        self._container_stub: Optional[ContainerServiceStub] = None
        self.syncer: FileSyncer = FileSyncer(self.gateway_stub)

    @property
    def stub(self) -> ContainerServiceStub:
        if not self._container_stub:
            self._container_stub = ContainerServiceStub(self.channel)
        return self._container_stub

    @stub.setter
    def stub(self, value: ContainerServiceStub) -> None:
        self._container_stub = value

    @with_grpc_error_handling
    def run(self, command: List[str]) -> int:
        """Run a command in a remote container and return the exit code"""
        if not self.prepare_runtime(
            stub_type=CONTAINER_STUB_TYPE,
        ):
            return 1

        with terminal.progress("Working..."):
            return self._run_remote(command)

    def _run_remote(self, command: List[str]) -> int:
        terminal.header("Running command")
        last_response = CommandExecutionResponse(done=False)

        for r in self.stub.execute_command(
            CommandExecutionRequest(stub_id=self.stub_id, command=" ".join(command).encode())
        ):
            if r.task_id != "":
                self.task_id = r.task_id

            if r.output != "":
                terminal.detail(r.output.strip())

            if r.done or r.exit_code != 0:
                last_response = r
                break

        if not last_response.done or last_response.exit_code != 0:
            terminal.warn(f"Command execution failed with exit code {last_response.exit_code} ❌")
            return last_response.exit_code

        terminal.header("Command execution complete 🎉")
        return last_response.exit_code


class App(RunnerAbstraction):
    """
    App allows you to deploy arbitrary containers.

    Parameters:
        cpu (Union[int, float, str]):
            The number of CPU cores allocated to the container. Default is 1.0.
        memory (Union[int, str]):
            The amount of memory allocated to the container. It should be specified in
            MiB, or as a string with units (e.g. "1Gi"). Default is 128 MiB.
        gpu (GpuTypeAlias):
            The type or name of the GPU device to be used for GPU-accelerated tasks. If not
            applicable or no GPU required, leave it empty. Default is [GpuType.NoGPU](#gputype).
        image (Union[Image, dict]):
            The container image used for the task execution. Default is [Image](#image).
        volumes (Optional[List[Volume]]):
            A list of volumes to be mounted to the container. Default is None.
        secrets (Optional[List[str]):
            A list of secrets that are injected into the container as environment variables. Default is [].
        name (Optional[str]):
            A name for the container. Default is None.
    Example usage:
        ```
        from beta9 import Image, container

        ```
    """

    def __init__(
        self,
        cpu: Union[int, float, str] = 1.0,
        memory: Union[int, str] = 128,
        gpu: GpuTypeAlias = GpuType.NoGPU,
        image: Image = Image(),
        volumes: Optional[List[Volume]] = None,
        secrets: Optional[List[str]] = None,
    ) -> None:
        super().__init__(
            cpu=cpu,
            memory=memory,
            gpu=gpu,
            image=image,
            volumes=volumes,
            secrets=secrets,
        )

        self.task_id = ""
        self._container_stub: Optional[ContainerServiceStub] = None
        self.syncer: FileSyncer = FileSyncer(self.gateway_stub)

    @property
    def stub(self) -> ContainerServiceStub:
        if not self._container_stub:
            self._container_stub = ContainerServiceStub(self.channel)
        return self._container_stub

    @stub.setter
    def stub(self, value: ContainerServiceStub) -> None:
        self._container_stub = value

    def __call__(self, func):
        return _CallableWrapper(func, self)


class _CallableWrapper(DeployableMixin):
    deployment_stub_type = APP_DEPLOYMENT_STUB_TYPE
    base_stub_type = APP_STUB_TYPE

    def __init__(self, func: Callable, parent: Union[App]):
        self.func: Callable = func
        self.parent: Union[App] = parent

    def __call__(self, *args, **kwargs) -> Any:
        if not is_local():
            return self.local(*args, **kwargs)

        raise NotImplementedError("Direct calls to Apps are not supported.")

    def local(self, *args, **kwargs) -> Any:
        return self.func(*args, **kwargs)

    @with_grpc_error_handling
    def serve(self, timeout: int = 0):
        stub_type = APP_SERVE_STUB_TYPE
        if not self.parent.prepare_runtime(
            func=self.func, stub_type=stub_type, force_create_stub=True
        ):
            return False

        try:
            with terminal.progress("Serving app..."):
                base_url = self.parent.settings.api_host
                if not base_url.startswith(("http://", "https://")):
                    base_url = f"http://{base_url}"

                invocation_url = f"{base_url}/{self.base_stub_type}/id/{self.parent.stub_id}"
                self.parent.print_invocation_snippet(invocation_url=invocation_url)

                return self._serve(
                    dir=os.getcwd(), object_id=self.parent.object_id, timeout=timeout
                )

        except KeyboardInterrupt:
            self._handle_serve_interrupt()

    def _handle_serve_interrupt(self) -> None:
        terminal.header("Stopping serve container")
        self.parent._container_stub.stop_endpoint_serve(
            StopEndpointServeRequest(stub_id=self.parent.stub_id)
        )
        terminal.print("Goodbye 👋")
        os._exit(0)  # kills all threads immediately

    def _serve(self, *, dir: str, object_id: str, timeout: int = 0):
        def notify(*_, **__):
            self.parent.endpoint_stub.endpoint_serve_keep_alive(
                EndpointServeKeepAliveRequest(
                    stub_id=self.parent.stub_id,
                    timeout=timeout,
                )
            )

        threading.Thread(
            target=self.parent.sync_dir_to_workspace,
            kwargs={"dir": dir, "object_id": object_id, "on_event": notify},
            daemon=True,
        ).start()

        r: Optional[StartEndpointServeResponse] = None
        for r in self.parent.endpoint_stub.start_endpoint_serve(
            StartEndpointServeRequest(
                stub_id=self.parent.stub_id,
                timeout=timeout,
            )
        ):
            if r.output != "":
                terminal.detail(r.output, end="")

            if r.done or r.exit_code != 0:
                break

        if r is None or not r.done or r.exit_code != 0:
            terminal.error("Serve container failed ❌")

        terminal.warn("App serve timed out. Container has been stopped.")


class AppConfig:
    def entry_point(args: List[str]):
        return []

    def expose_port(port: int):
        return 8080
