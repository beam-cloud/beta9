import atexit
import io
import shlex
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Union

from .. import terminal
from ..abstractions.base import unset_channel
from ..abstractions.base.runner import (
    SANDBOX_STUB_TYPE,
    BaseAbstraction,
)
from ..abstractions.image import Image
from ..abstractions.pod import Pod
from ..abstractions.volume import CloudBucket, Volume
from ..clients.gateway import GatewayServiceStub, StopContainerRequest, StopContainerResponse
from ..clients.pod import (
    CreatePodRequest,
    CreatePodResponse,
    PodSandboxConnectRequest,
    PodSandboxConnectResponse,
    PodSandboxCreateDirectoryRequest,
    PodSandboxCreateImageFromFilesystemRequest,
    PodSandboxCreateImageFromFilesystemResponse,
    PodSandboxDeleteDirectoryRequest,
    PodSandboxDeleteFileRequest,
    PodSandboxDownloadFileRequest,
    PodSandboxExecRequest,
    PodSandboxExposePortRequest,
    PodSandboxExposePortResponse,
    PodSandboxFindInFilesRequest,
    PodSandboxKillRequest,
    PodSandboxListFilesRequest,
    PodSandboxListProcessesRequest,
    PodSandboxListProcessesResponse,
    PodSandboxListUrlsRequest,
    PodSandboxListUrlsResponse,
    PodSandboxReplaceInFilesRequest,
    PodSandboxSnapshotMemoryRequest,
    PodSandboxSnapshotMemoryResponse,
    PodSandboxStatFileRequest,
    PodSandboxStatusRequest,
    PodSandboxStderrRequest,
    PodSandboxStdoutRequest,
    PodSandboxUpdateTtlRequest,
    PodSandboxUpdateTtlResponse,
    PodSandboxUploadFileRequest,
    PodServiceStub,
)
from ..env import is_remote
from ..exceptions import SandboxConnectionError, SandboxFileSystemError, SandboxProcessError
from ..type import GpuType, GpuTypeAlias


class Sandbox(Pod):
    """
    A sandboxed container for running Python code or arbitrary processes.
    You can use this to create isolated environments where you can execute code,
    manage files, and run processes.

    Parameters:
        cpu (Union[int, float, str]):
            The number of CPU cores allocated to the container. Default is 1.0.
        memory (Union[int, str]):
            The amount of memory allocated to the container. It should be specified in
            MiB, or as a string with units (e.g. "1Gi"). Default is 128 MiB.
        gpu (Union[GpuType, str]):
            The type or name of the GPU device to be used for GPU-accelerated tasks. If not
            applicable or no GPU required, leave it empty. Default is [GpuType.NoGPU](#gputype).
        gpu_count (int):
            The number of GPUs to allocate. Default is 0.
        image (Union[Image, dict]):
            The container image used for the task execution. Whatever you pass here will have an additional `add_python_packages` call
            with `["fastapi", "vllm", "huggingface_hub"]` added to it to ensure that we can run vLLM in the container.
        keep_warm_seconds (int):
            The number of seconds to keep the sandbox around. Default is 10 minutes (600s). Use -1 for sandboxes that never timeout.
        authorized (bool):
            Whether the sandbox should be authorized for external access. Default is False.
        name (str):
            The name of the Sandbox app. Default is none, which means you must provide it during deployment.
        volumes (List[Union[Volume, CloudBucket]]):
            The volumes and/or cloud buckets to mount into the Sandbox container. Default is an empty list.
        secrets (List[str]):
            The secrets to pass to the Sandbox container.
        env (Optional[Dict[str, str]]):
            A dictionary of environment variables to be injected into each Sandbox container. Default is {}.
        sync_local_dir (bool):
            Whether to sync the local directory to the sandbox filesystem on creation. Default is False.
        docker_enabled (bool):
            Enable Docker-in-Docker support inside the sandbox. When enabled with gVisor runtime,
            grants elevated capabilities required to run Docker and automatically starts the Docker daemon.

            IMPORTANT: You must install Docker in your image first using `Image().with_docker()`.

            Only works with gVisor runtime. Default is False.

            SECURITY NOTE: This grants significant capabilities within your sandbox.

            Example:
                ```python
                from beta9 import Image, Sandbox

                # Install Docker in the image
                image = Image(python_version="python3.11").with_docker()

                # Enable Docker in the sandbox
                sandbox = Sandbox(image=image, docker_enabled=True)
                instance = sandbox.create()

                # Docker daemon will be automatically started
                instance.docker.run("hello-world")
                ```

    Example:
        ```python
        from beta9 import Sandbox

        # Create a sandbox with GPU support
        sandbox = Sandbox(
            cpu=2.0,
            memory="2Gi",
            keep_warm_seconds=1800  # 30 minutes
        )

        # Create and connect to the sandbox
        instance = sandbox.create()

        # Run some code
        response = instance.process.run_code("print('Hello from sandbox!')")
        print(response.result)
        print(response.exit_code)

        # Clean up
        instance.terminate()
        ```
    """

    def __init__(
        self,
        cpu: Union[int, float, str] = 1.0,
        memory: Union[int, str] = 128,
        gpu: Union[GpuTypeAlias, List[GpuTypeAlias]] = GpuType.NoGPU,
        gpu_count: int = 0,
        image: Image = Image(python_version="python3.11"),
        keep_warm_seconds: int = 600,
        authorized: bool = False,
        name: Optional[str] = None,
        volumes: Optional[List[Union[Volume, CloudBucket]]] = [],
        secrets: Optional[List[str]] = None,
        env: Optional[Dict[str, str]] = {},
        sync_local_dir: bool = False,
        docker_enabled: bool = False,
    ):
        self.debug_buffer = io.StringIO()
        self.sync_local_dir = sync_local_dir

        super().__init__(
            cpu=cpu,
            memory=memory,
            gpu=gpu,
            gpu_count=gpu_count,
            image=image,
            keep_warm_seconds=keep_warm_seconds,
            authorized=authorized,
            name=name,
            volumes=volumes,
            secrets=secrets,
            env=env,
            docker_enabled=docker_enabled,
        )

    def debug(self):
        """
        Print the debug buffer contents to stdout.

        This method outputs any debug information that has been collected
        during sandbox operations.
        """
        print(self.debug_buffer.getvalue())

    def connect(self, id: str) -> "SandboxInstance":
        """
        Connect to an existing sandbox instance by ID.

        Parameters:
            id (str): The container ID of the existing sandbox instance.

        Returns:
            SandboxInstance: A connected sandbox instance.

        Raises:
            SandboxConnectionError: If the connection fails.

        Example:
            ```python
            # Connect to an existing sandbox
            instance = sandbox.connect("sandbox-123")
            ```
        """
        response: "PodSandboxConnectResponse" = self.stub.sandbox_connect(
            PodSandboxConnectRequest(
                container_id=id,
            )
        )

        if not response.ok:
            raise SandboxConnectionError(response.error_msg)

        return SandboxInstance(
            container_id=id,
            ok=True,
            error_msg="",
            stub_id=response.stub_id,
        )

    def create_from_memory_snapshot(self, snapshot_id: str) -> "SandboxInstance":
        """
        Create a sandbox instance from a filesystem snapshot.
        This will create a new sandbox instance with any filesystem-level changes made in that original sandbox instance.
        However, it will not restore any running processes or state present in the original sandbox instance.

        Parameters:
            snapshot_id (str): The ID of the snapshot to create the sandbox from.

        Returns:
            SandboxInstance: A new sandbox instance ready for use.

        Example:
            ```python
            # Create a sandbox instance from a memory snapshot
            instance = sandbox.create_from_memory_snapshot("snapshot-123")
            print(f"Sandbox created with ID: {instance.sandbox_id()}")
            ```
        """

        terminal.header(f"Creating sandbox from memory snapshot: {snapshot_id}")

        create_response: CreatePodResponse = self.stub.create_pod(
            CreatePodRequest(
                checkpoint_id=snapshot_id,
            )
        )

        if not create_response.ok:
            return SandboxInstance(
                container_id="",
                ok=False,
                error_msg=create_response.error_msg,
                stub_id="",
            )

        self.stub_id = create_response.stub_id

        terminal.header(f"Sandbox created successfully ===> {create_response.container_id}")

        if self.keep_warm_seconds < 0:
            terminal.header(
                "This sandbox has no timeout, it will run until it is shut down manually."
            )
        else:
            terminal.header(f"This sandbox will timeout after {self.keep_warm_seconds} seconds.")

        return SandboxInstance(
            stub_id=self.stub_id,
            container_id=create_response.container_id,
            ok=create_response.ok,
            error_msg=create_response.error_msg,
        )

    def create(self) -> "SandboxInstance":
        """
        Create a new sandbox instance.

        This method creates a new containerized sandbox environment with the
        specified configuration.

        Returns:
            SandboxInstance: A new sandbox instance ready for use.

        Example:
            ```python
            # Create a new sandbox
            instance = sandbox.create()
            print(f"Sandbox created with ID: {instance.sandbox_id()}")
            ```
        """

        self.entrypoint = ["tail", "-f", "/dev/null"]

        if not self.prepare_runtime(
            stub_type=SANDBOX_STUB_TYPE,
            force_create_stub=True,
            ignore_patterns=["*"] if not self.sync_local_dir else None,
        ):
            return SandboxInstance(
                container_id="",
                ok=False,
                error_msg="Failed to prepare runtime",
                stub_id="",
            )

        terminal.header("Creating sandbox")

        create_response: CreatePodResponse = self.stub.create_pod(
            CreatePodRequest(
                stub_id=self.stub_id,
            )
        )

        if not create_response.ok:
            return SandboxInstance(
                stub_id=self.stub_id,
                container_id="",
                ok=False,
                error_msg=create_response.error_msg,
            )

        terminal.header(f"Sandbox created successfully ===> {create_response.container_id}")

        if self.keep_warm_seconds < 0:
            terminal.header(
                "This sandbox has no timeout, it will run until it is shut down manually."
            )
        else:
            terminal.header(f"This sandbox will timeout after {self.keep_warm_seconds} seconds.")

        return SandboxInstance(
            stub_id=self.stub_id,
            container_id=create_response.container_id,
            ok=create_response.ok,
            error_msg=create_response.error_msg,
        )


@dataclass
class SandboxInstance(BaseAbstraction):
    """
    A sandbox instance that provides access to the sandbox internals.

    This class represents an active sandboxed container and provides methods for
    process management, file system operations, Docker management, preview URLs,
    and lifecycle management.

    Attributes:
        container_id (str): The unique ID of the created sandbox container.
        fs (SandboxFileSystem): File system interface for the sandbox.
        process (SandboxProcessManager): Process management interface for the sandbox.
        docker (SandboxDockerManager): Docker management interface for the sandbox.

    Example:
        ```python
        # Create a sandbox instance
        instance = sandbox.create()

        # Access file system
        instance.fs.upload_file("local_file.txt", "/remote_file.txt")

        # Run processes
        result = instance.process.run_code("import os; print(os.getcwd())")

        # Use Docker (requires docker_enabled=True)
        sandbox_with_docker = Sandbox(docker_enabled=True)
        instance = sandbox_with_docker.create()
        instance.docker.run("nginx:latest", detach=True, ports={"80": "8080"})

        # Expose a port
        url = instance.expose_port(8000)

        # Clean up
        instance.terminate()
        ```
    """

    container_id: str
    stub_id: str
    ok: bool = field(default=False)
    error_msg: str = field(default="")
    gateway_stub: "GatewayServiceStub" = field(init=False)
    stub: "PodServiceStub" = field(init=False)

    def __post_init__(self):
        super().__init__()
        self.gateway_stub = GatewayServiceStub(self.channel)
        self.stub = PodServiceStub(self.channel)
        self.fs = SandboxFileSystem(self)
        self.process = SandboxProcessManager(self)
        self.docker = SandboxDockerManager(self)
        self.terminated = False
        atexit.register(self._cleanup)

    def _cleanup(self):
        try:
            if hasattr(self, "container_id") and self.container_id and not self.terminated:
                if not is_remote():
                    terminal.warn(
                        f'WARNING: {self.container_id} is still running, to terminate use Sandbox().connect("{self.container_id}").terminate()'
                    )
        except BaseException as e:
            terminal.warn(f"Error during sandbox cleanup: {e}")

    def terminate(self) -> bool:
        """
        Terminate the container associated with this sandbox instance.

        This method stops the sandbox container and frees up associated resources.
        Once terminated, the sandbox instance cannot be used for further operations.

        Returns:
            bool: True if the container was terminated successfully, False otherwise.

        Example:
            ```python
            # Terminate the sandbox
            success = instance.terminate()
            if success:
                print("Sandbox terminated successfully")
            ```
        """
        res: "StopContainerResponse" = self.gateway_stub.stop_container(
            StopContainerRequest(container_id=self.container_id)
        )

        if res.ok:
            self.terminated = True

        return res.ok

    def create_image_from_filesystem(self) -> str:
        """
        Save the current sandbox filesystem state and create an image from it.

        Returns:
            str: The image ID.

        Example:
            ```python
            # Create an image from the sandbox filesystem contents
            image_id = instance.create_image_from_filesystem()
            print(f"Image created with ID: {image_id}")
            ```
        """
        terminal.header(f"Creating an image from sandbox filesystem: {self.container_id}")

        res: "PodSandboxCreateImageFromFilesystemResponse" = (
            self.stub.sandbox_create_image_from_filesystem(
                PodSandboxCreateImageFromFilesystemRequest(
                    stub_id=self.stub_id, container_id=self.container_id
                )
            )
        )

        if not res.ok:
            raise SandboxProcessError(res.error_msg)

        return res.image_id

    def snapshot_memory(self) -> str:
        """
        Create a memory snapshot of the sandbox (including all running processes and GPU state).

        Returns:
            str: The checkpoint ID.

        Example:
            ```python
            # Create a snapshot of the sandbox memory contents
            checkpoint_id = instance.snapshot_memory()
            print(f"Checkpoint created with ID: {checkpoint_id}")
            ```
        """
        terminal.header(f"Creating a memory snapshot of sandbox: {self.container_id}")

        res: "PodSandboxSnapshotMemoryResponse" = self.stub.sandbox_snapshot_memory(
            PodSandboxSnapshotMemoryRequest(stub_id=self.stub_id, container_id=self.container_id)
        )

        if not res.ok:
            raise SandboxProcessError(res.error_msg)

        return res.checkpoint_id

    def sandbox_id(self) -> str:
        """
        Get the ID of the sandbox.

        Returns:
            str: The container ID of the sandbox.

        Example:
            ```python
            sandbox_id = instance.sandbox_id()
            print(f"Working with sandbox: {sandbox_id}")
            ```
        """
        return self.container_id

    def update_ttl(self, ttl: int):
        """
        Update the keep warm setting of the sandbox.

        This method allows you to change how long the sandbox will remain active
        before automatically shutting down.

        Parameters:
            ttl (int): The number of seconds to keep the sandbox alive.
                      Use -1 for sandboxes that never timeout.

        Raises:
            SandboxProcessError: If the TTL update fails.

        Example:
            ```python
            # Keep the sandbox alive for 1 hour
            instance.update_ttl(3600)

            # Make the sandbox never timeout
            instance.update_ttl(-1)
            ```
        """
        res: "PodSandboxUpdateTtlResponse" = self.stub.sandbox_update_ttl(
            PodSandboxUpdateTtlRequest(container_id=self.container_id, ttl=ttl)
        )

        if not res.ok:
            raise SandboxProcessError(res.error_msg)

    def expose_port(self, port: int) -> str:
        """
        Dynamically expose a port to the internet.

        This method creates a public URL that allows external access to a specific
        port within the sandbox. The URL is SSL-terminated and provides secure
        access to services running in the sandbox.

        Parameters:
            port (int): The port number to expose within the sandbox.

        Returns:
            str: The public URL for accessing the exposed port.

        Raises:
            SandboxProcessError: If port exposure fails.

        Example:
            ```python
            # Expose port 8000 for a web service
            url = instance.expose_port(8000)
            print(f"Web service available at: {url}")
            ```
        """
        res: "PodSandboxExposePortResponse" = self.stub.sandbox_expose_port(
            PodSandboxExposePortRequest(
                container_id=self.container_id, stub_id=self.stub_id, port=port
            )
        )

        if res.ok:
            return res.url

        raise SandboxProcessError("Failed to expose port")

    def list_urls(self) -> Dict[int, str]:
        """
        List all exposed URLs in the sandbox    .

        Returns:
            Dict[int, str]: A dictionary of exposed URLs, organized by port.

        Raises:
            SandboxConnectionError: If listing URLs fails.

        Example:
            ```python
            # List all exposed URLs
            urls = instance.list_urls()
            print(f"Exposed URLs: {urls}")
            ```
        """
        res: "PodSandboxListUrlsResponse" = self.stub.sandbox_list_urls(
            PodSandboxListUrlsRequest(container_id=self.container_id)
        )
        if not res.ok:
            raise SandboxConnectionError(res.error_msg)

        return res.urls

    def __getstate__(self):
        state = self.__dict__.copy()

        # Remove non-picklable attributes
        state.pop("gateway_stub", None)
        state.pop("stub", None)
        state.pop("channel", None)

        unset_channel()
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        unset_channel()
        self.gateway_stub = GatewayServiceStub(self.channel)
        self.stub = PodServiceStub(self.channel)


class SandboxProcessResponse:
    """
    Response object containing the results of a completed process execution.

    This class encapsulates the output and status information from a process
    that has finished running in the sandbox.

    Attributes:
        pid (int): The process ID of the executed command.
        exit_code (int): The exit code of the process (0 typically indicates success).
        stdout (SandboxProcessStream): Stream containing the standard output.
        stderr (SandboxProcessStream): Stream containing the standard error output.
        result (str): Combined stdout and stderr output as a string.

    Example:
        ```python
        # Run a command and get the response
        response = instance.process.run_code("echo 'Hello World'")
        print(f"Exit code: {response.exit_code}")
        print(f"Output: {response.result}")
        ```
    """

    def __init__(
        self,
        pid: int,
        exit_code: int,
        stdout: "SandboxProcessStream",
        stderr: "SandboxProcessStream",
    ):
        self.pid = pid
        self.exit_code = exit_code
        self.result: str = stdout.read() + stderr.read()


class SandboxProcessManager:
    """
    Manager for executing and controlling processes within a sandbox.

    This class provides a high-level interface for running commands and Python
    code within the sandbox environment. It supports both blocking and non-blocking
    execution, environment variable configuration, and working directory specification.

    Attributes:
        sandbox_instance (SandboxInstance): The sandbox instance this manager operates on.
        processes (Dict[int, SandboxProcess]): Dictionary mapping PIDs to active processes.

    Example:
        ```python
        # Get the process manager
        pm = instance.process

        # Run Python code
        result = pm.run_code("import sys; print(sys.version)")

        # Run a shell command
        process = pm.exec("ls", "-la")
        process.wait()

        # List running processes
        active_processes = pm.list_processes()
        ```
    """

    def __init__(self, sandbox_instance: SandboxInstance) -> "SandboxProcess":
        self.sandbox_instance: SandboxInstance = sandbox_instance
        self.processes: Dict[int, SandboxProcess] = {}

    def run_code(
        self,
        code: str,
        blocking: bool = True,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> Union["SandboxProcessResponse", "SandboxProcess"]:
        """
        Run Python code in the sandbox.

        This method executes Python code within the sandbox environment. The code
        is executed using the Python interpreter available in the sandbox.

        Parameters:
            code (str): The Python code to execute.
            blocking (bool): Whether to wait for the process to complete.
                           If True, returns SandboxProcessResponse. If False, returns SandboxProcess.
            cwd (Optional[str]): The working directory to run the code in. Default is None.
            env (Optional[Dict[str, str]]): Environment variables to set for the process. Default is None.

        Returns:
            Union[SandboxProcessResponse, SandboxProcess]:
                - SandboxProcessResponse if blocking=True (process completed)
                - SandboxProcess if blocking=False (process still running)

        Example:
            ```python
            # Run blocking Python code
            result = pm.run_code("print('Hello from sandbox!')")
            print(result.result)

            # Run non-blocking Python code
            process = pm.run_code("import time; time.sleep(10)", blocking=False)
            # Do other work while process runs
            process.wait()
            ```
        """
        process = self._exec("python3", "-c", code, cwd=cwd, env=env)

        if blocking:
            process.wait()

            return SandboxProcessResponse(
                pid=process.pid,
                exit_code=process.exit_code,
                stdout=process.stdout,
                stderr=process.stderr,
            )

        return process

    def exec(
        self,
        *args,
        cwd: Optional[str] = "/workspace",
        env: Optional[Dict[str, str]] = None,
    ) -> "SandboxProcess":
        """
        Run an arbitrary command in the sandbox.

        This method executes shell commands within the sandbox environment.
        The command is executed using the shell available in the sandbox.

        Parameters:
            *args: The command and its arguments to execute.
            cwd (Optional[str]): The working directory to run the command in. Default is None.
            env (Optional[Dict[str, str]]): Environment variables to set for the command. Default is None.

        Returns:
            SandboxProcess: A process object that can be used to interact with the running command.

        Example:
            ```python
            # Run a simple command
            process = pm.exec("ls", "-la")
            process.wait()

            # Run with custom environment
            process = pm.exec("echo", "$CUSTOM_VAR", env={"CUSTOM_VAR": "hello"})

            # Run in specific directory
            process = pm.exec("pwd", cwd="/tmp")
            ```
        """
        return self._exec(*args, cwd=cwd, env=env)

    def _exec(
        self,
        *args,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> "SandboxProcess":
        """
        Internal method to execute commands in the sandbox.

        Parameters:
            *args: The command and its arguments.
            cwd (Optional[str]): Working directory.
            env (Optional[Dict[str, str]]): Environment variables.

        Returns:
            SandboxProcess: The created process object.

        Raises:
            SandboxProcessError: If process creation fails.
        """
        command = list(args) if not isinstance(args[0], list) else args[0]
        shell_command = " ".join(shlex.quote(arg) for arg in command)

        response = self.sandbox_instance.stub.sandbox_exec(
            PodSandboxExecRequest(
                container_id=self.sandbox_instance.container_id,
                command=shell_command,
                cwd=cwd,
                env=env,
            )
        )
        if not response.ok or response.pid <= 0:
            raise SandboxProcessError(response.error_msg)

        if response.pid > 0:
            process = SandboxProcess(
                self.sandbox_instance,
                pid=response.pid,
                cwd=cwd,
                args=command,
                env=env,
            )
            self.processes[response.pid] = process
            return process

    def list_processes(self) -> Dict[int, "SandboxProcess"]:
        """
        List all processes running in the sandbox.

        Returns:
            Dict[int, SandboxProcess]: Dictionary of active process objects, indexed by PID.

        Example:
            ```python
            processes = pm.list_processes()
            for pid, process in processes.items():
                print(f"Process {process.pid} is running")
            ```
        """
        processes: PodSandboxListProcessesResponse = (
            self.sandbox_instance.stub.sandbox_list_processes(
                PodSandboxListProcessesRequest(container_id=self.sandbox_instance.container_id)
            )
        )
        if not processes.ok:
            raise SandboxProcessError(processes.error_msg)

        return {
            process.pid: SandboxProcess(
                self.sandbox_instance,
                pid=process.pid,
                cwd=process.cwd,
                args=process.cmd.split(" "),
                env=process.env,
                exit_code=process.exit_code,
            )
            for process in processes.processes
        }

    def get_process(self, pid: int) -> "SandboxProcess":
        """
        Get a process by its PID.

        Parameters:
            pid (int): The process ID to look up.

        Returns:
            SandboxProcess: The process object for the given PID.

        Raises:
            SandboxProcessError: If the process is not found.

        Example:
            ```python
            try:
                process = pm.get_process(12345)
                print(f"Found process: {process.pid}")
            except SandboxProcessError:
                print("Process not found")
            ```
        """
        self.processes = self.list_processes()

        if pid not in self.processes.keys():
            raise SandboxProcessError(f"Process with pid {pid} not found")

        return self.processes[pid]


class SandboxProcessStream:
    """
    A stream-like interface for reading process output in real-time.

    This class provides an iterator interface for reading stdout or stderr
    from a running process. It buffers output and provides both line-by-line
    iteration and bulk reading capabilities.

    Attributes:
        process (SandboxProcess): The process this stream belongs to.
        fetch_fn: Function to fetch new output chunks.
        _buffer (str): Internal buffer for incomplete lines.
        _closed (bool): Whether the stream has been closed.
        _last_output (str): Last known output for change detection.

    Example:
        ```python
        # Get a process stream
        process = pm.exec("echo", "Hello\nWorld")

        # Read line by line
        for line in process.stdout:
            print(f"Output: {line.strip()}")

        # Read all output at once
        all_output = process.stdout.read()
        ```
    """

    def __init__(self, process: "SandboxProcess", fetch_fn):
        self.process = process
        self.fetch_fn = fetch_fn
        self._buffer = ""
        self._closed = False
        self._last_output = ""

    def __iter__(self):
        """
        Return an iterator for reading the stream line by line.

        Returns:
            self: The stream object as an iterator.
        """
        return self

    def __next__(self):
        """
        Get the next line from the stream.

        Returns:
            str: The next line from the stream.

        Raises:
            StopIteration: When the stream is exhausted.
        """
        while True:
            if "\n" in self._buffer:
                line, self._buffer = self._buffer.split("\n", 1)
                return line + "\n"

            if self._closed:
                if self._buffer:
                    line, self._buffer = self._buffer, ""
                    return line
                raise StopIteration

            chunk = self._fetch_next_chunk()
            if chunk:
                self._buffer += chunk
            else:
                exit_code, _ = self.process.status()
                if exit_code >= 0:
                    last_chunk = self._fetch_next_chunk()
                    if last_chunk:
                        self._buffer += last_chunk
                        continue

                    self._closed = True
                else:
                    time.sleep(0.1)

    def _fetch_next_chunk(self):
        """
        Fetch the next chunk of output from the process.

        Returns:
            str: New output chunk, or empty string if no new output.
        """
        output = self.fetch_fn()

        if output == self._last_output:
            return ""

        new_output = output[len(self._last_output) :]
        self._last_output = output
        return new_output

    def read(self):
        """
        Return whatever output is currently available in the stream.
        """
        data = self._buffer
        self._buffer = ""

        while True:
            chunk = self._fetch_next_chunk()
            if chunk:
                data += chunk
            else:
                break

        return data


class SandboxProcess:
    """
    Represents a running process within a sandbox.

    This class provides control and monitoring capabilities for processes
    running in the sandbox. It allows you to wait for completion, kill
    processes, check status, and access output streams.

    Attributes:
        sandbox_instance (SandboxInstance): The sandbox this process runs in.
        pid (int): The process ID.
        exit_code (int): The exit code of the process (-1 if still running).
        _status (str): Internal status tracking.

    Example:
        ```python
        # Start a process
        process = pm.exec("sleep", "10")

        # Check if it's still running
        exit_code, status = process.status()
        if exit_code < 0:
            print("Process is still running")

        # Wait for completion
        exit_code = process.wait()
        print(f"Process finished with exit code: {exit_code}")

        # Access output streams
        stdout = process.stdout.read()
        stderr = process.stderr.read()
        ```
    """

    def __init__(
        self,
        sandbox_instance: SandboxInstance,
        *,
        pid: int,
        cwd: str,
        args: List[str],
        env: Dict[str, str],
        exit_code: int = -1,
    ):
        self.sandbox_instance = sandbox_instance
        self.pid = pid
        self.exit_code = exit_code
        self._status = ""
        self.cwd = cwd
        self.args = args
        self.env = env

    def wait(self) -> int:
        """
        Wait for the process to complete.

        This method blocks until the process finishes execution and returns
        the exit code. It polls the process status until completion.

        Returns:
            int: The exit code of the completed process.

        Example:
            ```python
            process = pm.exec("long_running_command")
            exit_code = process.wait()
            if exit_code == 0:
                print("Command completed successfully")
            ```
        """
        self.exit_code, self._status = self.status()

        while self.exit_code < 0:
            self.exit_code, self._status = self.status()
            time.sleep(0.1)

        return self.exit_code

    def kill(self):
        """
        Kill the process.

        This method forcefully terminates the running process. Use this
        when you need to stop a process that is not responding or when
        you want to cancel a long-running operation.

        Raises:
            SandboxProcessError: If the kill operation fails.

        Example:
            ```python
            process = pm.exec("sleep", "100")

            # Kill the process after 5 seconds
            import time
            time.sleep(5)
            process.kill()
            ```
        """
        response = self.sandbox_instance.stub.sandbox_kill(
            PodSandboxKillRequest(container_id=self.sandbox_instance.container_id, pid=self.pid)
        )
        if not response.ok:
            raise SandboxProcessError(response.error_msg)

    def status(self) -> Tuple[int, str]:
        """
        Get the status of the process.

        This method returns the current exit code and status string of the process.
        An exit code of -1 indicates the process is still running.

        Returns:
            Tuple[int, str]: A tuple containing (exit_code, status_string).

        Raises:
            SandboxProcessError: If status retrieval fails.

        Example:
            ```python
            process = pm.exec("sleep", "5")

            # Check status periodically
            while True:
                exit_code, status = process.status()
                if exit_code >= 0:
                    print(f"Process finished with exit code: {exit_code}")
                    break
                time.sleep(1)
            ```
        """
        response = self.sandbox_instance.stub.sandbox_status(
            PodSandboxStatusRequest(container_id=self.sandbox_instance.container_id, pid=self.pid)
        )

        if not response.ok:
            raise SandboxProcessError(response.error_msg)

        return response.exit_code, response.status

    @property
    def stdout(self):
        """
        Get a handle to a stream of the process's stdout.

        Returns:
            SandboxProcessStream: A stream object for reading stdout.

        Example:
            ```python
            process = pm.exec("echo", "Hello World")
            stdout_content = process.stdout.read()
            print(f"STDOUT: {stdout_content}")
            ```
        """
        return SandboxProcessStream(
            self,
            lambda: self.sandbox_instance.stub.sandbox_stdout(
                PodSandboxStdoutRequest(
                    container_id=self.sandbox_instance.container_id, pid=self.pid
                )
            ).stdout,
        )

    @property
    def stderr(self):
        """
        Get a handle to a stream of the process's stderr.

        Returns:
            SandboxProcessStream: A stream object for reading stderr.

        Example:
            ```python
            process = pm.exec("python3", "-c", "import sys; print('Error', file=sys.stderr)")
            stderr_content = process.stderr.read()
            print(f"STDERR: {stderr_content}")
            ```
        """
        return SandboxProcessStream(
            self,
            lambda: self.sandbox_instance.stub.sandbox_stderr(
                PodSandboxStderrRequest(
                    container_id=self.sandbox_instance.container_id, pid=self.pid
                )
            ).stderr,
        )

    @property
    def logs(self):
        """
        Returns a combined stream of both stdout and stderr.

        This is a convenience property that combines both output streams.
        The streams are read concurrently, so if one stream is empty, it won't block
        the other stream from being read.

        Returns:
            CombinedStream: A stream object that combines stdout and stderr.

        Example:
            ```python
            process = pm.exec("python3", "-c", "import sys; print('stdout'); print('stderr', file=sys.stderr)")

            # Read combined output
            for line in process.logs:
                print(f"LOG: {line.strip()}")

            # Or read all at once
            all_logs = process.logs.read()
            ```
        """

        class CombinedStream:
            def __init__(self, process: "SandboxProcess"):
                self.process = process
                self._stdout = process.stdout
                self._stderr = process.stderr
                self._queue = []
                self._streams = {
                    "stdout": {"stream": self._stdout, "buffer": "", "exhausted": False},
                    "stderr": {"stream": self._stderr, "buffer": "", "exhausted": False},
                }

            def _process_stream(self, stream_name):
                """Process a single stream, adding any complete lines to the queue."""
                stream_info = self._streams[stream_name]
                if stream_info["exhausted"]:
                    return

                chunk = stream_info["stream"]._fetch_next_chunk()
                if chunk:
                    stream_info["buffer"] += chunk

                    while "\n" in stream_info["buffer"]:  # Process any complete lines
                        line, stream_info["buffer"] = stream_info["buffer"].split("\n", 1)
                        self._queue.append(line + "\n")

                else:
                    exit_code, _ = self.process.status()
                    if exit_code >= 0:  # Process has exited
                        if stream_info["buffer"]:
                            self._queue.append(stream_info["buffer"])
                            stream_info["buffer"] = ""
                            return

                        stream_info["exhausted"] = True

            def _fill_queue(self):
                self._process_stream("stdout")
                self._process_stream("stderr")

            def __iter__(self):
                return self

            def __next__(self):
                while True:
                    # If queue is empty, try to fill it
                    if not self._queue:
                        self._fill_queue()
                        # If still empty after trying to fill, we're done
                        if not self._queue and all(s["exhausted"] for s in self._streams.values()):
                            raise StopIteration

                        # If queue is still empty but streams aren't exhausted, wait and try again
                        if not self._queue:
                            try:
                                time.sleep(0.1)
                                continue
                            except KeyboardInterrupt:
                                raise

                    # Return the next line from the queue
                    return self._queue.pop(0)

            def read(self):
                stdout_data = self._stdout.read()
                stderr_data = self._stderr.read()
                return stdout_data + stderr_data

        return CombinedStream(self)

    def __getstate__(self):
        state = self.__dict__.copy()
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)


@dataclass
class SandboxFileInfo:
    """
    Metadata of a file in the sandbox.

    This class provides detailed information about files and directories
    within the sandbox filesystem, including permissions, ownership,
    and modification times.

    Attributes:
        name (str): The name of the file or directory.
        is_dir (bool): Whether this is a directory.
        size (int): The size of the file in bytes.
        mode (int): The file mode (permissions and type).
        mod_time (int): The modification time as a Unix timestamp.
        permissions (int): The file permissions as an integer.
        owner (str): The owner of the file.
        group (str): The group owner of the file.

    Example:
        ```python
        # Get file info
        file_info = instance.fs.stat_file("/path/to/file.txt")
        print(f"File: {file_info.name}")
        print(f"Size: {file_info.size} bytes")
        print(f"Owner: {file_info.owner}")
        print(f"Permissions: {oct(file_info.permissions)}")
        ```
    """

    name: str
    is_dir: bool
    size: int
    mode: int
    mod_time: int
    permissions: int
    owner: str
    group: str

    def __str__(self):
        """
        Return a string representation of the file info.

        Returns:
            str: A formatted string showing file information.
        """
        octal_perms = oct(self.permissions & 0o7777)
        return f"SandboxFileInfo(name='{self.name}', is_dir={self.is_dir}, size={self.size}, mode={self.mode}, mod_time={self.mod_time}, permissions={octal_perms}, owner='{self.owner}', group='{self.group}')"


@dataclass
class SandboxFilePosition:
    """
    A position in a file.

    Attributes:
        line (int): The line number.
        column (int): The column number.
    """

    line: int
    column: int


@dataclass
class SandboxFileSearchRange:
    """
    A range in a file.

    Attributes:
        start (SandboxFilePosition): The start position.
        end (SandboxFilePosition): The end position.
    """

    start: SandboxFilePosition
    end: SandboxFilePosition


@dataclass
class SandboxFileSearchMatch:
    """
    A match in a file.

    Attributes:
        range (SandboxFileSearchRange): The range of the match.
        content (str): The content of the match.
    """

    range: SandboxFileSearchRange
    content: str


@dataclass
class SandboxFileSearchResult:
    """
    A search result in a file.

    Attributes:
        path (str): The path to the file.
        matches (List[SandboxFileSearchMatch]): The matches in the file with the start and end position of the match.
    """

    path: str
    matches: List[SandboxFileSearchMatch]


class SandboxFileSystem:
    """
    File system interface for managing files within a sandbox.

    This class provides a comprehensive API for file operations within
    the sandbox, including uploading, downloading, listing, and managing
    files and directories.

    Attributes:
        sandbox_instance (SandboxInstance): The sandbox instance this filesystem operates on.

    Example:
        ```python
        # Get the filesystem interface
        fs = instance.fs

        # Upload a file
        fs.upload_file("local_file.txt", "/remote_file.txt")

        # List files in a directory
        files = fs.list_files("/")
        for file_info in files:
            print(f"{file_info.name}: {file_info.size} bytes")

        # Download a file
        fs.download_file("/remote_file.txt", "downloaded_file.txt")
        ```
    """

    def __init__(self, sandbox_instance: SandboxInstance):
        self.sandbox_instance = sandbox_instance

    def upload_file(self, local_path: str, sandbox_path: str):
        """
        Upload a local file to the sandbox.

        This method reads a file from the local filesystem and uploads
        it to the specified path within the sandbox.

        Parameters:
            local_path (str): The path to the local file to upload.
            sandbox_path (str): The destination path within the sandbox.

        Raises:
            SandboxFileSystemError: If the upload fails.
            FileNotFoundError: If the local file doesn't exist.

        Example:
            ```python
            # Upload a Python script
            fs.upload_file("my_script.py", "/workspace/script.py")

            # Upload to a subdirectory
            fs.upload_file("config.json", "/app/config/config.json")
            ```
        """

        with open(local_path, "rb") as f:
            content = f.read()

            response = self.sandbox_instance.stub.sandbox_upload_file(
                PodSandboxUploadFileRequest(
                    container_id=self.sandbox_instance.container_id,
                    container_path=sandbox_path,
                    data=content,
                    mode=644,
                )
            )

            if not response.ok:
                raise SandboxFileSystemError(response.error_msg)

    def download_file(self, sandbox_path: str, local_path: str):
        """
        Download a file from the sandbox to a local path.

        This method downloads a file from the sandbox filesystem and
        saves it to the specified local path.

        Parameters:
            sandbox_path (str): The path to the file within the sandbox.
            local_path (str): The destination path on the local filesystem.

        Raises:
            SandboxFileSystemError: If the download fails.

        Example:
            ```python
            # Download a log file
            fs.download_file("/var/log/app.log", "local_app.log")

            # Download to a specific directory
            fs.download_file("/output/result.txt", "./results/result.txt")
            ```
        """
        response = self.sandbox_instance.stub.sandbox_download_file(
            PodSandboxDownloadFileRequest(
                container_id=self.sandbox_instance.container_id,
                container_path=sandbox_path,
            )
        )

        if not response.ok:
            raise SandboxFileSystemError(response.error_msg)

        with open(local_path, "wb") as f:
            f.write(response.data)

    def stat_file(self, sandbox_path: str) -> "SandboxFileInfo":
        """
        Get the metadata of a file in the sandbox.

        This method retrieves detailed information about a file or directory
        within the sandbox, including size, permissions, ownership, and
        modification time.

        Parameters:
            sandbox_path (str): The path to the file within the sandbox.

        Returns:
            SandboxFileInfo: Detailed information about the file.

        Raises:
            SandboxFileSystemError: If the file doesn't exist or stat fails.

        Example:
            ```python
            # Get file information
            file_info = fs.stat_file("/path/to/file.txt")
            print(f"File size: {file_info.size} bytes")
            print(f"Is directory: {file_info.is_dir}")
            print(f"Modified: {file_info.mod_time}")
            ```
        """
        response = self.sandbox_instance.stub.sandbox_stat_file(
            PodSandboxStatFileRequest(
                container_id=self.sandbox_instance.container_id,
                container_path=sandbox_path,
            )
        )
        if not response.ok:
            raise SandboxFileSystemError(response.error_msg)

        return SandboxFileInfo(
            **{
                "name": response.file_info.name,
                "is_dir": response.file_info.is_dir,
                "size": response.file_info.size,
                "mode": response.file_info.mode,
                "mod_time": response.file_info.mod_time,
                "owner": response.file_info.owner,
                "group": response.file_info.group,
                "permissions": response.file_info.permissions,
            }
        )

    def list_files(self, sandbox_path: str) -> List["SandboxFileInfo"]:
        """
        List the files in a directory in the sandbox.

        This method returns information about all files and directories
        within the specified directory in the sandbox.

        Parameters:
            sandbox_path (str): The path to the directory within the sandbox.

        Returns:
            List[SandboxFileInfo]: List of file information objects.

        Raises:
            SandboxFileSystemError: If the directory doesn't exist or listing fails.

        Example:
            ```python
            # List files in the root directory
            files = fs.list_files("/")
            for file_info in files:
                if file_info.is_dir:
                    print(f"Directory: {file_info.name}")
                else:
                    print(f"File: {file_info.name} ({file_info.size} bytes)")

            # List files in a specific directory
            workspace_files = fs.list_files("/workspace")
            ```
        """
        response = self.sandbox_instance.stub.sandbox_list_files(
            PodSandboxListFilesRequest(
                container_id=self.sandbox_instance.container_id,
                container_path=sandbox_path,
            )
        )
        if not response.ok:
            raise SandboxFileSystemError(response.error_msg)

        file_infos = []
        for file in response.files:
            f = {
                "name": file.name,
                "is_dir": file.is_dir,
                "size": file.size,
                "mode": file.mode,
                "mod_time": file.mod_time,
                "owner": file.owner,
                "group": file.group,
                "permissions": file.permissions,
            }
            file_infos.append(SandboxFileInfo(**f))

        return file_infos

    def create_directory(self, sandbox_path: str):
        """
        Create a directory in the sandbox.

        Parameters:
            sandbox_path (str): The path where the directory should be created.

        Raises:
            SandboxFileSystemError: If the directory creation fails.
        """
        resp = self.sandbox_instance.stub.sandbox_create_directory(
            PodSandboxCreateDirectoryRequest(
                container_id=self.sandbox_instance.container_id,
                container_path=sandbox_path,
            )
        )
        if not resp.ok:
            raise SandboxFileSystemError(resp.error_msg)

    def delete_directory(self, sandbox_path: str):
        """
        Delete a directory in the sandbox.

        Parameters:
            sandbox_path (str): The path of the directory to delete.

        Raises:
            SandboxFileSystemError: If the directory deletion fails.
        """
        resp = self.sandbox_instance.stub.sandbox_delete_directory(
            PodSandboxDeleteDirectoryRequest(
                container_id=self.sandbox_instance.container_id,
                container_path=sandbox_path,
            )
        )
        if not resp.ok:
            raise SandboxFileSystemError(resp.error_msg)

    def delete_file(self, sandbox_path: str):
        r"""
        Delete a file in the sandbox.

        This method removes a file from the sandbox filesystem.

        Parameters:
            sandbox_path (str): The path to the file within the sandbox.

        Raises:
            SandboxFileSystemError: If the file doesn't exist or deletion fails.

        Example:
            ```python
            # Delete a temporary file
            fs.delete_file("/tmp/temp_file.txt")

            # Delete a log file
            fs.delete_file("/var/log/old_log.log")
            ```
        """
        response = self.sandbox_instance.stub.sandbox_delete_file(
            PodSandboxDeleteFileRequest(
                container_id=self.sandbox_instance.container_id,
                container_path=sandbox_path,
            )
        )

        if not response.ok:
            raise SandboxFileSystemError(response.error_msg)

    def replace_in_files(self, sandbox_path: str, old_string: str, new_string: str):
        r"""
        Replace a string in all files in a directory.

        This method performs a find-and-replace operation on all files
        within the specified directory, replacing occurrences of the
        old string with the new string.

        Parameters:
            sandbox_path (str): The directory path to search in.
            old_string (str): The string to find and replace.
            new_string (str): The string to replace with.

        Raises:
            SandboxFileSystemError: If the operation fails.

        Example:
            ```python
            # Replace a configuration value
            fs.replace_in_files("/config", "old_host", "new_host")

            # Update version numbers
            fs.replace_in_files("/app", "1.0.0", "1.1.0")
            ```
        """
        response = self.sandbox_instance.stub.sandbox_replace_in_files(
            PodSandboxReplaceInFilesRequest(
                container_id=self.sandbox_instance.container_id,
                container_path=sandbox_path,
                pattern=old_string,
                new_string=new_string,
            )
        )

        if not response.ok:
            raise SandboxFileSystemError(response.error_msg)

    def find_in_files(self, sandbox_path: str, pattern: str) -> List[SandboxFileSearchResult]:
        r"""
        Search file contents in the sandbox using a regular expression pattern.

        This method scans files under the given directory and returns matches
        for the provided regex pattern, including line/column ranges and the
        matched text for each file.

        Parameters:
            sandbox_path (str): The directory path to search under.
            pattern (str): A regular expression applied to file contents.

        Returns:
            List[SandboxFileSearchResult]: Per-file results with match ranges and text.

        Raises:
            SandboxFileSystemError: If the search fails.

        Example:
            ```python
            # Find TODO comments
            todos = fs.find_in_files("/workspace", r"\bTODO\b")

            # Find imports of requests
            imports = fs.find_in_files("/app", r"^import\s+requests")
            ```
        """
        response = self.sandbox_instance.stub.sandbox_find_in_files(
            PodSandboxFindInFilesRequest(
                container_id=self.sandbox_instance.container_id,
                container_path=sandbox_path,
                pattern=pattern,
            )
        )

        results = []
        for result in response.results:
            matches = []
            for match in result.matches:
                matches.append(
                    SandboxFileSearchMatch(
                        range=SandboxFileSearchRange(
                            start=SandboxFilePosition(
                                line=match.range.start.line,
                                column=match.range.start.column,
                            ),
                            end=SandboxFilePosition(
                                line=match.range.end.line,
                                column=match.range.end.column,
                            ),
                        ),
                        content=match.content,
                    )
                )
            results.append(SandboxFileSearchResult(path=result.path, matches=matches))

        if not response.ok:
            raise SandboxFileSystemError(response.error_msg)

        return results


class DockerResult:
    """
    Result object for Docker operations that provides access to both output and logs.

    Attributes:
        process: The underlying SandboxProcess for streaming logs
        success: Whether the operation succeeded
        output: The primary output (container ID, image ID, etc.)

    Example:
        ```python
        # Build with logs
        result = sandbox.docker.build("myapp:v1", context=".")
        for line in result.logs():
            print(line)  # Stream build output

        # Or just wait for completion
        result.wait()
        print(f"Build {'succeeded' if result.success else 'failed'}")

        # Run container and get ID
        result = sandbox.docker.run("nginx", detach=True)
        result.wait()
        print(f"Container ID: {result.output}")
        ```
    """

    def __init__(self, process: "SandboxProcess", extract_output: callable = None):
        self.process = process
        self._extract_output = extract_output
        self._waited = False
        self._output = None
        self._success = None

    def wait(self) -> bool:
        """Wait for operation to complete. Returns True if successful."""
        if not self._waited:
            exit_code = self.process.wait()
            self._success = exit_code == 0
            if self._success and self._extract_output:
                self._output = self._extract_output(self.process)
            self._waited = True
        return self._success

    def logs(self):
        """
        Stream logs line by line from both stdout and stderr.
        Docker commands often output to stderr (e.g., build progress).
        """
        import threading
        from queue import Empty, Queue

        q = Queue()

        def read_stream(stream, prefix=""):
            try:
                for line in stream:
                    q.put((prefix, line))
            except Exception:
                # Stream closed or error reading
                pass

        # Read both stdout and stderr concurrently
        stdout_thread = threading.Thread(target=read_stream, args=(self.process.stdout, ""))
        stderr_thread = threading.Thread(target=read_stream, args=(self.process.stderr, ""))

        stdout_thread.daemon = True
        stderr_thread.daemon = True

        stdout_thread.start()
        stderr_thread.start()

        # Yield lines as they arrive
        threads_alive = True
        while threads_alive or not q.empty():
            try:
                prefix, line = q.get(timeout=0.1)
                yield line
            except Empty:
                # Check if threads are still alive
                threads_alive = stdout_thread.is_alive() or stderr_thread.is_alive()
                continue

    @property
    def output(self) -> str:
        """Get the primary output (auto-waits if needed)."""
        if not self._waited:
            self.wait()
        return self._output or ""

    @property
    def success(self) -> bool:
        """Check if operation succeeded (auto-waits if needed)."""
        if not self._waited:
            self.wait()
        return self._success

    @property
    def stdout(self) -> str:
        """Get all stdout (auto-waits if needed)."""
        if not self._waited:
            self.wait()
        return self.process.stdout.read()

    @property
    def stderr(self) -> str:
        """Get all stderr (auto-waits if needed)."""
        if not self._waited:
            self.wait()
        return self.process.stderr.read()


class SandboxDockerManager:
    """
    Docker manager for sandbox operations with streaming log support.

    Most operations return DockerResult objects that provide:
    - Streaming logs via .logs()
    - Automatic waiting via .wait()
    - Access to output via .output property

    Example:
        ```python
        sandbox = Sandbox(
            docker_enabled=True,
            image=Image().with_docker()
        ).create()

        # Build with streaming logs
        result = sandbox.docker.build("myapp:v1", context=".")
        for line in result.logs():
            print(line)  # See build progress

        # Or just wait for completion
        result = sandbox.docker.build("myapp:v1", context=".")
        if result.wait():
            print("Build succeeded!")

        # Pull and see logs
        result = sandbox.docker.pull("nginx:latest")
        print(result.stdout)  # Auto-waits and returns output

        # Run container
        result = sandbox.docker.run("nginx", name="web", detach=True)
        container_id = result.output  # Auto-waits and returns container ID
        ```
    """

    def __init__(self, sandbox_instance: SandboxInstance, daemon_timeout: int = 30):
        self.sandbox_instance = sandbox_instance
        self._daemon_timeout = daemon_timeout
        self._daemon_ready = False
        self._daemon_check_attempted = False
        self._authenticated = False

    def _ensure_ready(self):
        """Ensure Docker daemon is ready. Only blocks when actually needed for docker commands."""
        if self._daemon_ready:
            return

        # Mark that we've attempted a check to avoid interfering with non-docker operations
        self._daemon_check_attempted = True

        start, backoff = time.time(), 0.5
        while time.time() - start < self._daemon_timeout:
            try:
                p = self.sandbox_instance.process.exec("docker", "info")
                if p.wait() == 0:
                    self._daemon_ready = True
                    # Attempt authentication if credentials are in env
                    self._auto_login()
                    return
            except Exception:
                # Ignore errors during daemon check - we'll retry
                pass
            time.sleep(backoff)
            backoff = min(backoff * 1.5, 2.0)

        from beta9.exceptions import DockerDaemonNotReadyError

        raise DockerDaemonNotReadyError(self._daemon_timeout)

    def _auto_login(self):
        """Automatically login to Docker if credentials are available in environment."""
        if self._authenticated:
            return

        try:
            # Check if DOCKER_USERNAME and DOCKER_PASSWORD are set
            proc = self.sandbox_instance.process.exec("printenv", "DOCKER_USERNAME")
            proc.wait()
            username = proc.stdout.read().strip()

            if username:
                proc = self.sandbox_instance.process.exec("printenv", "DOCKER_PASSWORD")
                proc.wait()
                password = proc.stdout.read().strip()

                if password:
                    # Perform docker login
                    self.login(username=username, password=password)
        except BaseException:
            # If auto-login fails, don't block - user can manually call login()
            pass

    def _exec(self, *cmd) -> "SandboxProcess":
        """Execute docker command."""
        self._ensure_ready()
        return self.sandbox_instance.process.exec(*cmd)

    def _run(self, *cmd) -> str:
        """Execute docker command and return output (for simple operations)."""
        from beta9.exceptions import DockerCommandError

        p = self._exec(*cmd)
        exit_code = p.wait()

        if exit_code != 0:
            stderr = p.stderr.read()
            raise DockerCommandError(" ".join(cmd), exit_code, stderr)

        return p.stdout.read().strip()

    def _result(self, *cmd, extract_output: callable = None) -> DockerResult:
        """Execute docker command and return a DockerResult (for operations with logs)."""
        process = self._exec(*cmd)
        return DockerResult(process, extract_output=extract_output)

    # === Container Operations ===

    def run(
        self,
        image: str,
        command: Optional[Union[str, List[str]]] = None,
        name: Optional[str] = None,
        detach: bool = False,
        remove: bool = False,
        ports: Optional[Dict[str, str]] = None,
        volumes: Optional[Dict[str, str]] = None,
        env: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> DockerResult:
        """
        Run a Docker container.

        Args:
            image: Docker image (e.g. "nginx:latest")
            command: Command to run in container
            name: Container name
            detach: Run in background
            remove: Auto-remove when stopped
            ports: Port mappings {"container_port": "host_port"}
            volumes: Volume mappings {"host_path": "container_path"}
            env: Environment variables

        Returns:
            DockerResult: Result with container ID in .output property

        Example:
            ```python
            # Run detached and get container ID
            result = sandbox.docker.run("nginx:latest", name="web", ports={"80": "8080"}, detach=True)
            container_id = result.output  # Auto-waits

            # Run and stream logs
            result = sandbox.docker.run("alpine", command=["echo", "hello"])
            for line in result.logs():
                print(line)

            # Run and check success
            result = sandbox.docker.run("alpine", command=["sh", "-c", "exit 1"])
            if not result.success:
                print("Container failed")
            ```
        """
        cmd = ["docker", "run"]
        
        # Force host networking for gVisor compatibility
        # Bridge networking is not supported in Docker-in-gVisor
        cmd.extend(["--network", "host"])
        
        if detach:
            cmd.append("-d")
        if remove:
            cmd.append("--rm")
        if name:
            cmd.extend(["--name", name])
        if ports:
            for cp, hp in ports.items():
                cmd.extend(["-p", f"{hp}:{cp}"])
        if volumes:
            for hp, cp in volumes.items():
                cmd.extend(["-v", f"{hp}:{cp}"])
        if env:
            for k, v in env.items():
                cmd.extend(["-e", f"{k}={v}"])

        cmd.append(image)

        if command:
            if isinstance(command, str):
                cmd.append(command)
            else:
                cmd.extend(command)

        # Extract container ID from output if detached
        def extract_container_id(process):
            return process.stdout.read().strip()

        return self._result(*cmd, extract_output=extract_container_id if detach else None)

    def ps(self, all: bool = False, quiet: bool = False) -> Union[List[str], str]:
        """
        List containers.

        Args:
            all: Show all containers (default: running only)
            quiet: Only return container IDs

        Returns:
            List[str]: Container IDs if quiet=True
            str: Formatted table if quiet=False
        """
        cmd = ["docker", "ps"]
        if all:
            cmd.append("-a")
        if quiet:
            cmd.append("-q")

        output = self._run(*cmd)
        return output.split("\n") if quiet and output else output

    def stop(self, container: str) -> bool:
        """Stop a container. Returns True on success, False if container not found or already stopped."""
        from beta9.exceptions import DockerCommandError

        try:
            self._run("docker", "stop", container)
            return True
        except DockerCommandError:
            return False

    def rm(self, container: str, force: bool = False) -> bool:
        """Remove a container. Returns True on success, False if container not found."""
        from beta9.exceptions import DockerCommandError

        try:
            cmd = ["docker", "rm"]
            if force:
                cmd.append("-f")
            cmd.append(container)
            self._run(*cmd)
            return True
        except DockerCommandError:
            return False

    def logs(
        self, container: str, follow: bool = False, tail: Optional[int] = None
    ) -> "SandboxProcess":
        """Get container logs."""
        cmd = ["docker", "logs"]
        if follow:
            cmd.append("-f")
        if tail is not None:
            cmd.extend(["--tail", str(tail)])
        cmd.append(container)
        return self._exec(*cmd)

    def exec(self, container: str, command: Union[str, List[str]], **kwargs) -> "SandboxProcess":
        """Execute command in running container."""
        cmd = ["docker", "exec", container]
        if isinstance(command, str):
            cmd.append(command)
        else:
            cmd.extend(command)
        return self._exec(*cmd)

    # === Image Operations ===

    def pull(self, image: str, quiet: bool = False) -> DockerResult:
        """
        Pull an image.

        Args:
            image: Image to pull (e.g. "nginx:latest")
            quiet: Suppress verbose output

        Returns:
            DockerResult: Result with streaming logs

        Example:
            ```python
            # Pull and see progress
            result = sandbox.docker.pull("nginx:latest")
            for line in result.logs():
                print(line)  # See pull progress

            # Or just wait
            result = sandbox.docker.pull("nginx:latest")
            if result.success:
                print("Pull succeeded!")
            ```
        """
        # Ensure authentication before pull operations
        if not self._authenticated:
            self._auto_login()

        cmd = ["docker", "pull"]
        if quiet:
            cmd.append("-q")
        cmd.append(image)
        return self._result(*cmd)

    def build(
        self,
        tag: str,
        context: str = ".",
        dockerfile: Optional[str] = None,
        build_args: Optional[Dict[str, str]] = None,
        no_cache: bool = False,
        quiet: bool = False,
    ) -> DockerResult:
        """
        Build a Docker image.

        Args:
            tag: Image tag (e.g. "myapp:v1")
            context: Build context path
            dockerfile: Path to Dockerfile
            build_args: Build arguments
            no_cache: Don't use cache
            quiet: Suppress output

        Returns:
            DockerResult: Result with streaming build logs

        Example:
            ```python
            # Build and stream logs
            result = sandbox.docker.build("myapp:v1", context=".")
            for line in result.logs():
                print(line)  # See build progress

            # Or wait for completion
            result = sandbox.docker.build("myapp:v1", context=".")
            if result.success:
                print("Build succeeded!")
            else:
                print(f"Build failed: {result.stderr}")
            ```
        """
        # Ensure authentication before build operations
        if not self._authenticated:
            self._auto_login()

        cmd = ["docker", "build", "-t", tag]

        # Use host networking for gVisor
        # Safe because "host" = sandbox's network namespace, still isolated
        cmd.extend(["--network", "host"])

        if dockerfile:
            cmd.extend(["-f", dockerfile])

        if build_args:
            for k, v in build_args.items():
                cmd.extend(["--build-arg", f"{k}={v}"])

        if no_cache:
            cmd.append("--no-cache")

        if quiet:
            cmd.append("--quiet")

        cmd.append(context)
        return self._result(*cmd)

    def images(self, quiet: bool = False) -> Union[List[str], str]:
        """
        List images.

        Args:
            quiet: Only return image IDs

        Returns:
            List[str]: Image IDs if quiet=True
            str: Formatted table if quiet=False
        """
        cmd = ["docker", "images"]
        if quiet:
            cmd.append("-q")

        output = self._run(*cmd)
        return output.split("\n") if quiet and output else output

    def rmi(self, image: str, force: bool = False) -> bool:
        """Remove an image. Returns True on success, False if image not found or in use."""
        from beta9.exceptions import DockerCommandError

        try:
            cmd = ["docker", "rmi"]
            if force:
                cmd.append("-f")
            cmd.append(image)
            self._run(*cmd)
            return True
        except DockerCommandError:
            return False

    def push(self, image: str) -> bool:
        """Push an image to registry. Returns True on success, False if not authenticated or network error."""
        from beta9.exceptions import DockerCommandError

        try:
            self._run("docker", "push", image)
            return True
        except DockerCommandError:
            return False

    def tag(self, source: str, target: str) -> bool:
        """Tag an image. Returns True on success, False if source image not found."""
        from beta9.exceptions import DockerCommandError

        try:
            self._run("docker", "tag", source, target)
            return True
        except DockerCommandError:
            return False

    # === Authentication ===

    def login(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
        registry: Optional[str] = None,
    ) -> bool:
        """
        Login to a Docker registry.

        Args:
            username: Docker registry username. If not provided, reads from DOCKER_USERNAME env var.
            password: Docker registry password. If not provided, reads from DOCKER_PASSWORD env var.
            registry: Docker registry URL. If not provided, defaults to Docker Hub.

        Returns:
            bool: True if login succeeded, False otherwise.

        Example:
            ```python
            # Login with explicit credentials
            sandbox.docker.login(username="myuser", password="mypass")

            # Login using environment variables (DOCKER_USERNAME, DOCKER_PASSWORD)
            sandbox.docker.login()

            # Login to a private registry
            sandbox.docker.login(username="myuser", password="mypass", registry="registry.example.com")
            ```
        """
        from beta9.exceptions import DockerCommandError

        try:
            # Get credentials from env if not provided
            if not username:
                proc = self.sandbox_instance.process.exec("printenv", "DOCKER_USERNAME")
                proc.wait()
                username = proc.stdout.read().strip()

            if not password:
                proc = self.sandbox_instance.process.exec("printenv", "DOCKER_PASSWORD")
                proc.wait()
                password = proc.stdout.read().strip()

            if not username or not password:
                return False

            # Use docker login with password-stdin for security
            # Pass password via stdin using process env to avoid shell escaping issues
            registry_arg = f" {shlex.quote(registry)}" if registry else ""
            login_cmd = (
                f"docker login --username {shlex.quote(username)} --password-stdin{registry_arg}"
            )

            # Use printf instead of echo to handle special characters in password
            cmd = ["sh", "-c", f"printf '%s' {shlex.quote(password)} | {login_cmd}"]

            p = self._exec(*cmd)
            exit_code = p.wait()

            if exit_code == 0:
                self._authenticated = True
                return True
            return False
        except (DockerCommandError, Exception):
            return False

    # === Docker Compose ===

    def compose_up(
        self,
        file: str = "docker-compose.yml",
        detach: bool = True,
        build: bool = False,
        cwd: Optional[str] = None,
    ) -> "SandboxProcess":
        """
        Start services from docker-compose file.

        Args:
            file: Path to docker-compose file (default: "docker-compose.yml")
            detach: Run in background (default: True)
            build: Build images before starting (default: False)
            cwd: Working directory to run compose from (default: None, uses sandbox's current directory)

        Returns:
            SandboxProcess: Process object for the compose command

        Example:
            ```python
            # Start services from a compose file
            process = sandbox.docker.compose_up()
            process.wait()

            # Start with build and custom path
            process = sandbox.docker.compose_up(file="./myapp/docker-compose.yml", build=True, cwd="/workspace/myapp")
            ```
        """
        # Ensure authentication before compose operations
        if not self._authenticated:
            self._auto_login()

        # Create override file to force host networking for gVisor compatibility
        override_path = "/tmp/.docker-compose-gvisor-override.yml"
        override_content = """# Auto-generated for gVisor compatibility
# Bridge networking is not supported in Docker-in-gVisor
networks:
  default:
    name: host
    external: true
"""
        self.sandbox_instance.process.exec("sh", "-c", f"echo '{override_content}' > {override_path}").wait()
        
        cmd = ["docker-compose", "-f", file, "-f", override_path, "up"]
        if detach:
            cmd.append("-d")
        if build:
            cmd.append("--build")

        env = {}
        if cwd:
            return self.sandbox_instance.process.exec(*cmd, cwd=cwd, env=env)

        return self.sandbox_instance.process.exec(*cmd, env=env)

    def compose_down(
        self,
        file: str = "docker-compose.yml",
        volumes: bool = False,
        cwd: Optional[str] = None,
    ) -> bool:
        """
        Stop and remove compose services.

        Args:
            file: Path to docker-compose file (default: "docker-compose.yml")
            volumes: Also remove volumes (default: False)
            cwd: Working directory to run compose from (default: None)

        Returns:
            bool: True on success, False if compose file not found.
        """
        from beta9.exceptions import DockerCommandError

        try:
            cmd = ["docker-compose", "-f", file, "down"]
            if volumes:
                cmd.append("-v")

            if cwd:
                p = self.sandbox_instance.process.exec(*cmd, cwd=cwd)
                return p.wait() == 0

            self._run(*cmd)
            return True
        except DockerCommandError:
            return False

    def compose_logs(
        self,
        file: str = "docker-compose.yml",
        follow: bool = False,
        cwd: Optional[str] = None,
    ) -> "SandboxProcess":
        """
        View compose service logs.

        Args:
            file: Path to docker-compose file (default: "docker-compose.yml")
            follow: Follow log output (default: False)
            cwd: Working directory to run compose from (default: None)

        Returns:
            SandboxProcess: Process object for streaming logs
        """
        cmd = ["docker-compose", "-f", file, "logs"]
        if follow:
            cmd.append("-f")

        if cwd:
            return self.sandbox_instance.process.exec(*cmd, cwd=cwd)
        return self._exec(*cmd)

    def compose_ps(self, file: str = "docker-compose.yml", cwd: Optional[str] = None) -> str:
        """
        List compose services.

        Args:
            file: Path to docker-compose file (default: "docker-compose.yml")
            cwd: Working directory to run compose from (default: None)

        Returns:
            str: List of compose services
        """
        cmd = ["docker-compose", "-f", file, "ps"]

        if cwd:
            p = self.sandbox_instance.process.exec(*cmd, cwd=cwd)
            p.wait()
            return p.stdout.read()

        return self._run(*cmd)

    def compose_build(
        self,
        file: str = "docker-compose.yml",
        no_cache: bool = False,
        pull: bool = False,
        cwd: Optional[str] = None,
    ) -> "SandboxProcess":
        """
        Build or rebuild services from docker-compose file.

        Args:
            file: Path to docker-compose file (default: "docker-compose.yml")
            no_cache: Don't use cache when building (default: False)
            pull: Always pull newer versions of images (default: False)
            cwd: Working directory to run compose from (default: None)

        Returns:
            SandboxProcess: Process object for the compose build command

        Example:
            ```python
            # Build services
            process = sandbox.docker.compose_build()
            process.wait()

            # Build without cache
            process = sandbox.docker.compose_build(no_cache=True, cwd=\"/workspace/myapp\")
            for line in process.logs:
                print(line)
            ```
        """
        # Ensure authentication before compose build operations
        if not self._authenticated:
            self._auto_login()

        cmd = ["docker-compose", "-f", file, "build"]

        if no_cache:
            cmd.append("--no-cache")
        if pull:
            cmd.append("--pull")

        env = {}

        if cwd:
            return self.sandbox_instance.process.exec(*cmd, cwd=cwd, env=env)
        return self.sandbox_instance.process.exec(*cmd, env=env)

    # === Networks ===

    def network_create(self, name: str, driver: Optional[str] = None) -> bool:
        """Create a network. Returns True on success, False if network already exists."""
        from beta9.exceptions import DockerCommandError

        try:
            cmd = ["docker", "network", "create"]
            if driver:
                cmd.extend(["--driver", driver])
            cmd.append(name)
            self._run(*cmd)
            return True
        except DockerCommandError:
            return False

    def network_rm(self, name: str) -> bool:
        """Remove a network. Returns True on success, False if network not found or in use."""
        from beta9.exceptions import DockerCommandError

        try:
            self._run("docker", "network", "rm", name)
            return True
        except DockerCommandError:
            return False

    def network_ls(self, quiet: bool = False) -> Union[List[str], str]:
        """List networks."""
        cmd = ["docker", "network", "ls"]
        if quiet:
            cmd.append("-q")
        output = self._run(*cmd)
        return output.split("\n") if quiet and output else output

    # === Volumes ===

    def volume_create(self, name: str) -> bool:
        """Create a volume. Returns True on success, False if volume already exists."""
        from beta9.exceptions import DockerCommandError

        try:
            self._run("docker", "volume", "create", name)
            return True
        except DockerCommandError:
            return False

    def volume_rm(self, name: str, force: bool = False) -> bool:
        """Remove a volume. Returns True on success, False if volume not found or in use."""
        from beta9.exceptions import DockerCommandError

        try:
            cmd = ["docker", "volume", "rm"]
            if force:
                cmd.append("-f")
            cmd.append(name)
            self._run(*cmd)
            return True
        except DockerCommandError:
            return False

    def volume_ls(self, quiet: bool = False) -> Union[List[str], str]:
        """List volumes."""
        cmd = ["docker", "volume", "ls"]
        if quiet:
            cmd.append("-q")
        output = self._run(*cmd)
        return output.split("\n") if quiet and output else output
