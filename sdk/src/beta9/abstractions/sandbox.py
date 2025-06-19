import atexit
import io
import shlex
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Union

from .. import terminal
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
    PodSandboxDeleteFileRequest,
    PodSandboxDownloadFileRequest,
    PodSandboxExecRequest,
    PodSandboxExposePortRequest,
    PodSandboxExposePortResponse,
    PodSandboxFindInFilesRequest,
    PodSandboxKillRequest,
    PodSandboxListFilesRequest,
    PodSandboxReplaceInFilesRequest,
    PodSandboxStatFileRequest,
    PodSandboxStatusRequest,
    PodSandboxStderrRequest,
    PodSandboxStdoutRequest,
    PodSandboxUpdateTtlRequest,
    PodSandboxUpdateTtlResponse,
    PodSandboxUploadFileRequest,
    PodServiceStub,
)
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
        sync_local_dir (bool):
            Whether to sync the local directory to the sandbox filesystem on creation. Default is False.

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
        sync_local_dir: bool = False,
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
            )

        terminal.header("Creating sandbox")

        create_response: CreatePodResponse = self.stub.create_pod(
            CreatePodRequest(
                stub_id=self.stub_id,
            )
        )

        if create_response.ok:
            terminal.header(f"Sandbox created successfully ===> {create_response.container_id}")

            if self.keep_warm_seconds < 0:
                terminal.header(
                    "This sandbox has no timeout, it will run until it is shut down manually."
                )
            else:
                terminal.header(
                    f"This sandbox will timeout after {self.keep_warm_seconds} seconds."
                )

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
    process management, file system operations, preview URLs, and lifecycle
    management.

    Attributes:
        container_id (str): The unique ID of the created sandbox container.
        fs (SandboxFileSystem): File system interface for the sandbox.
        process (SandboxProcessManager): Process management interface for the sandbox.

    Example:
        ```python
        # Create a sandbox instance
        instance = sandbox.create()

        # Access file system
        instance.fs.upload_file("local_file.txt", "/remote_file.txt")

        # Run processes
        result = instance.process.run_code("import os; print(os.getcwd())")

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
        self.terminated = False
        atexit.register(self._cleanup)

    def _cleanup(self):
        try:
            if hasattr(self, "container_id") and self.container_id and not self.terminated:
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
        cwd: Optional[str] = None,
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
            process = SandboxProcess(self.sandbox_instance, response.pid)
            self.processes[response.pid] = process
            return process

    def list_processes(self) -> List["SandboxProcess"]:
        """
        List all processes running in the sandbox.

        Returns:
            List[SandboxProcess]: List of active process objects.

        Example:
            ```python
            processes = pm.list_processes()
            for process in processes:
                print(f"Process {process.pid} is running")
            ```
        """
        return list(self.processes.values())

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
        if pid not in self.processes:
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
        Read all remaining output from the stream.

        Returns:
            str: All output from the stream as a single string.
        """
        output = []
        for line in self:
            output.append(line)

        return "".join(output)


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

    def __init__(self, sandbox_instance: SandboxInstance, pid: int):
        self.sandbox_instance = sandbox_instance
        self.pid = pid
        self.exit_code = -1
        self._status = ""

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

        Note: This method is not yet implemented.

        Parameters:
            sandbox_path (str): The path where the directory should be created.

        Raises:
            NotImplementedError: This method is not yet implemented.
        """
        raise NotImplementedError("Create directory not implemented")

    def delete_directory(self, sandbox_path: str):
        """
        Delete a directory in the sandbox.

        Note: This method is not yet implemented.

        Parameters:
            sandbox_path (str): The path of the directory to delete.

        Raises:
            NotImplementedError: This method is not yet implemented.
        """
        raise NotImplementedError("Delete directory not implemented")

    def delete_file(self, sandbox_path: str):
        """
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
        """
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
        """
        Find files matching a pattern in the sandbox.

        This method searches for files within the specified directory
        that match the given pattern.

        Parameters:
            sandbox_path (str): The directory path to search in.
            pattern (str): The pattern to match files against.

        Returns:
            List[SandboxFileSearchResult]: List of matching file information objects.

        Raises:
            SandboxFileSystemError: If the search fails.

        Example:
            ```python
            # Find all Python files
            python_files = fs.find_in_files("/workspace", "*.py")

            # Find all log files
            log_files = fs.find_in_files("/var/log", "*.log")
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
