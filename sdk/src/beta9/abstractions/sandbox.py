import io
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Union

from .. import terminal
from ..abstractions.base.runner import (
    POD_RUN_STUB_TYPE,
    BaseAbstraction,
)
from ..abstractions.image import Image
from ..abstractions.pod import Pod
from ..abstractions.volume import CloudBucket, Volume
from ..clients.gateway import GatewayServiceStub, StopContainerRequest, StopContainerResponse
from ..clients.pod import (
    CreatePodRequest,
    CreatePodResponse,
    PodSandboxDownloadFileRequest,
    PodSandboxExecRequest,
    PodSandboxKillRequest,
    PodSandboxStatusRequest,
    PodSandboxStderrRequest,
    PodSandboxStdoutRequest,
    PodSandboxUploadFileRequest,
    PodServiceStub,
)
from ..exceptions import SandboxFileSystemError, SandboxProcessError
from ..type import GpuType, GpuTypeAlias


class Sandbox(Pod):
    """

    Parameters:
        cpu (Union[int, float, str]):
            The number of CPU cores allocated to the container. Default is 1.0.
        memory (Union[int, str]):
            The amount of memory allocated to the container. It should be specified in
            MiB, or as a string with units (e.g. "1Gi"). Default is 128 MiB.
        gpu (Union[GpuType, str]):
            The type or name of the GPU device to be used for GPU-accelerated tasks. If not
            applicable or no GPU required, leave it empty. Default is [GpuType.NoGPU](#gputype).
        image (Union[Image, dict]):
            The container image used for the task execution. Whatever you pass here will have an additional `add_python_packages` call
            with `["fastapi", "vllm", "huggingface_hub"]` added to it to ensure that we can run vLLM in the container.
        keep_warm_seconds (int):
            The number of seconds to keep the sandbox around. Default is -1 (requires manual termination).
        name (str):
            The name of the Sandbox app. Default is none, which means you must provide it during deployment.
        volumes (List[Union[Volume, CloudBucket]]):
            The volumes and/or cloud buckets to mount into the Sandbox container. Default is an empty list.
        secrets (List[str]):
            The secrets to pass to the Sandbox container.
    """

    def __init__(
        self,
        cpu: Union[int, float, str] = 1.0,
        memory: Union[int, str] = 128,
        gpu: Union[GpuTypeAlias, List[GpuTypeAlias]] = GpuType.NoGPU,
        gpu_count: int = 0,
        image: Image = Image(python_version="python3.11"),
        keep_warm_seconds: int = -1,
        authorized: bool = False,
        name: Optional[str] = None,
        volumes: Optional[List[Union[Volume, CloudBucket]]] = [],
        secrets: Optional[List[str]] = None,
    ):
        self.debug_buffer = io.StringIO()

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
        print(self.debug_buffer.getvalue())

    def create(self) -> "SandboxInstance":
        """
        Create a new sandbox instance.

        """

        self.entrypoint = ["tail", "-f", "/dev/null"]

        if not self.prepare_runtime(
            stub_type=POD_RUN_STUB_TYPE,
            force_create_stub=True,
        ):
            return SandboxInstance(
                container_id="",
                url="",
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
                container_id=create_response.container_id,
                ok=create_response.ok,
                error_msg=create_response.error_msg,
            )


@dataclass
class SandboxInstance(BaseAbstraction):
    """
    Stores the result of creating a Sandbox.

    Attributes:
        container_id: The unique ID of the created sandbox container.
        url: The URL for accessing the container over HTTP (if ports were exposed).
    """

    container_id: str
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

    def terminate(self) -> bool:
        """
        Terminate the container associated with this sandbox instance. Returns True if the container was terminated, False otherwise.
        """
        res: "StopContainerResponse" = self.gateway_stub.stop_container(
            StopContainerRequest(container_id=self.container_id)
        )
        return res.ok

    def expose_port(self, port: int):
        raise NotImplementedError("Expose port not implemented")


class SandboxProcessResponse:
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
        process = self._exec("python3", "-c", f"'{code}'", cwd=cwd, env=env)

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
        self, *args, cwd: Optional[str] = None, env: Optional[Dict[str, str]] = None
    ) -> "SandboxProcess":
        args = list(args)
        args = ["bash", "-c", "'" + " ".join(args) + "'"]
        return self._exec(args, cwd=cwd, env=env)

    def _exec(
        self, *args, cwd: Optional[str] = None, env: Optional[Dict[str, str]] = None
    ) -> "SandboxProcess":
        command = list(args) if not isinstance(args[0], list) else args[0]
        shell_command = " ".join(command)

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
        return list(self.processes.values())

    def get_process(self, pid: int) -> "SandboxProcess":
        if pid not in self.processes:
            raise SandboxProcessError(f"Process with pid {pid} not found")

        return self.processes[pid]


class SandboxProcessStream:
    def __init__(self, process: "SandboxProcess", fetch_fn):
        self.process = process
        self.fetch_fn = fetch_fn
        self._buffer = ""
        self._closed = False
        self._last_output = ""

    def __iter__(self):
        return self

    def __next__(self):
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
        output = self.fetch_fn()

        if output == self._last_output:
            return ""

        new_output = output[len(self._last_output) :]
        self._last_output = output
        return new_output

    def read(self):
        output = []
        for line in self:
            output.append(line)

        return "".join(output)


class SandboxProcess:
    def __init__(self, sandbox_instance: SandboxInstance, pid: int):
        self.sandbox_instance = sandbox_instance
        self.pid = pid
        self.exit_code = -1
        self._status = ""

    def wait(self) -> int:
        self.exit_code, self._status = self.status()

        while self.exit_code < 0:
            self.exit_code, self._status = self.status()
            time.sleep(0.1)

        return self.exit_code

    def kill(self):
        response = self.sandbox_instance.stub.sandbox_kill(
            PodSandboxKillRequest(container_id=self.sandbox_instance.container_id, pid=self.pid)
        )
        if not response.ok:
            raise SandboxProcessError(response.error_msg)

    def status(self) -> Tuple[int, str]:
        response = self.sandbox_instance.stub.sandbox_status(
            PodSandboxStatusRequest(container_id=self.sandbox_instance.container_id, pid=self.pid)
        )

        if not response.ok:
            raise SandboxProcessError(response.error_msg)

        return response.exit_code, response.status

    @property
    def stdout(self):
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
        return SandboxProcessStream(
            self,
            lambda: self.sandbox_instance.stub.sandbox_stderr(
                PodSandboxStderrRequest(
                    container_id=self.sandbox_instance.container_id, pid=self.pid
                )
            ).stderr,
        )


class SandboxFileSystem:
    """
    A SandboxFileSystem is a wrapper around the SandboxFileSystem library that allows you to deploy it as an ASGI app.
    """

    def __init__(self, sandbox_instance: SandboxInstance):
        self.sandbox_instance = sandbox_instance

    def upload_file(self, local_path: str, sandbox_path: str):
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

    def download_file(self, container_path: str, local_path: str):
        response = self.sandbox_instance.stub.sandbox_download_file(
            PodSandboxDownloadFileRequest(
                container_id=self.sandbox_instance.container_id,
                container_path=container_path,
            )
        )

        if not response.ok:
            raise SandboxFileSystemError(response.error_msg)

        with open(local_path, "wb") as f:
            f.write(response.data)

    def list_files(self, container_path: str):
        raise NotImplementedError("List files not implemented")

    def create_directory(self, container_path: str):
        raise NotImplementedError("Create directory not implemented")

    def delete_file(self, container_path: str):
        raise NotImplementedError("Delete file not implemented")

    def delete_directory(self, container_path: str):
        raise NotImplementedError("Delete directory not implemented")
