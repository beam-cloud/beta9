import hashlib
import io
from types import SimpleNamespace
from unittest.mock import MagicMock, call

from beta9.abstractions.base.runner import SANDBOX_STUB_TYPE
from beta9.abstractions.image import ImageBuildResult
from beta9.abstractions.sandbox import Sandbox, SandboxFileSystem, SandboxInstance
from beta9.clients.gateway import GetOrCreateStubResponse
from beta9.sync import FileSyncResult


class FakeSandboxStub:
    def __init__(self):
        self.uploads = []
        self.deleted_files = []
        self.deleted_dirs = []
        self.statuses = []
        self.file_info = SimpleNamespace(
            name="file.txt",
            is_dir=False,
            size=4,
            mode=644,
            mod_time=0,
            owner="",
            group="",
            permissions=644,
        )

    def sandbox_download_file(self, request):
        return SimpleNamespace(ok=True, error_msg="", data=b"hello")

    def sandbox_upload_file(self, request):
        self.uploads.append(request)
        return SimpleNamespace(ok=True, error_msg="")

    def sandbox_stat_file(self, request):
        return SimpleNamespace(ok=True, error_msg="", file_info=self.file_info)

    def sandbox_delete_file(self, request):
        self.deleted_files.append(request)
        return SimpleNamespace(ok=True, error_msg="")

    def sandbox_delete_directory(self, request):
        self.deleted_dirs.append(request)
        return SimpleNamespace(ok=True, error_msg="")

    def sandbox_status(self, request):
        if self.statuses:
            return self.statuses.pop(0)
        return SimpleNamespace(ok=True, error_msg="", exit_code=-1, status="running")


def fake_instance(stub):
    instance = SandboxInstance.__new__(SandboxInstance)
    instance.container_id = "sandbox-123"
    instance.stub = stub
    return instance


def test_sandbox_filesystem_text_bytes_and_remove_wrappers():
    stub = FakeSandboxStub()
    instance = fake_instance(stub)
    instance.process = MagicMock()
    instance.process.exec.side_effect = [
        SimpleNamespace(wait=lambda timeout: 0, stdout=io.StringIO(output))
        for data in (b"hello", b"\x00\x01")
        for output in (hashlib.sha256(data).hexdigest(), "")
    ]
    fs = SandboxFileSystem(instance)

    fs.write_text("/workspace/message.txt", "hello")
    fs.write_bytes("/workspace/blob.bin", b"\x00\x01")
    assert len(stub.uploads) == 2
    expected_commands = []
    for request, path, data in zip(
        stub.uploads, ("/workspace/message.txt", "/workspace/blob.bin"), (b"hello", b"\x00\x01")
    ):
        assert request.container_id == "sandbox-123"
        assert request.container_path.startswith(path + ".beta9-")
        assert (request.data, request.mode, request.offset) == (data, 0o644, 0)
        expected_commands.extend(
            [
                call("sha256sum", "--", request.container_path),
                call("mv", "--", request.container_path, path),
            ]
        )
    assert instance.process.exec.call_args_list == expected_commands

    assert fs.read_bytes("/workspace/message.txt") == b"hello"
    assert fs.read_text("/workspace/message.txt") == "hello"

    fs.remove("/workspace/message.txt")
    assert stub.deleted_files[-1].container_path == "/workspace/message.txt"

    stub.file_info.is_dir = True
    fs.remove("/workspace/data")
    assert stub.deleted_dirs[-1].container_path == "/workspace/data"


def test_sandbox_instance_poll_and_wait():
    stub = FakeSandboxStub()
    instance = fake_instance(stub)

    stub.statuses = [SimpleNamespace(ok=True, error_msg="", exit_code=-1, status="running")]
    assert instance.poll() is None

    stub.statuses = [
        SimpleNamespace(ok=True, error_msg="", exit_code=-1, status="running"),
        SimpleNamespace(ok=True, error_msg="", exit_code=17, status="complete"),
    ]
    assert instance.wait(timeout=1) == 17


def test_sandbox_instance_poll_defaults_terminal_exit_code():
    stub = FakeSandboxStub()
    instance = fake_instance(stub)

    stub.statuses = [SimpleNamespace(ok=True, error_msg="", exit_code=-1, status="terminated")]
    assert instance.poll() == 137


def test_sandbox_prepare_runtime_uses_idle_entrypoint():
    sandbox = Sandbox()
    sandbox.image.build = MagicMock(
        return_value=ImageBuildResult(
            success=True, image_id="image-id", python_version="python3.11"
        )
    )
    sandbox.syncer.sync = MagicMock(
        return_value=FileSyncResult(success=True, object_id="object-id")
    )
    sandbox.gateway_stub.get_or_create_stub = MagicMock(
        return_value=GetOrCreateStubResponse(ok=True, stub_id="stub-id")
    )

    assert sandbox.prepare_runtime(
        stub_type=SANDBOX_STUB_TYPE,
        force_create_stub=True,
        ignore_patterns=["*"],
    )

    request = sandbox.gateway_stub.get_or_create_stub.call_args.args[0]
    assert request.entrypoint == ["tail", "-f", "/dev/null"]
