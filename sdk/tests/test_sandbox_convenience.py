from types import SimpleNamespace

from beta9.abstractions.sandbox import SandboxFileSystem, SandboxInstance


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

    def sandbox_upload_file(self, request):
        self.uploads.append(request)
        return SimpleNamespace(ok=True, error_msg="")

    def sandbox_download_file(self, request):
        return SimpleNamespace(ok=True, error_msg="", data=b"hello")

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
    fs = SandboxFileSystem(fake_instance(stub))

    fs.write_text("/workspace/message.txt", "hello")
    assert stub.uploads[-1].container_path == "/workspace/message.txt"
    assert stub.uploads[-1].data == b"hello"

    fs.write_bytes("/workspace/blob.bin", b"\x00\x01")
    assert stub.uploads[-1].container_path == "/workspace/blob.bin"
    assert stub.uploads[-1].data == b"\x00\x01"

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
