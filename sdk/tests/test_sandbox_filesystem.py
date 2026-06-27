import asyncio
import unittest
from dataclasses import dataclass

from beta9.abstractions.sandbox import SandboxFileInfo, SandboxFileSystem
from beta9.clients.pod import (
    PodSandboxCreateDirectoryResponse,
    PodSandboxStatFileResponse,
    PodSandboxUploadFileResponse,
)
from beta9.exceptions import SandboxFileSystemError


@dataclass
class FakeSandboxInstance:
    container_id: str = "sandbox-123"

    def __post_init__(self):
        self.stub = FakePodStub()


class FakePodStub:
    def __init__(self):
        self.files = {}
        self.dirs = {"/"}
        self.uploads = []
        self.created_dirs = []

    def sandbox_stat_file(self, request):
        path = request.container_path
        if path in self.dirs:
            return PodSandboxStatFileResponse(
                ok=True,
                file_info=SandboxFileInfo(
                    name=path,
                    is_dir=True,
                    size=0,
                    mode=0o755,
                    mod_time=0,
                    owner="root",
                    group="root",
                    permissions=0o755,
                ),
            )

        if path in self.files:
            return PodSandboxStatFileResponse(
                ok=True,
                file_info=SandboxFileInfo(
                    name=path,
                    is_dir=False,
                    size=len(self.files[path]),
                    mode=0o644,
                    mod_time=0,
                    owner="root",
                    group="root",
                    permissions=0o644,
                ),
            )

        return PodSandboxStatFileResponse(ok=False, error_msg="not found")

    def sandbox_create_directory(self, request):
        self.created_dirs.append(request.container_path)
        self.dirs.add(request.container_path)
        return PodSandboxCreateDirectoryResponse(ok=True)

    def sandbox_upload_file(self, request):
        self.uploads.append(request)
        self.files[request.container_path] = request.data
        return PodSandboxUploadFileResponse(ok=True)


class TestSandboxFileSystemCreateFile(unittest.TestCase):
    def setUp(self):
        self.instance = FakeSandboxInstance()
        self.fs = SandboxFileSystem(self.instance)

    def test_create_file_uploads_string_contents(self):
        self.fs.create_file("/workspace/app.py", "print('hi')\n")

        upload = self.instance.stub.uploads[-1]
        self.assertEqual(upload.container_id, "sandbox-123")
        self.assertEqual(upload.container_path, "/workspace/app.py")
        self.assertEqual(upload.data, b"print('hi')\n")
        self.assertEqual(upload.mode, 0o644)

    def test_create_file_accepts_bytes_and_mode(self):
        self.fs.create_file("tmp/model.bin", b"\x00\x01", mode=0o600)

        upload = self.instance.stub.uploads[-1]
        self.assertEqual(upload.container_path, "/tmp/model.bin")
        self.assertEqual(upload.data, b"\x00\x01")
        self.assertEqual(upload.mode, 0o600)

    def test_create_file_creates_missing_parents_in_order(self):
        self.fs.create_file("/workspace/src/app.py", "print(1)", parents=True)

        self.assertEqual(self.instance.stub.created_dirs, ["/workspace", "/workspace/src"])
        self.assertIn("/workspace/src/app.py", self.instance.stub.files)

    def test_create_file_refuses_to_overwrite_by_default(self):
        self.instance.stub.files["/workspace/app.py"] = b"old"

        with self.assertRaises(FileExistsError):
            self.fs.create_file("/workspace/app.py", "new")

        self.assertEqual(self.instance.stub.files["/workspace/app.py"], b"old")

    def test_create_file_allows_explicit_overwrite(self):
        self.instance.stub.files["/workspace/app.py"] = b"old"

        self.fs.create_file("/workspace/app.py", "new", overwrite=True)

        self.assertEqual(self.instance.stub.files["/workspace/app.py"], b"new")

    def test_create_file_rejects_parent_that_is_file(self):
        self.instance.stub.files["/workspace"] = b"not a dir"

        with self.assertRaises(SandboxFileSystemError):
            self.fs.create_file("/workspace/app.py", "print(1)", parents=True)

    def test_create_file_rejects_invalid_contents(self):
        with self.assertRaises(ValueError):
            self.fs.create_file("/workspace/app.py", object())  # type: ignore[arg-type]

    def test_async_create_file_delegates_to_sync_filesystem(self):
        asyncio.run(self.fs.aio.create_file("/workspace/async.py", "print('async')"))

        self.assertEqual(self.instance.stub.files["/workspace/async.py"], b"print('async')")


if __name__ == "__main__":
    unittest.main()
