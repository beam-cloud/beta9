import hashlib
import os
import tempfile
import threading
import zipfile
from http import HTTPStatus
from pathlib import Path
from queue import Queue
from typing import Generator, NamedTuple

import requests
from watchdog.events import FileSystemEvent, FileSystemEventHandler

from beta9.vendor.pathspec import PathSpec

from . import terminal
from .clients.gateway import (
    CreateObjectRequest,
    CreateObjectResponse,
    GatewayServiceStub,
    HeadObjectRequest,
    HeadObjectResponse,
    ObjectMetadata,
    PutObjectRequest,
    SyncContainerWorkspaceOperation,
)
from .config import get_settings
from .env import is_local, is_notebook_env

_sync_lock = threading.Lock()

# Global workspace object id to signal to any other threads that the workspace has already been synced
_workspace_object_id = ""


def set_workspace_object_id(object_id: str) -> None:
    global _workspace_object_id
    _workspace_object_id = object_id


def get_workspace_object_id() -> str:
    global _workspace_object_id
    if not _workspace_object_id:
        _workspace_object_id = ""
    return _workspace_object_id


CHUNK_SIZE = 1024 * 1024 * 4


def ignore_file_name() -> str:
    return f".{get_settings().name}ignore".lower()


def ignore_file_contents() -> str:
    return f"""# Generated by {get_settings().name} SDK
.{get_settings().name.lower()}ignore
pyproject.toml
.git
.idea
.python-version
.vscode
.venv
venv
__pycache__
.DS_Store
.config
drive/MyDrive
.coverage
.pytest_cache
.ipynb
.ruff_cache
.dockerignore
.ipynb_checkpoints
.env.local
.envrc
**/__pycache__/
**/.pytest_cache/
**/node_modules/
**/.venv/
*.pyc
.next/
.circleci
"""


class FileSyncResult(NamedTuple):
    success: bool = False
    object_id: str = ""


class FileSyncer:
    def __init__(
        self,
        gateway_stub: GatewayServiceStub,
        root_dir=".",
    ):
        self.root_dir = Path(root_dir).absolute()
        self.gateway_stub: GatewayServiceStub = gateway_stub
        self.is_workspace_dir = root_dir == "."

    @property
    def ignore_file_path(self) -> Path:
        return self.root_dir / ignore_file_name()

    def _init_ignore_file(self) -> None:
        if not is_local():
            return

        if self.ignore_file_path.exists():
            return

        terminal.detail(f"Writing {ignore_file_name()} file")
        with self.ignore_file_path.open(mode="w") as f:
            f.writelines(ignore_file_contents())

    def _read_ignore_file(self) -> list:
        if not is_local():
            return []

        terminal.detail(f"Reading {ignore_file_name()} file")

        patterns = []

        if self.ignore_file_path.is_file():
            with self.ignore_file_path.open() as file:
                patterns = [line.strip() for line in file.readlines() if line.strip()]

        return patterns

    def _should_ignore(self, path: str) -> bool:
        relative_path = os.path.relpath(path, self.root_dir)
        spec = PathSpec.from_lines("gitwildmatch", self.ignore_patterns)
        return spec.match_file(relative_path)

    def _collect_files(self) -> Generator[str, None, None]:
        terminal.detail(f"Collecting files from {self.root_dir}")

        for root, dirs, files in os.walk(self.root_dir):
            dirs[:] = [d for d in dirs if not self._should_ignore(os.path.join(root, d))]

            for file in files:
                file_path = os.path.join(root, file)

                if not self._should_ignore(file_path):
                    yield file_path

    @staticmethod
    def _calculate_sha256(file_path: str, chunk_size: int = CHUNK_SIZE) -> str:
        hasher = hashlib.sha256()
        with open(file_path, "rb") as file:
            while chunk := file.read(chunk_size):
                hasher.update(chunk)
        return hasher.hexdigest()

    def sync(self) -> FileSyncResult:
        with _sync_lock:
            if self.is_workspace_dir and get_workspace_object_id() != "" and not is_notebook_env():
                terminal.header("Files already synced")
                return FileSyncResult(success=True, object_id=get_workspace_object_id())
            return self._sync()

    def _sync(self) -> FileSyncResult:
        terminal.header("Syncing files")

        self._init_ignore_file()
        self.ignore_patterns = self._read_ignore_file()
        temp_zip_name = tempfile.NamedTemporaryFile(delete=False).name

        with zipfile.ZipFile(temp_zip_name, "w") as zipf:
            for file in self._collect_files():
                try:
                    zipf.write(file, os.path.relpath(file, self.root_dir))
                    terminal.detail(f"Added {file}")
                except OSError as e:
                    terminal.warn(f"Failed to add {file}: {e}")

        size = os.path.getsize(temp_zip_name)
        hash = self._calculate_sha256(temp_zip_name)

        terminal.detail(f"Collected object is {terminal.humanize_memory(size, base=10)}")

        object_id = None
        head_response: HeadObjectResponse = self.gateway_stub.head_object(
            HeadObjectRequest(hash=hash)
        )
        if not head_response.exists:
            metadata = ObjectMetadata(name=hash, size=size)

            def _upload_object() -> requests.Response:
                with terminal.progress_open(temp_zip_name, "rb", description=None) as file:
                    response = requests.put(presigned_url, data=file)
                return response

            # TODO: remove this once all workspaces are migrated to use workspace storage
            def stream_requests():
                with terminal.progress_open(temp_zip_name, "rb", description=None) as file:
                    while chunk := file.read(CHUNK_SIZE):
                        yield PutObjectRequest(chunk, metadata, hash, False)

            terminal.header("Uploading")
            if head_response.use_workspace_storage:
                create_object_response: CreateObjectResponse = self.gateway_stub.create_object(
                    CreateObjectRequest(
                        object_metadata=metadata, hash=hash, size=size, overwrite=False
                    )
                )
                if create_object_response.ok and self.is_workspace_dir:
                    presigned_url = create_object_response.presigned_url
                    response = _upload_object()
                    if response.status_code == HTTPStatus.OK:
                        set_workspace_object_id(create_object_response.object_id)
                        object_id = create_object_response.object_id
                    else:
                        terminal.error("File sync failed ☠️")
            else:
                put_response = self.gateway_stub.put_object_stream(stream_requests())
                if put_response.ok and self.is_workspace_dir:
                    set_workspace_object_id(put_response.object_id)
                    object_id = put_response.object_id
                else:
                    terminal.error("File sync failed ☠️")

        elif head_response.exists and head_response.ok:
            terminal.header("Files already synced")

            if self.is_workspace_dir:
                set_workspace_object_id(head_response.object_id)
                object_id = head_response.object_id

            return FileSyncResult(success=True, object_id=head_response.object_id)

        os.remove(temp_zip_name)

        if object_id is None:
            terminal.error("File sync failed ☠️")
            return FileSyncResult(success=False, object_id="")

        terminal.header("Files synced")
        return FileSyncResult(success=True, object_id=object_id)


class SyncEventHandler(FileSystemEventHandler):
    def __init__(self, queue: Queue):
        super().__init__()
        self.queue = queue

    def on_any_event(self, event: FileSystemEvent) -> None:
        if not event.is_directory and event.src_path.endswith(".py"):
            terminal.warn(f"Detected changes in '{event.src_path}'. Reloading...")

    def on_created(self, event) -> None:
        self.queue.put((SyncContainerWorkspaceOperation.WRITE, event.src_path, None))

    def on_modified(self, event: FileSystemEvent) -> None:
        self.on_created(event)

    def on_deleted(self, event: FileSystemEvent) -> None:
        self.queue.put((SyncContainerWorkspaceOperation.DELETE, event.src_path, None))

    def on_moved(self, event: FileSystemEvent) -> None:
        self.queue.put((SyncContainerWorkspaceOperation.MOVED, event.src_path, event.dest_path))
