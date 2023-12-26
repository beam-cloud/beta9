import asyncio
import fnmatch
import hashlib
import os
import uuid
import zipfile
from typing import Generator, NamedTuple, Union

from beam import terminal
from beam.clients.gateway import (
    GatewayServiceStub,
    HeadObjectResponse,
    ObjectMetadata,
    PutObjectResponse,
)

IGNORE_FILE_NAME = ".beamignore"


class FileSyncResult(NamedTuple):
    success: bool = False
    object_id: str = ""


class FileSyncer:
    def __init__(
        self,
        gateway_stub: GatewayServiceStub,
        root_dir=".",
    ):
        self.loop = asyncio.get_event_loop()
        self.root_dir = os.path.abspath(root_dir)
        self.gateway_stub: GatewayServiceStub = gateway_stub

    def _read_ignore_file(self) -> list:
        terminal.detail(f"Reading {IGNORE_FILE_NAME} file")

        ignore_file = os.path.join(self.root_dir, IGNORE_FILE_NAME)
        patterns = []

        if os.path.isfile(ignore_file):
            with open(ignore_file, "r") as file:
                patterns = [line.strip() for line in file.readlines() if line.strip()]

        return patterns

    def _should_ignore(self, path: str) -> bool:
        relative_path = os.path.relpath(path, self.root_dir)

        for pattern in self.ignore_patterns:
            if fnmatch.fnmatch(relative_path, pattern) or fnmatch.fnmatch(
                os.path.basename(path), pattern
            ):
                return True

        return False

    def _collect_files(self) -> Generator[str, None, None]:
        terminal.detail(f"Collecting files from {self.root_dir}")

        for root, dirs, files in os.walk(self.root_dir):
            dirs[:] = [d for d in dirs if not self._should_ignore(os.path.join(root, d))]

            for file in files:
                file_path = os.path.join(root, file)

                if not self._should_ignore(file_path):
                    yield file_path

    def sync(self) -> FileSyncResult:
        terminal.header("Syncing files")

        self.ignore_patterns = self._read_ignore_file()
        temp_zip_name = f"/tmp/{uuid.uuid4()}"

        with zipfile.ZipFile(temp_zip_name, "w") as zipf:
            for file in self._collect_files():
                zipf.write(file, os.path.relpath(file, self.root_dir))
                terminal.detail(f"Added {file}")

        object_id = None
        size = 0
        object_content = None

        with open(temp_zip_name, "rb") as f:
            object_content = f.read()
            size = len(object_content)
            object_id = hashlib.sha256(f.read()).hexdigest()

        head_response: HeadObjectResponse = self.loop.run_until_complete(
            self.gateway_stub.head_object(object_id=object_id)
        )
        put_response: Union[PutObjectResponse, None] = None
        if not head_response.exists:
            metadata = ObjectMetadata(name=object_id, size=size)

            with terminal.progress("Uploading"):
                put_response: PutObjectResponse = self.loop.run_until_complete(
                    self.gateway_stub.put_object(
                        object_content=object_content,
                        object_metadata=metadata,
                    )
                )
        elif head_response.exists and head_response.ok:
            return FileSyncResult(success=True, object_id=object_id)

        os.remove(temp_zip_name)

        if not put_response.ok:
            terminal.header("File sync failed ☠️")
            return FileSyncResult(success=False, object_id=put_response.object_id)

        terminal.header("Files synced")
        return FileSyncResult(success=True, object_id=put_response.object_id)
