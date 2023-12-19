import asyncio
import fnmatch
import hashlib
import os
import uuid
import zipfile
from typing import Any, Generator, Union

from beam.clients.gateway import (
    GatewayServiceStub,
    HeadObjectResponse,
    ObjectMetadata,
    PutAndExtractObjectResponse,
)
from beam.terminal import Terminal

IGNORE_FILE_NAME = ".beamignore"


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
        Terminal.detail(f"Reading {IGNORE_FILE_NAME} file")

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
        Terminal.detail(f"Collecting files from {self.root_dir}")

        for root, dirs, files in os.walk(self.root_dir):
            dirs[:] = [d for d in dirs if not self._should_ignore(os.path.join(root, d))]

            for file in files:
                file_path = os.path.join(root, file)

                if not self._should_ignore(file_path):
                    yield file_path

    def _run_sync(self, coroutine) -> Any:
        return self.loop.run_until_complete(coroutine)

    def sync(self) -> bool:
        Terminal.header("Syncing files")

        self.ignore_patterns = self._read_ignore_file()
        temp_zip_name = f"/tmp/{uuid.uuid4()}"

        with zipfile.ZipFile(temp_zip_name, "w") as zipf:
            for file in self._collect_files():
                zipf.write(file, os.path.relpath(file, self.root_dir))
                Terminal.detail(f"Added {file}")

        object_id = None
        size = 0
        object_content = None

        with open(temp_zip_name, "rb") as f:
            object_content = f.read()
            size = len(object_content)
            object_id = hashlib.sha256(f.read()).hexdigest()

        head_response: HeadObjectResponse = self._run_sync(
            self.gateway_stub.head_object(object_id=object_id)
        )
        put_response: Union[PutAndExtractObjectResponse, None] = None
        if not head_response.exists:
            metadata = ObjectMetadata(name=object_id, size=size)

            with Terminal.progress("Uploading"):
                put_response: PutAndExtractObjectResponse = self._run_sync(
                    self.gateway_stub.put_and_extract_object(
                        object_content=object_content,
                        object_metadata=metadata,
                        destination=f"/data/objects/{object_id}",
                    )
                )

        os.remove(temp_zip_name)

        if not put_response.ok:
            Terminal.header("File sync failed ☠️")
            return False

        Terminal.header("Files synced")
        return True
