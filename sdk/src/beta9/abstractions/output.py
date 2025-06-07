import os
import shutil
import uuid
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, BinaryIO, Dict, Generator, NamedTuple, Optional, Protocol, Union

from betterproto import Casing

from ..abstractions.base import BaseAbstraction
from ..clients.output import (
    OutputPublicUrlRequest,
    OutputPublicUrlResponse,
    OutputSaveRequest,
    OutputSaveResponse,
    OutputServiceStub,
    OutputStatRequest,
    OutputStatResponse,
)
from ..env import is_local
from ..runner.common import USER_OUTPUTS_DIR
from ..sync import CHUNK_SIZE


class Output(BaseAbstraction):
    """
    A file or directory to persist after a task has ended.
    """

    _tmp_dir = Path("/tmp/outputs")

    def __init__(self, *, path: Union[Path, str]) -> None:
        """
        Creates an Output instance.

        Use this to store the output of an invocation of a task (function, taskqueue, etc.).
        This can be any file or directory.

        When the output is a directory, it is zipped up before being saved. When it
        is a file, it's saved as-as (no zipping).

        Args:
            path: The path to a file or directory.

        Raises:
            OutputCannotRunLocallyError:
                When trying to create an Output outside of a container environment.
            OutputTaskIdError:
                When the container environment doesn't set the task ID. This is needed
                to tell the server where to save the Output.
            FileNotFoundError:
                When the user-provided path doesn't exist.

        Example:

            Saving a directory.

            ```python
            mydir = Path(path="/volumes/myvol/mydir")
            output = Output(path=mydir)
            output.save()
            ```

            Saving a file and generating a public URL.

            ```python
            myfile = "path/to/my.txt"
            output = Output(path=myfile)
            output.save()
            output_url = output.public_url()
            ```

            Saving a PIL.Image object.

            ```python
            image = pipe( ... )
            output = Output.from_pil_image(image)
            output.save()
            ```
        """
        super().__init__()
        self.prepare_tmp_dir()

        if is_local():
            raise OutputCannotRunLocallyError

        self.task_id = os.getenv("TASK_ID", "")
        if not self.task_id:
            raise OutputTaskIdError

        self.path = Path(path)
        if not self.path.exists():
            raise FileNotFoundError

        self.id: Optional[str] = None
        self._stub: Optional[OutputServiceStub] = None

    def __del__(self) -> None:
        """
        Cleans up temp data stored on disk.
        """
        try:
            shutil.rmtree(self._tmp_dir / str(id(self)))
        except FileNotFoundError:
            pass

    @property
    def stub(self) -> OutputServiceStub:
        if not self._stub:
            self._stub = OutputServiceStub(self.channel)
        return self._stub

    @stub.setter
    def stub(self, value: OutputServiceStub) -> None:
        self._stub = value

    @property
    def zipped_path(self) -> Path:
        """
        Gets a zipped path for a directory.

        The path is unique to the instance of this class.

        Raises:
            ValueError: An error indicating the output is not a directory.
        """
        if not self.path.is_dir():
            raise ValueError("Output must be a directory to get the zipped path.")

        return self._tmp_dir / str(id(self)) / f"{self.path.name}.zip"

    @classmethod
    def from_pil_image(cls, image: "PILImage", format: Optional[str] = "png") -> "Output":
        """
        Creates an instance of Output with a PIL.Image object.
        """
        cls.prepare_tmp_dir()

        path = cls._tmp_dir / str(uuid.uuid4())
        if format:
            suffix = format.lower() if format.startswith(".") else f".{format.lower()}"
            path = path.with_suffix(suffix)

        image.save(path, format=format.lstrip(".") if format else format)

        self = cls(path=path)
        new_dir = self.path.parent / str(id(self))
        new_dir.mkdir(mode=755, exist_ok=True, parents=True)
        self.path = self.path.rename(new_dir / self.path.name)

        return self

    @classmethod
    def from_file(cls, file_handle: BinaryIO) -> "Output":
        """
        Creates an instance of Output from a file-like object.

        For example:

            ```python
            with open("myfile.txt", "rb") as f:
                output = Output.from_file(f)
                output.save()
            ```
        """
        cls.prepare_tmp_dir()
        path = cls._tmp_dir / str(uuid.uuid4())

        with open(path, "wb") as tmp_file:
            shutil.copyfileobj(file_handle, tmp_file)

        return cls(path=path)

    @classmethod
    def prepare_tmp_dir(cls) -> None:
        cls._tmp_dir.mkdir(mode=755, parents=True, exist_ok=True)

    def save(self) -> "Output":
        """
        Saves the Output.

        If the Output is a directory, it will be zipped before being uploaded
        as a single file. If the Output is a file, it will be uploaded as-is
        (not zipped).

        Raises:
            OutputSaveError: An error indicating the output failed to save.
        """
        path = Path(self.path)
        if self.path.is_dir():
            path = self.zip_dir(self.path)

        storage_available = os.getenv("STORAGE_AVAILABLE", "false").lower() == "true"
        if storage_available:
            output_id = str(uuid.uuid4())
            output_path = Path(USER_OUTPUTS_DIR) / self.task_id / output_id / path.name
            output_path.parent.mkdir(mode=755, parents=True, exist_ok=True)
            shutil.copy(path, output_path)

            # Ensure the file is written to disk
            with open(output_path, "rb+") as f:
                os.fsync(f.fileno())

            self.id = output_id
            return self

        def stream_request(p: Path) -> Generator[OutputSaveRequest, None, None]:
            if p.stat().st_size == 0:
                yield OutputSaveRequest(self.task_id, p.name, b"")
                return

            with open(p, mode="rb") as file:
                while chunk := file.read(CHUNK_SIZE):
                    yield OutputSaveRequest(self.task_id, p.name, chunk)

        res: OutputSaveResponse = self.stub.output_save_stream(stream_request(path))
        if not res.ok:
            raise OutputSaveError(res.err_msg)

        self.id = res.id
        return self

    def stat(self) -> "Stat":
        """
        Gets file metadata of the saved Output.

        Raises:
            OutputNotSavedError: An error indicating the output must be saved first.
            OutputNotFoundError: An error indicating the output does not exist on the server.
        """
        if not self.id:
            raise OutputNotSavedError

        path_name = self.zipped_path.name if self.path.is_dir() else self.path.name

        res: OutputStatResponse = self.stub.output_stat(
            OutputStatRequest(self.id, self.task_id, path_name)
        )

        if not res.ok:
            raise OutputNotFoundError(res.err_msg)

        stat = res.stat.to_pydict(casing=Casing.SNAKE)  # type:ignore

        size = stat.get("size")
        if size is None:
            stat["size"] = 0

        return Stat(**stat)

    def exists(self) -> bool:
        """
        Checks if the output exists on the server.
        """
        try:
            return True if self.stat() else False
        except (OutputNotSavedError, OutputNotFoundError):
            return False

    def public_url(self, expires: int = 3600) -> str:
        """
        Generates a unique publicly accessible URL.

        Args:
            expires: When the URL expires in seconds. Defaults to 3600.

        Raises:
            OutputNotSavedError: An error indicating the output must be saved first.
            OutputPublicURLError: An error indicating an issue with the server generating the URL.
        """
        if not self.id:
            raise OutputNotSavedError

        res: OutputPublicUrlResponse
        res = self.stub.output_public_url(
            OutputPublicUrlRequest(
                self.id,
                self.task_id,
                self.zipped_path.name if self.path.is_dir() else self.path.name,
                expires,
            )
        )

        if not res.ok:
            raise OutputPublicURLError(res.err_msg)

        return res.public_url

    def zip_dir(self, dir_path: Union[Path, str], compress_level: int = 9) -> Path:
        """
        Zips the provided directory.
        """
        dir_path = Path(dir_path)
        self.zipped_path.unlink(missing_ok=True)
        self.zipped_path.parent.mkdir(mode=755, parents=True, exist_ok=True)

        with zipfile.ZipFile(
            file=self.zipped_path,
            mode="w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=compress_level,
        ) as zipf:
            for file_path in dir_path.rglob("*"):
                if file_path.is_file():
                    zipf.write(file_path, file_path.relative_to(dir_path))

        return self.zipped_path


class Stat(NamedTuple):
    mode: str  # protection bits
    size: int  # size in bytes
    atime: datetime  # accessed time
    mtime: datetime  # modified time

    def to_dict(self) -> Dict[str, Any]:
        return self._asdict()


class PILImage(Protocol):
    """
    Describes the attributes and methods needed on a PIL.Image object.
    """

    def save(self, fp, format=None, **params) -> None: ...


class OutputCannotRunLocallyError(Exception):
    pass


class OutputNotFoundError(Exception):
    pass


class OutputSaveError(Exception):
    pass


class OutputNotSavedError(Exception):
    pass


class OutputPublicURLError(Exception):
    pass


class OutputTaskIdError(Exception):
    pass
