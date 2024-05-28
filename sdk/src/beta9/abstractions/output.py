import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Generator, NamedTuple, Optional, Union

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
from ..sync import CHUNK_SIZE


class Output(BaseAbstraction):
    """
    A file that a task has created.

    Use this to save a file you may want to save and share later.
    """

    def __init__(self, *, path: Union[Path, str]) -> None:
        super().__init__()

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

    @property
    def stub(self) -> OutputServiceStub:
        if not self._stub:
            self._stub = OutputServiceStub(self.channel)
        return self._stub

    @stub.setter
    def stub(self, value: OutputServiceStub):
        self._stub = value

    def save(self) -> None:
        def stream_request() -> Generator[OutputSaveRequest, None, None]:
            with self.path.open(mode="rb") as file:
                while chunk := file.read(CHUNK_SIZE):
                    yield OutputSaveRequest(self.task_id, self.path.name, chunk)

        res: OutputSaveResponse = self.stub.output_save_stream(stream_request())
        if not res.ok:
            raise OutputSaveError(res.err_msg)

        self.id = res.id

    def stat(self) -> "Stat":
        if not self.id:
            raise OutputNotSavedError

        res: OutputStatResponse = self.stub.output_stat(
            OutputStatRequest(self.id, self.task_id, self.path.name)
        )

        if not res.ok:
            raise OutputNotFoundError(res.err_msg)

        stat = res.stat.to_pydict(casing=Casing.SNAKE)  # type:ignore
        return Stat(**stat)

    def exists(self) -> bool:
        try:
            return True if self.stat() else False
        except (OutputNotSavedError, OutputNotFoundError):
            return False

    def public_url(self, expires: int = 3600) -> str:
        if not self.id:
            raise OutputNotSavedError

        res: OutputPublicUrlResponse
        res = self.stub.output_public_url(
            OutputPublicUrlRequest(self.id, self.task_id, self.path.name, expires)
        )

        if not res.ok:
            raise OutputPublicURLError(res.err_msg)

        return res.public_url


class Stat(NamedTuple):
    mode: str  # protection bits
    size: int  # size in bytes
    atime: datetime  # accessed time
    mtime: datetime  # modified time

    def to_dict(self) -> Dict[str, Any]:
        return self._asdict()


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
