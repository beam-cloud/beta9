from datetime import datetime
from pathlib import Path
from typing import NamedTuple, Optional, Union

from betterproto import Casing

from ..abstractions.base import BaseAbstraction
from ..clients.output import (
    OutputSaveRequest,
    OutputSaveResponse,
    OutputServiceStub,
    OutputSignUrlRequest,
    OutputSignUrlResponse,
    OutputStatRequest,
    OutputStatResponse,
)
from ..env import is_local
from ..runner.state import thread_local


class Stat(NamedTuple):
    mode: str  # protection bits
    size: int  # size in bytes
    atime: datetime  # accessed time
    mtime: datetime  # modified time


class Output(BaseAbstraction):
    def __init__(self, *, path: Union[Path, str]) -> None:
        super().__init__()

        if is_local():
            raise Exception("cant run locally")

        self.task_id = thread_local.task_id
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
        content = self.path.open(mode="rb").read()

        res: OutputSaveResponse = self.run_sync(
            self.stub.output_save(OutputSaveRequest(self.task_id, self.path.name, content))
        )

        if not res.ok:
            raise OutputSaveError(res.err_msg)

        self.id = res.id

    def stat(self) -> Stat:
        if not self.id:
            raise OutputNotSavedError

        res: OutputStatResponse = self.run_sync(
            self.stub.output_stat(OutputStatRequest(self.id, self.task_id, self.path.name))
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

    def public_url(self, *a, **kw) -> str:
        return self.signed_url(*a, **kw)

    def signed_url(self, expires: int = 3600) -> str:
        if not self.id:
            raise OutputNotSavedError

        res: OutputSignUrlResponse
        res = self.run_sync(
            self.stub.output_sign_url(
                OutputSignUrlRequest(self.id, self.task_id, self.path.name, expires)
            )
        )
        if not res.ok:
            raise OutputSignURLError(res.err_msg)

        return res.signed_url


class OutputNotFoundError(Exception):
    pass


class OutputSaveError(Exception):
    pass


class OutputNotSavedError(Exception):
    pass


class OutputSignURLError(Exception):
    pass


class OutputTaskIdError(Exception):
    pass
