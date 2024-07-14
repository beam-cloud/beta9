import threading
from typing import Callable, Optional, Union

from ..abstractions.base import BaseAbstraction
from ..clients.signal import (
    SignalClearRequest,
    SignalClearResponse,
    SignalMonitorRequest,
    SignalMonitorResponse,
    SignalServiceStub,
    SignalSetRequest,
    SignalSetResponse,
)
from ..env import called_on_import


class Signal(BaseAbstraction):
    """"""

    def __init__(self, *, name: str, handler: Optional[Callable] = None) -> None:
        """
        Creates a Signal Instance.

        """
        super().__init__()

        self.name: str = name
        self.handler: Union[Callable, None] = handler
        self._stub: Optional[SignalServiceStub] = None

        if self.handler is not None and called_on_import():
            threading.Thread(
                target=self._monitor,
                daemon=True,
            ).start()

    @property
    def stub(self) -> SignalServiceStub:
        if not self._stub:
            self._stub = SignalServiceStub(self.channel)
        return self._stub

    @stub.setter
    def stub(self, value: SignalServiceStub):
        self._stub = value

    def set(self) -> bool:
        r: SignalSetResponse = self.stub.signal_set(SignalSetRequest(name=self.name))
        return r.ok

    def clear(self) -> bool:
        r: SignalClearResponse = self.stub.signal_clear(SignalClearRequest(name=self.name))
        return r.ok

    def _monitor(self) -> None:
        for response in self.stub.signal_monitor(
            SignalMonitorRequest(
                name=self.name,
            )
        ):
            response: SignalMonitorResponse
            if response.set:
                self.handler()
