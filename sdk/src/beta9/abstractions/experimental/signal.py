import threading
import time
from typing import Callable, Optional, Union

from ...clients.signal import (
    SignalClearRequest,
    SignalClearResponse,
    SignalMonitorRequest,
    SignalMonitorResponse,
    SignalServiceStub,
    SignalSetRequest,
    SignalSetResponse,
)
from ...env import called_on_import
from ..base import BaseAbstraction


class Signal(BaseAbstraction):
    def __init__(
        self,
        *,
        name: str,
        handler: Optional[Callable] = None,
        clear_after_interval: Optional[int] = -1,
    ) -> None:
        """

        Creates a Signal Instance. Signals can be used to notify a container to do something using a flag.
        For example, you may want to reload some global state, send a webhook, or exit the container.
        Signals are an experimental feature.

        Parameters:
        name (str):
            The name of the signal.
        handler (Optional[Callable]):
            If set, this function will be called when the signal is set. Default is None.
        clear_after_interval (Optional[int]):
            If both handler and clear_after_interval are set, the signal will be automatically cleared after
            'clear_after_interval' seconds.

        Example usage:
            ```

            # This is how you would set up a consumer of a signal:

            s = Signal(name="reload-model", handler=reload_model, clear_after_interval=5)
            some_global_model = None

            def load_model():
                global some_global_model
                some_global_model = LoadIt()

            @endpoint(on_start=load_model)
            def handler(**kwargs):
                global some_global_model
                return some_global_model(kwargs["param1"])

            # To trigger load_model to happen again while the container is still running, another process can run:
            s = Signal(name="reload-model")
            s.set(ttl=60)

            ```
        """
        super().__init__()

        self.name: str = name
        self.handler: Union[Callable, None] = handler
        self._stub: Optional[SignalServiceStub] = None
        self.clear_after_interval: Optional[int] = clear_after_interval

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

    def set(self, ttl: Optional[int] = 600) -> bool:
        """Fires an event to another container to notify the container that something has occurred"""
        r: SignalSetResponse = self.stub.signal_set(SignalSetRequest(name=self.name, ttl=ttl))
        return r.ok

    def clear(self) -> bool:
        """Removes the signal flag that has been set. This supersedes clear_after_interval, if set"""
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
                print(f"Signal '{self.name}' received, running handler: {self.handler}")
                self.handler()

            if self.clear_after_interval > 0:
                time.sleep(self.clear_after_interval)
                self.clear()
