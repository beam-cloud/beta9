import os
import signal
import sys
from multiprocessing import Event, set_start_method
from multiprocessing.synchronize import Event as TEvent

from ..runner.common import FunctionHandler


class AppManager:
    def __init__(self) -> None:
        # Manager attributes
        self.pid: int = os.getpid()
        self.exit_code: int = 0
        self.shutdown_event = Event()

        self._setup_signal_handlers()
        set_start_method("spawn", force=True)

        self.handler: FunctionHandler = FunctionHandler()
        print(self.handler)
        # self.on_start_value = asyncio.run(OnStartMethodHandler(worker).start())

    def _setup_signal_handlers(self):
        if os.getpid() == self.pid:
            signal.signal(signal.SIGTERM, self._init_shutdown)

    def _init_shutdown(self, signum=None, frame=None):
        self.shutdown_event.set()

    def run(self):
        self.shutdown()

    def shutdown(self):
        print("Spinning down app")

    def _start_worker(self, worker_index: int):
        pass

    def _watchdog(self, worker_index: int):
        pass


class AppWorker:
    def __init__(
        self,
        *,
        worker_index: int,
        parent_pid: int,
        worker_startup_event: TEvent,
    ) -> None:
        self.worker_index: int = worker_index
        self.parent_pid: int = parent_pid
        self.worker_startup_event: TEvent = worker_startup_event


if __name__ == "__main__":
    app = AppManager()
    app.run()

    if app.exit_code != 0:
        sys.exit(app.exit_code)
