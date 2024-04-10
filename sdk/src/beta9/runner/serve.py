import atexit
import os
import signal
import subprocess
import sys
import traceback
from typing import List, Union

from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver

from ..abstractions.base.runner import (
    ENDPOINT_SERVE_STUB_TYPE,
    FUNCTION_SERVE_STUB_TYPE,
    TASKQUEUE_SERVE_STUB_TYPE,
)
from ..exceptions import RunnerException
from ..runner.common import USER_CODE_VOLUME
from ..runner.common import config as cfg


class SyncEventHandler(FileSystemEventHandler):
    def __init__(self, restart_callback):
        super().__init__()
        self.restart_callback = restart_callback

    def on_created(self, event):
        self._trigger_reload(event)

    def on_modified(self, event):
        self._trigger_reload(event)

    def on_deleted(self, event):
        self._trigger_reload(event)

    def _trigger_reload(self, event):
        print(event)
        if not event.is_directory:
            self.restart_callback()


class ServeGateway:
    def __init__(self) -> None:
        self.process: Union[subprocess.Popen, None] = None
        self.exit_code: int = 0
        self.watch_dir: str = USER_CODE_VOLUME

        # Set up the file change event handler and observer
        self.event_handler = SyncEventHandler(self.restart)
        self.observer = PollingObserver()
        self.observer.schedule(self.event_handler, self.watch_dir, recursive=True)
        self.observer.start()

    def shutdown(self, signum=None, frame=None) -> None:
        if self.process:
            self.process.send_signal(signal.SIGTERM)

        if self.process and self.process.poll() is None:
            self.process.terminate()
            self.process = None

    def run(self, *, command: List[str]) -> None:
        if self.process:
            self.shutdown()

        self.process = subprocess.Popen(
            " ".join(command),
            shell=True,
            preexec_fn=lambda: signal.signal(signal.SIGINT, signal.SIG_IGN),
            env=os.environ,
            stdout=sys.stdout,
            stderr=sys.stdout,
        )

        self.exit_code = self.process.wait()

    def restart(self) -> None:
        print("Detected file change, restarting...")
        self.run(command=_command())


def _command() -> List[str]:
    command = []

    if cfg.stub_type == ENDPOINT_SERVE_STUB_TYPE:
        command = [cfg.python_version, "-m", "beta9.runner.endpoint"]
    elif cfg.stub_type == TASKQUEUE_SERVE_STUB_TYPE:
        command = [cfg.python_version, "-m", "beta9.runner.taskqueue"]
    elif cfg.stub_type == FUNCTION_SERVE_STUB_TYPE:
        command = [cfg.python_version, "-m", "beta9.runner.function"]
    else:
        raise RunnerException(f"Invalid stub type: {cfg.stub_type}")

    return command


if __name__ == "__main__":
    sg = ServeGateway()
    atexit.register(sg.shutdown)

    try:
        sg.run(command=_command())
    except BaseException:
        print(f"Error occurred: {traceback.format_exc()}")
        sg.exit_code = 1

    if sg.exit_code != 0:
        sys.exit(sg.exit_code)
