import os
import signal
import subprocess
import sys
import traceback
from threading import Event
from typing import List, Union

from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver

from ..abstractions.base.runner import (
    ASGI_SERVE_STUB_TYPE,
    ENDPOINT_SERVE_STUB_TYPE,
    FUNCTION_SERVE_STUB_TYPE,
    TASKQUEUE_SERVE_STUB_TYPE,
)
from ..exceptions import RunnerException
from ..runner.common import USER_CODE_DIR
from ..runner.common import config as cfg


class SyncEventHandler(FileSystemEventHandler):
    def __init__(self, restart_callback):
        super().__init__()
        self.restart_callback = restart_callback
        self.on_created = self._trigger_reload
        self.on_deleted = self._trigger_reload
        self.on_modified = self._trigger_reload
        self.on_moved = self._trigger_reload

    def _trigger_reload(self, event):
        if not event.is_directory and event.src_path.endswith(".py"):
            self.restart_callback()


class ServeGateway:
    def __init__(self) -> None:
        self.process: Union[subprocess.Popen, None] = None
        self.exit_code: int = 0
        self.watch_dir: str = USER_CODE_DIR
        self.restart_event = Event()
        self.exit_event = Event()

        # Register signal handlers
        signal.signal(signal.SIGTERM, self.shutdown)

        # Set up the file change event handler & observer
        self.event_handler = SyncEventHandler(self.trigger_restart)
        self.observer = PollingObserver()
        self.observer.schedule(self.event_handler, self.watch_dir, recursive=True)
        self.observer.start()

    def shutdown(self, signum=None, frame=None) -> None:
        self.kill_subprocess()
        if self.observer.is_alive():
            self.observer.stop()
            self.observer.join(timeout=0.1)
        self.exit_event.set()
        self.restart_event.set()

    def kill_subprocess(self, signum=None, frame=None) -> None:
        if self.process:
            try:
                os.killpg(
                    os.getpgid(self.process.pid), signal.SIGTERM
                )  # Send SIGTERM to the process group
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(
                    os.getpgid(self.process.pid), signal.SIGKILL
                )  # SIGTERM isn't working, kill the process group
                self.process.wait()
            except BaseException:
                pass

        self.process = None
        self.restart_event.set()

    def run(self, *, command: List[str]) -> None:
        while not self.exit_event.is_set():
            if self.process:
                self.kill_subprocess()

            self.process = subprocess.Popen(
                " ".join(command),
                shell=True,
                preexec_fn=os.setsid,
                env={
                    **os.environ,
                    "GUNICORN_NO_WAIT": "true",
                },
                stdout=sys.stdout,
                stderr=sys.stdout,
            )

            self.restart_event.clear()
            self.restart_event.wait()

    def trigger_restart(self) -> None:
        self.restart_event.set()


def _command() -> List[str]:
    command = []

    if cfg.stub_type in [ENDPOINT_SERVE_STUB_TYPE, ASGI_SERVE_STUB_TYPE]:
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

    try:
        sg.run(command=_command())
    except BaseException:
        print(f"Error occurred: {traceback.format_exc()}")
        sg.exit_code = 1

    if sg.exit_code != 0:
        sys.exit(sg.exit_code)
