import base64
import os
import signal
import subprocess
import sys
from typing import Union

from ..aio import run_sync
from ..channel import Channel, with_runner_context
from ..clients.gateway import EndTaskRequest, GatewayServiceStub, StartTaskRequest
from ..logging import StdoutJsonInterceptor
from ..runner.common import config
from ..type import TaskStatus
from .common import FunctionContext, end_task_and_send_callback, send_callback


def task_status(exit_code: int, *, killed: bool) -> TaskStatus:
    if exit_code == 0:
        return TaskStatus.Complete
    if killed:
        return TaskStatus.Cancelled
    return TaskStatus.Error


class ContainerManager:
    def __init__(self, cmd: str) -> None:
        self.cmd = cmd
        self.process: Union[subprocess.Popen, None] = None
        self.pid: int = os.getpid()
        self.exit_code: int = 0
        self.task_id: str = os.getenv("TASK_ID")
        self.killed: bool = False

        signal.signal(signal.SIGTERM, self.shutdown)

    @with_runner_context
    def start(self, channel: Channel):
        async def _run():
            with StdoutJsonInterceptor(task_id=self.task_id):
                stub = GatewayServiceStub(channel)
                stub.start_task(
                    StartTaskRequest(
                        task_id=self.task_id,
                        container_id=config.container_id,
                    )
                )

                self.process = subprocess.Popen(
                    ["/bin/bash", "-c", self.cmd],
                    shell=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    env=os.environ,
                )

                while self.process.poll() is None:
                    line = self.process.stdout.readline()
                    if not line:
                        continue
                    print(line.strip().decode("utf-8"))

                self.exit_code = int(self.process.returncode or 0)
                if not self.killed:
                    end_task_and_send_callback(
                        gateway_stub=stub,
                        payload={},
                        end_task_request=EndTaskRequest(
                            task_id=self.task_id,
                            container_id=config.container_id,
                            task_status=task_status(self.exit_code, killed=False),
                        ),
                    )
                else:
                    send_callback(
                        gateway_stub=stub,
                        context=FunctionContext.new(
                            config=config,
                            task_id=self.task_id,
                            on_start_value=None,
                        ),
                        payload={},
                        task_status=task_status(self.exit_code, killed=True),
                    )

        run_sync(_run())
        return self.exit_code

    def shutdown(self, *_, **__):
        if self.process:
            self.killed = True
            self.process.kill()


if __name__ == "__main__":
    cmd = base64.b64decode(sys.argv[1]).decode("utf-8")
    print(f"Running command: {cmd}")
    container = ContainerManager(cmd)
    sys.exit(container.start())
