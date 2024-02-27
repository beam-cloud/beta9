import base64
import os
import signal
import subprocess
import sys

from grpclib.client import Channel

from beta9.aio import run_sync
from beta9.clients.gateway import GatewayServiceStub
from beta9.config import with_runner_context
from beta9.runner.common import config
from beta9.type import TaskStatus


class ContainerManager:
    def __init__(self, cmd: str) -> None:
        self.process = None
        self.pid: int = os.getpid()
        self.exit_code: int = 0
        self.task_id = os.getenv("TASK_ID")
        self.killed = False

        signal.signal(signal.SIGTERM, self.shutdown)

    @with_runner_context
    def start(self, channel: Channel):
        async def _run():
            stub = GatewayServiceStub(channel)
            await stub.start_task(
                task_id=self.task_id,
                container_id=config.container_id,
            )

            sys.stdout.flush()
            sys.stderr.flush()

            self.process = subprocess.Popen(cmd, shell=True, stderr=sys.stderr, stdout=sys.stdout)
            self.process.wait()

            if not self.killed:
                await stub.end_task(
                    task_id=self.task_id,
                    container_id=config.container_id,
                    task_status=TaskStatus.Complete,
                )

        run_sync(_run())

    def shutdown(self, *_, **__):
        if self.process:
            self.killed = True
            self.process.kill()


if __name__ == "__main__":
    cmd = base64.b64decode(sys.argv[1]).decode("utf-8")
    print(f"Running command: {cmd}")
    container = ContainerManager(cmd)
    container.start()
