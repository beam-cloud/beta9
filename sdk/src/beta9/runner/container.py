import asyncio
import base64
import os
import signal
import subprocess
import sys

from grpclib.client import Channel

from beta9.clients.container import ContainerServiceStub
from beta9.config import with_runner_context
from beta9.type import TaskStatus


class ContainerManager:
    def __init__(self, cmd: str) -> None:
        self.processe = None
        self.pid: int = os.getpid()
        self.exit_code: int = 0
        self.task_id = os.getenv("TASK_ID")
        self.killed = False

        signal.signal(signal.SIGTERM, self.shutdown)

    @with_runner_context
    def start(self, channel: Channel):
        loop = asyncio.get_event_loop()

        async def _run():
            container_stub = ContainerServiceStub(channel)
            await container_stub.update_task_status(
                task_id=self.task_id,
                status=TaskStatus.Running,
            )

            self.process = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            self.process.wait()

            if not self.killed:
                await container_stub.update_task_status(
                    task_id=self.task_id,
                    status=TaskStatus.Complete,
                )

        loop.run_until_complete(_run())

    def shutdown(self, *_, **__):
        if self.process:
            self.killed = True
            self.process.kill()


if __name__ == "__main__":
    cmd = base64.b64decode(sys.argv[1])
    container = ContainerManager(cmd)
    container.start()
