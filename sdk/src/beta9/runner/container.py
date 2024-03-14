import base64
import os
import signal
import subprocess
import sys
import asyncio

from grpclib.client import Channel

from beta9.aio import run_sync
from beta9.clients.gateway import GatewayServiceStub
from beta9.config import with_runner_context
from beta9.runner.common import config
from beta9.type import TaskStatus
from beta9.logging import StdoutJsonInterceptor

sys.stdout = StdoutJsonInterceptor(sys.__stdout__)
sys.stderr = StdoutJsonInterceptor(sys.__stderr__)

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
            StdoutJsonInterceptor.add_context_var("task_id", self.task_id)
            
            stub = GatewayServiceStub(channel)
            await stub.start_task(
                task_id=self.task_id,
                container_id=config.container_id,
            )
                
            self.process = subprocess.Popen(["/bin/bash", "-c", cmd], shell=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=os.environ)
            
            for line in iter(self.process.stdout.readline, b''): # This breaks when the subprocess ends
                if line:
                    sys.stdout.write(line.decode("utf-8"))
            
            self.process.wait() # This isn't needed because the for loop breaks when the subprocess ends, but it's here for clarity
            
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
