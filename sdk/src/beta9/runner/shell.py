import sys
import traceback
from threading import Event
from typing import List, Union
from ..abstractions.image import Image
from src.beta9.runner.common import config
from beta9.abstractions.container import Container
from .. import terminal
from beta9.clients.container import CommandExecutionRequest, CommandExecutionResponse

class Shell:
    def __init__(self):
        self.exit_code = 0
        self.image = None
        self.container = None 

    def create_shell(self):
        image = Image(
            python_version="python3.10",
            python_packages=["numpy", "pandas==2.2.2"],
            commands=["apt-get update", "apt-get install -y curl"]
        )

        self.container = Container(cpu=1.0, memory=128, image=image)

        if not self.container.stub_id:
            terminal.error("Failed to create container")
            return
        while True:
            command = input("root@beam:~# ")
            if command.lower() in ['exit', 'quit']:
                break
            self.stream_output(command)

    def stream_output(self, command: str):
        if not self.container:
            terminal.error("Container not initialized.")
            return

        try:
            for response in self.container.stub.ExecuteCommand(
                CommandExecutionRequest(stub_id=self.container.stub_id, command=command.encode())
            ):
                if response.output:
                    print(response.output, end="")  
                if response.done:
                    self.exit_code = response.exit_code
                    break

        except Exception as e:
            terminal.error(f"Error executing command: {e}")
            self.exit_code = 1

if __name__ == "__main__":
    s = Shell()
    try:
        s.create_shell()
    except BaseException:
        print(f"Error occurred: {traceback.format_exc()}")
        s.exit_code = 1

    if s.exit_code != 0:
        sys.exit(s.exit_code)