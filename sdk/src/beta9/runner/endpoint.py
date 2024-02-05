import os


class EndpointManager:
    def __init__(self) -> None:
        # Manager attributes
        self.pid: int = os.getpid()
        self.exit_code: int = 0
