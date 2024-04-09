import atexit
import sys


class ServeGateway:
    def __init__(self) -> None:
        self.exit_code = 0

    def shutdown(self):
        pass

    def run(self):
        pass

    def reload(self):
        pass


if __name__ == "__main__":
    sg = ServeGateway()
    atexit.register(sg.shutdown)

    sg.run()

    if sg.exit_code != 0:
        sys.exit(sg.exit_code)
