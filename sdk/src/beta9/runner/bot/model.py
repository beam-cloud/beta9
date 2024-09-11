from ..common import FunctionHandler, config


class BotTransition:
    def __init__(self) -> None:
        self.handler = FunctionHandler(handler_path=config.handler)
        print(self.handler)

    def start(self):
        pass


if __name__ == "__main__":
    bt = BotTransition()
    bt.start()
