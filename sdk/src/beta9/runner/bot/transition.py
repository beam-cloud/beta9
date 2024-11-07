from ...clients.bot import (
    BotServiceStub,  # noqa
    PopBotTaskRequest,  # noqa
    PushBotMarkerRequest,  # noqa
)
from ...runner.common import FunctionHandler, config


class BotTransition:
    def __init__(self) -> None:
        print(config.handler)
        self.handler = FunctionHandler(handler_path=config.handler)
        print(self.handler.handler)
        # self.result = handler(context, *args, **kwargs)

    def start(self):
        pass


if __name__ == "__main__":
    bt = BotTransition()
    bt.start()
