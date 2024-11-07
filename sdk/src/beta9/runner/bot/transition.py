from ...channel import handle_error
from ...clients.bot import (
    BotServiceStub,  # noqa
    PopBotTaskRequest,  # noqa
    PushBotMarkerRequest,  # noqa
)
from ...runner.common import FunctionContext, FunctionHandler, config


class BotTransition:
    @handle_error()
    def __init__(self) -> None:
        context = FunctionContext.new(config=config, task_id=config.task_id)
        payload = {}
        inputs = payload.get("inputs") or {}

        self.handler = FunctionHandler(handler_path=config.handler)
        self.result = self.handler(context, inputs)

    def start(self):
        pass


if __name__ == "__main__":
    bt = BotTransition()
    bt.start()
