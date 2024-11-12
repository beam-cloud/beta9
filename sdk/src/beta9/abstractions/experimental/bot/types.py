import json
from typing import Any, Optional

from ....clients.bot import (
    BotServiceStub,
    PushBotEventRequest,
)
from ....runner.common import FunctionContext
from .bot import BotEventType


class BotContext(FunctionContext):
    session_id: str = ""
    transition_name: str = ""
    bot_stub: BotServiceStub = None

    @classmethod
    def new(
        cls,
        *,
        config: Any,
        task_id: Optional[str],
        on_start_value: Optional[Any] = None,
        session_id: str = "",
        transition_name: str = "",
        bot_stub: BotServiceStub = None,
    ) -> "BotContext":
        """
        Create a new instance of BotContext, to be passed directly into a bot transition handler
        """

        instance = cls(
            container_id=config.container_id,
            stub_id=config.stub_id,
            stub_type=config.stub_type,
            callback_url=config.callback_url,
            python_version=config.python_version,
            task_id=task_id,
            bind_port=config.bind_port,
            timeout=config.timeout,
            on_start_value=on_start_value,
        )

        instance.session_id = session_id
        instance.transition_name = transition_name
        instance.bot_stub = bot_stub
        return instance

    def push_event(cls, *, event_type: BotEventType, event_value: str):
        print(f"Sending bot event<{event_type}> {event_value}")
        cls.bot_stub.push_bot_event(
            PushBotEventRequest(
                stub_id=cls.stub_id,
                session_id=cls.session_id,
                event_type=event_type,
                event_value=event_value,
            )
        )

    def prompt(cls, msg: str):
        cls.bot_stub.push_bot_event(
            PushBotEventRequest(
                stub_id=cls.stub_id,
                session_id=cls.session_id,
                event_type=BotEventType.TRANSITION_MESSAGE,
                event_value=msg,
            )
        )

    def remember(cls, obj: Any):
        cls.bot_stub.push_bot_event(
            PushBotEventRequest(
                stub_id=cls.stub_id,
                session_id=cls.session_id,
                event_type=BotEventType.MEMORY_MESSAGE,
                event_value=json.dumps(obj),
            )
        )
