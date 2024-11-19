import json
import mimetypes
from typing import Any, Optional, Union

from ....clients.bot import (
    BotServiceStub,
    PushBotEventBlockingRequest,
    PushBotEventBlockingResponse,
    PushBotEventRequest,
)
from ....runner.common import FunctionContext
from .bot import BotEvent, BotEventType


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
        """Send an event to the bot (supports all event types)"""

        print(f"Sending bot event<{event_type}> {event_value}")
        cls.bot_stub.push_bot_event(
            PushBotEventRequest(
                stub_id=cls.stub_id,
                session_id=cls.session_id,
                event_type=event_type,
                event_value=event_value,
                metadata={
                    "task_id": cls.task_id,
                    "session_id": cls.session_id,
                    "transition_name": cls.transition_name,
                },
            )
        )

    def prompt(
        cls, msg: str, timeout_seconds: int = 10, wait_for_response=True
    ) -> Union[BotEvent, None]:
        """Send a raw prompt to your model. By default, this will wait for a response for up to timeout_seconds."""

        if not wait_for_response:
            cls.bot_stub.push_bot_event(
                PushBotEventRequest(
                    stub_id=cls.stub_id,
                    session_id=cls.session_id,
                    event_type=BotEventType.TRANSITION_MESSAGE,
                    event_value=msg,
                    metadata={
                        "task_id": cls.task_id,
                        "session_id": cls.session_id,
                        "transition_name": cls.transition_name,
                    },
                )
            )
            return None

        r: PushBotEventBlockingResponse = cls.bot_stub.push_bot_event_blocking(
            PushBotEventBlockingRequest(
                stub_id=cls.stub_id,
                session_id=cls.session_id,
                event_type=BotEventType.TRANSITION_MESSAGE,
                event_value=msg,
                metadata={
                    "task_id": cls.task_id,
                    "session_id": cls.session_id,
                    "transition_name": cls.transition_name,
                },
                timeout_seconds=timeout_seconds,
            )
        )

        if not r.ok:
            return None

        return BotEvent(
            type=r.event.type,
            value=r.event.value,
            metadata=r.event.metadata,
        )

    def say(cls, msg: str):
        """Send a message to the user from the bot"""

        cls.bot_stub.push_bot_event(
            PushBotEventRequest(
                stub_id=cls.stub_id,
                session_id=cls.session_id,
                event_type=BotEventType.AGENT_MESSAGE,
                event_value=msg,
                metadata={
                    "task_id": cls.task_id,
                    "session_id": cls.session_id,
                    "transition_name": cls.transition_name,
                },
            )
        )

    def remember(cls, obj: Any):
        """Store an arbitrary object in the bot's memory (must be JSON serializable)"""

        cls.bot_stub.push_bot_event(
            PushBotEventRequest(
                stub_id=cls.stub_id,
                session_id=cls.session_id,
                event_type=BotEventType.MEMORY_MESSAGE,
                event_value=json.dumps(obj),
                metadata={
                    "task_id": cls.task_id,
                    "session_id": cls.session_id,
                    "transition_name": cls.transition_name,
                },
            )
        )

    def send_file(cls, *, path: str, description: str):
        """Capture a file and send it to the user"""

        from beta9 import Output

        o = Output(path=path)
        o.save()

        filetype, _ = mimetypes.guess_type(path)
        file_msg = {
            "url": o.public_url(),
            "description": description,
            "filetype": filetype,
        }

        cls.bot_stub.push_bot_event(
            PushBotEventRequest(
                stub_id=cls.stub_id,
                session_id=cls.session_id,
                event_type=BotEventType.OUTPUT_FILE,
                event_value=json.dumps(file_msg),
                metadata={
                    "task_id": cls.task_id,
                    "session_id": cls.session_id,
                    "transition_name": cls.transition_name,
                },
            )
        )
