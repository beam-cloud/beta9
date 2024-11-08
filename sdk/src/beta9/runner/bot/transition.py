import os
from typing import Any, Dict

from ...channel import Channel, handle_error, pass_channel
from ...clients.bot import (
    BotServiceStub,
    PopBotTaskRequest,
    PopBotTaskResponse,
    PushBotEventRequest,
)
from ...clients.gateway import (
    EndTaskRequest,
    EndTaskResponse,
    GatewayServiceStub,
    StartTaskRequest,
    StartTaskResponse,
)
from ...logging import json_output_interceptor
from ...runner.common import FunctionContext, FunctionHandler, config


class BotTransition:
    def __init__(self) -> None:
        self.handler = FunctionHandler(handler_path=config.handler)

    def format_inputs(self, markers: Dict[str, Any]) -> Dict[str, Any]:
        expected_inputs = self.handler.handler.config.get("inputs", {})
        formatted_inputs = {}

        for marker_class in expected_inputs.keys():
            marker_name = marker_class.__name__

            if marker_name in markers.keys():
                marker_data = markers[marker_name]
                formatted_inputs[marker_class] = marker_class(
                    **{
                        field.field_name: field.field_value
                        for field in marker_data.markers[0].fields
                    }
                )

        print(f"formatted_inputs: {formatted_inputs}")
        return formatted_inputs

    def run(self, inputs: Dict[str, Any]):
        context = FunctionContext.new(config=config, task_id=config.task_id)
        outputs = self.handler(context, inputs)
        return outputs


@json_output_interceptor(task_id=config.task_id)
@handle_error()
@pass_channel
def main(channel: Channel):
    bt: BotTransition = BotTransition()

    bot_stub: BotServiceStub = BotServiceStub(channel)
    gateway_stub: GatewayServiceStub = GatewayServiceStub(channel)
    task_id: str = config.task_id
    session_id: str = os.environ.get("SESSION_ID")
    transition_name: str = os.environ.get("TRANSITION_NAME")

    bot_stub.push_bot_event(
        PushBotEventRequest(
            stub_id=config.stub_id,
            session_id=session_id,
            event_type="task_started",
            event_value=task_id,
        )
    )

    task_args: PopBotTaskResponse = bot_stub.pop_bot_task(
        PopBotTaskRequest(
            stub_id=config.stub_id, session_id=session_id, transition_name=transition_name
        )
    )

    if not task_args.ok:
        raise RuntimeError("Failed to pop task.")

    inputs = bt.format_inputs(task_args.markers)
    print(f"inputs: {inputs}")

    start_task_response: StartTaskResponse = gateway_stub.start_task(
        StartTaskRequest(task_id=task_id, container_id=config.container_id)
    )
    if not start_task_response.ok:
        raise RuntimeError("Failed to start task.")

    # Run the transition
    output = bt.run(inputs=inputs)  # noqa

    # End the task
    end_task_response: EndTaskResponse = gateway_stub.end_task(
        EndTaskRequest(
            task_id=task_id,
            container_id=config.container_id,
            keep_warm_seconds=0,
            task_status="COMPLETE",
            task_duration=10.0,
        )
    )
    if not end_task_response.ok:
        raise RuntimeError("Failed to end task.")

    bot_stub.push_bot_event(
        PushBotEventRequest(
            stub_id=config.stub_id,
            session_id=session_id,
            event_type="task_completed",  # TODO: convert to a enum of different event types
            event_value=task_id,
        )
    )


if __name__ == "__main__":
    main()
