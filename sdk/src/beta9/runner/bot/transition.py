import json
import os
import time
from typing import Any, Dict

from ...channel import Channel, handle_error, pass_channel
from ...clients.bot import (
    BotServiceStub,
    Marker,
    MarkerField,
    PopBotTaskRequest,
    PopBotTaskResponse,
    PushBotEventRequest,
    PushBotMarkersRequest,
    PushBotMarkersRequestMarkerList,
    PushBotMarkersResponse,
)
from ...clients.gateway import (
    EndTaskRequest,
    GatewayServiceStub,
    StartTaskRequest,
    StartTaskResponse,
)
from ...exceptions import RunnerException, TaskEndError
from ...logging import json_output_interceptor
from ...runner.common import FunctionContext, FunctionHandler, config, end_task_and_send_callback
from ...type import TaskStatus


class BotTransition:
    def __init__(self) -> None:
        self.handler = FunctionHandler(handler_path=config.handler)

    def _format_inputs(self, markers: Dict[str, Any]) -> Dict[str, Any]:
        expected_inputs = self.handler.handler.config.get("inputs", {})
        formatted_inputs = {}

        for marker_class in expected_inputs.keys():
            marker_name = marker_class.__name__

            if marker_name in markers.keys():
                marker_data = markers[marker_name]

                if marker_class not in formatted_inputs.keys():
                    formatted_inputs[marker_class] = []

                for marker in marker_data.markers:
                    fields_dict = {}

                    for field in marker.fields:
                        field_name = field.field_name
                        field_value = field.field_value

                        # Get field type from marker_class
                        if field_name in marker_class.model_fields:
                            field_type = marker_class.model_fields[field_name].annotation
                        else:
                            field_type = str  # default to string if field not defined

                        # Try to convert field_value to field_type
                        try:
                            converted_value = field_type(field_value)
                        except (ValueError, TypeError):
                            converted_value = field_value

                        fields_dict[field_name] = converted_value

                    formatted_inputs[marker_class].append(marker_class(**fields_dict))

        return formatted_inputs

    def _format_outputs(self, outputs: Dict[str, Any]) -> Dict[str, Any]:
        expected_outputs = self.handler.handler.config.get("outputs", {})
        formatted_outputs = {}

        for output, markers in outputs.items():
            if output not in expected_outputs:
                continue

            location_name = output.__name__
            marker_list = []

            for marker in markers:
                fields = []
                marker_dict = marker.model_dump()
                marker_annotations = marker.__annotations__

                for field_name, field_value in marker_dict.items():
                    field_type = marker_annotations.get(field_name, str)

                    # Serialize the field_value based on its type
                    if issubclass(field_type, (bool, int, float, list, dict)):
                        converted_value = json.dumps(field_value)
                    else:
                        converted_value = str(field_value)

                    fields.append(MarkerField(field_name=field_name, field_value=converted_value))

                # Create a Marker for each set of fields
                marker_list.append(Marker(location_name=location_name, fields=fields))

            # Assign a single PushBotMarkersRequestMarkerList for all markers at this location
            formatted_outputs[location_name] = PushBotMarkersRequestMarkerList(markers=marker_list)

        return formatted_outputs

    def run(self, inputs: Dict[str, Any]):
        context = FunctionContext.new(config=config, task_id=config.task_id)
        outputs = self.handler(context, self._format_inputs(inputs))
        return self._format_outputs(outputs)


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

    # TODO: store tasks one by one instead of in a queue
    task_args: PopBotTaskResponse = bot_stub.pop_bot_task(
        PopBotTaskRequest(
            stub_id=config.stub_id, session_id=session_id, transition_name=transition_name
        )
    )

    if not task_args.ok:
        raise RunnerException("Failed to retrieve task.")

    start_time = time.time()
    start_task_response: StartTaskResponse = gateway_stub.start_task(
        StartTaskRequest(task_id=task_id, container_id=config.container_id)
    )
    if not start_task_response.ok:
        raise RunnerException("Failed to start task.")

    # Run the transition
    inputs = task_args.markers
    outputs = bt.run(inputs=inputs)

    push_bot_markers_response: PushBotMarkersResponse = bot_stub.push_bot_markers(
        PushBotMarkersRequest(stub_id=config.stub_id, session_id=session_id, markers=outputs)
    )
    if not push_bot_markers_response.ok:
        raise RunnerException("Failed to push markers.")

    task_status = TaskStatus.Complete
    task_duration = time.time() - start_time

    end_task_response = end_task_and_send_callback(
        gateway_stub=gateway_stub,
        payload={},
        end_task_request=EndTaskRequest(
            task_id=task_id,
            task_duration=task_duration,
            task_status=task_status,
            container_id=config.container_id,
            container_hostname=config.container_hostname,
            keep_warm_seconds=config.keep_warm_seconds,
        ),
    )

    if not end_task_response.ok:
        raise TaskEndError

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
