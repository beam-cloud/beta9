import json
import os
import time
import traceback
from typing import Any, Dict, get_args, get_origin

from ...abstractions.experimental.bot.bot import BotEventType
from ...abstractions.experimental.bot.types import BotContext
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
from ...runner.common import FunctionHandler, config, end_task_and_send_callback
from ...type import TaskStatus


class BotTransitionResult:
    def __init__(self, outputs: Dict[str, Any], exception: BaseException):
        self.outputs = outputs
        self.exception = exception


class BotTransition:
    def __init__(self, session_id: str, transition_name: str, bot_stub: BotServiceStub) -> None:
        self.handler = FunctionHandler(handler_path=config.handler)
        self.session_id = session_id
        self.transition_name = transition_name
        self.bot_stub = bot_stub

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
                            field_type = str  # default to string if field is not defined

                        # Try to convert field_value to field_type
                        try:
                            if get_origin(field_type) is dict and isinstance(field_value, str):
                                converted_value = json.loads(field_value)
                            else:
                                converted_value = field_type(field_value)
                        except (ValueError, TypeError, json.JSONDecodeError):
                            converted_value = field_value

                        fields_dict[field_name] = converted_value

                    formatted_inputs[marker_class].append(marker_class(**fields_dict))

        return formatted_inputs

    def _format_outputs(self, outputs: Dict[str, Any]) -> Dict[str, Any]:
        expected_outputs = self.handler.handler.config.get("outputs", [])
        formatted_outputs = {}

        if not isinstance(outputs, dict):
            return formatted_outputs

        for output, markers in outputs.items():
            if output not in expected_outputs:
                continue

            location_name = output.__name__
            marker_list = []

            if not isinstance(markers, list):
                if hasattr(markers, "model_dump"):
                    markers = [markers]

            for marker in markers:
                fields = []
                marker_dict = marker.model_dump()
                marker_annotations = marker.__annotations__

                for field_name, field_value in marker_dict.items():
                    field_type = marker_annotations.get(field_name, str)

                    # Decompose the field_type to handle complex types like Optional[int]
                    origin_type = get_origin(field_type) or field_type
                    args = get_args(field_type)

                    # Determine if the field_type is a subclass of the desired types
                    if isinstance(origin_type, type) and issubclass(
                        origin_type, (bool, int, float, list, dict)
                    ):
                        converted_value = json.dumps(field_value)
                    elif args and all(
                        isinstance(arg, type) and issubclass(arg, (bool, int, float, list, dict))
                        for arg in args
                    ):
                        converted_value = json.dumps(field_value)
                    else:
                        converted_value = str(field_value)

                    fields.append(MarkerField(field_name=field_name, field_value=converted_value))

                # Create a Marker for each set of fields
                marker_list.append(Marker(location_name=location_name, fields=fields))

            # Assign a single PushBotMarkersRequestMarkerList for all markers at this location
            formatted_outputs[location_name] = PushBotMarkersRequestMarkerList(markers=marker_list)

        return formatted_outputs

    def run(self, inputs: Dict[str, Any]) -> BotTransitionResult:
        result = BotTransitionResult(outputs={}, exception=None)
        context: BotContext = BotContext.new(
            config=config,
            task_id=config.task_id,
            session_id=self.session_id,
            transition_name=self.transition_name,
            bot_stub=self.bot_stub,
        )

        context.push_event(
            event_type=BotEventType.TRANSITION_STARTED, event_value=self.transition_name
        )

        try:
            outputs = self.handler(context=context, inputs=self._format_inputs(inputs))
            outputs = self._format_outputs(outputs)
            result.outputs = outputs
        except BaseException as exc:
            error_message = (
                f"Error occurred in transition<{context.transition_name}>: {traceback.format_exc()}"
            )
            print(error_message)
            context.push_event(event_type=BotEventType.AGENT_MESSAGE, event_value=error_message)
            result.exception = exc

        return result


@json_output_interceptor(task_id=config.task_id)
@handle_error()
@pass_channel
def main(channel: Channel):
    bot_stub: BotServiceStub = BotServiceStub(channel)
    session_id: str = os.environ.get("SESSION_ID")
    transition_name: str = os.environ.get("TRANSITION_NAME")

    bt: BotTransition = BotTransition(
        session_id=session_id, transition_name=transition_name, bot_stub=bot_stub
    )
    gateway_stub: GatewayServiceStub = GatewayServiceStub(channel)
    task_id: str = config.task_id

    task_args: PopBotTaskResponse = bot_stub.pop_bot_task(
        PopBotTaskRequest(
            stub_id=config.stub_id,
            session_id=session_id,
            transition_name=transition_name,
            task_id=task_id,
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

    task_status = TaskStatus.Complete

    # Run the transition
    inputs = task_args.markers
    result = bt.run(inputs=inputs)
    if result.exception:
        task_status = TaskStatus.Error
    else:
        push_bot_markers_response: PushBotMarkersResponse = bot_stub.push_bot_markers(
            PushBotMarkersRequest(
                stub_id=config.stub_id,
                session_id=session_id,
                markers=result.outputs,
                source_task_id=task_id,
            )
        )
        if not push_bot_markers_response.ok:
            raise RunnerException("Failed to push markers.")

    end_task_response = end_task_and_send_callback(
        gateway_stub=gateway_stub,
        payload={},
        end_task_request=EndTaskRequest(
            task_id=task_id,
            task_duration=time.time() - start_time,
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
            event_type=BotEventType.TRANSITION_COMPLETED
            if task_status == TaskStatus.Complete
            else BotEventType.TRANSITION_FAILED,
            event_value=transition_name,
            metadata={
                "task_id": task_id,
                "session_id": session_id,
                "transition_name": transition_name,
            },
        )
    )


if __name__ == "__main__":
    main()
