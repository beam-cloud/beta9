package common

import "github.com/beam-cloud/beta9/pkg/types"

type ContainerSchedulingFailure struct {
	TaskID       string
	ContainerID  string
	PoolSelector string
	Reason       types.ContainerSchedulingFailureReason
}

func NewContainerSchedulingFailedEvent(failure ContainerSchedulingFailure) *Event {
	return &Event{
		Type: EventTypeContainerSchedulingFailed,
		Args: map[string]any{
			types.EventAttrTaskID:       failure.TaskID,
			types.EventAttrContainerID:  failure.ContainerID,
			types.EventAttrPoolSelector: failure.PoolSelector,
			types.EventAttrReason:       string(failure.Reason),
		},
		LockAndDelete: true,
	}
}

func ParseContainerSchedulingFailure(event *Event) (ContainerSchedulingFailure, bool) {
	if event == nil || event.Type != EventTypeContainerSchedulingFailed {
		return ContainerSchedulingFailure{}, false
	}

	failure := ContainerSchedulingFailure{
		TaskID:       eventString(event.Args, types.EventAttrTaskID),
		ContainerID:  eventString(event.Args, types.EventAttrContainerID),
		PoolSelector: eventString(event.Args, types.EventAttrPoolSelector),
		Reason:       types.ContainerSchedulingFailureReason(eventString(event.Args, types.EventAttrReason)),
	}
	return failure, failure.TaskID != "" && failure.ContainerID != "" && failure.Reason != ""
}

func eventString(args map[string]any, key string) string {
	value, _ := args[key].(string)
	return value
}
