package common

import (
	"strconv"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
)

// ContainerSchedulingFailure is a task's container that the scheduler gave
// up on, and why.
type ContainerSchedulingFailure struct {
	TaskID      string
	ContainerID string
	types.ContainerRequestFailure
}

func NewContainerSchedulingFailedEvent(failure ContainerSchedulingFailure) *Event {
	return &Event{
		Type: EventTypeContainerSchedulingFailed,
		Args: map[string]any{
			types.EventAttrTaskID:        failure.TaskID,
			types.EventAttrContainerID:   failure.ContainerID,
			types.EventAttrPoolSelector:  failure.PoolSelector,
			types.EventAttrReason:        string(failure.Reason),
			types.EventAttrGpus:          strings.Join(failure.Gpus, ","),
			types.EventAttrWaitedSeconds: strconv.FormatInt(failure.WaitedSeconds, 10),
		},
		LockAndDelete: true,
	}
}

func ParseContainerSchedulingFailure(event *Event) (ContainerSchedulingFailure, bool) {
	if event == nil || event.Type != EventTypeContainerSchedulingFailed {
		return ContainerSchedulingFailure{}, false
	}

	failure := ContainerSchedulingFailure{
		TaskID:      eventString(event.Args, types.EventAttrTaskID),
		ContainerID: eventString(event.Args, types.EventAttrContainerID),
		ContainerRequestFailure: types.ContainerRequestFailure{
			PoolSelector: eventString(event.Args, types.EventAttrPoolSelector),
			Reason:       types.ContainerSchedulingFailureReason(eventString(event.Args, types.EventAttrReason)),
		},
	}
	// A gateway from before these were sent leaves them out.
	if gpus := eventString(event.Args, types.EventAttrGpus); gpus != "" {
		failure.Gpus = strings.Split(gpus, ",")
	}
	failure.WaitedSeconds, _ = strconv.ParseInt(eventString(event.Args, types.EventAttrWaitedSeconds), 10, 64)
	return failure, failure.TaskID != "" && failure.ContainerID != "" && failure.Reason != ""
}

func eventString(args map[string]any, key string) string {
	value, _ := args[key].(string)
	return value
}
