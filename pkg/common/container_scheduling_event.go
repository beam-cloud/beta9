package common

type ContainerSchedulingFailure struct {
	TaskID       string
	ContainerID  string
	PoolSelector string
	Reason       string
}

func NewContainerSchedulingFailedEvent(failure ContainerSchedulingFailure) *Event {
	return &Event{
		Type: EventTypeContainerSchedulingFailed,
		Args: map[string]any{
			"task_id":       failure.TaskID,
			"container_id":  failure.ContainerID,
			"pool_selector": failure.PoolSelector,
			"reason":        failure.Reason,
		},
		LockAndDelete: true,
	}
}

func ParseContainerSchedulingFailure(event *Event) (ContainerSchedulingFailure, bool) {
	if event == nil || event.Type != EventTypeContainerSchedulingFailed {
		return ContainerSchedulingFailure{}, false
	}

	failure := ContainerSchedulingFailure{
		TaskID:       eventString(event.Args, "task_id"),
		ContainerID:  eventString(event.Args, "container_id"),
		PoolSelector: eventString(event.Args, "pool_selector"),
		Reason:       eventString(event.Args, "reason"),
	}
	return failure, failure.TaskID != "" && failure.ContainerID != "" && failure.Reason != ""
}

func eventString(args map[string]any, key string) string {
	value, _ := args[key].(string)
	return value
}
