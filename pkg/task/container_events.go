package task

import (
	"context"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

type ContainerEventSink interface {
	PushContainerPhaseEvent(phase types.EventContainerPhaseSchema)
	PushContainerEvent(event types.EventContainerEventSchema)
}

func PushContainerTaskEvent(eventRepo ContainerEventSink, task *types.TaskWithRelated, eventID types.ContainerEventID, source string, message string, attrs map[string]string) {
	PushContainerTaskEventWithReason(eventRepo, task, eventID, "", source, message, attrs)
}

func PushContainerTaskEventWithReason(eventRepo ContainerEventSink, task *types.TaskWithRelated, eventID types.ContainerEventID, reason string, source string, message string, attrs map[string]string) {
	if eventRepo == nil || task == nil || task.ContainerId == "" {
		return
	}

	eventRepo.PushContainerEvent(types.EventContainerEventSchema{
		ID:          eventID,
		ContainerID: task.ContainerId,
		StubID:      task.Stub.ExternalId,
		StubType:    string(task.Stub.Type.Kind()),
		TaskID:      task.ExternalId,
		WorkspaceID: task.Workspace.ExternalId,
		Reason:      reason,
		Source:      source,
		Message:     message,
		Attrs:       copyEventAttrs(attrs),
	})
}

func PushContainerTaskPhaseSincePhase(ctx context.Context, rdb *common.RedisClient, eventRepo ContainerEventSink, task *types.TaskWithRelated, spanID types.ContainerPhaseID, phase string, end time.Time, source string, attrs map[string]string) {
	if eventRepo == nil || task == nil || task.ContainerId == "" {
		return
	}

	start, ok, err := NewPhaseMetrics(rdb).Timestamp(ctx, task.Workspace.Name, task.ExternalId, phase)
	if err != nil || !ok || end.Before(start) {
		return
	}

	PushContainerTaskPhase(eventRepo, task, spanID, start, end, source, attrs)
}

func PushContainerTaskPhase(eventRepo ContainerEventSink, task *types.TaskWithRelated, spanID types.ContainerPhaseID, start time.Time, end time.Time, source string, attrs map[string]string) {
	if eventRepo == nil || task == nil || task.ContainerId == "" || end.Before(start) {
		return
	}

	success := true
	phaseAttrs := copyEventAttrs(attrs)
	phaseAttrs["source"] = source
	eventRepo.PushContainerPhaseEvent(types.EventContainerPhaseSchema{
		ID:          spanID,
		StartTime:   start.UTC(),
		EndTime:     end.UTC(),
		DurationMs:  end.Sub(start).Milliseconds(),
		ContainerID: task.ContainerId,
		StubID:      task.Stub.ExternalId,
		StubType:    string(task.Stub.Type.Kind()),
		TaskID:      task.ExternalId,
		WorkspaceID: task.Workspace.ExternalId,
		Success:     &success,
		Attrs:       phaseAttrs,
	})
}

func copyEventAttrs(attrs map[string]string) map[string]string {
	copied := map[string]string{}
	for key, value := range attrs {
		copied[key] = value
	}
	return copied
}
