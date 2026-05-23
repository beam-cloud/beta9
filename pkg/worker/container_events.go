package worker

import (
	"context"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

func (s *Worker) recordContainerEvent(ctx context.Context, request *types.ContainerRequest, event types.EventContainerEventSchema) {
	if s.eventRepo == nil {
		return
	}
	if request != nil {
		populateContainerEventFromRequest(&event, request)
	}
	if event.WorkerID == "" {
		event.WorkerID = s.workerId
	}
	s.eventRepo.PushContainerEvent(event)
}

func (s *Worker) recordContainerPhase(ctx context.Context, request *types.ContainerRequest, phase types.EventContainerPhaseSchema) {
	if s.eventRepo == nil {
		return
	}
	if request != nil {
		populateContainerPhaseFromRequest(&phase, request)
	}
	if phase.WorkerID == "" {
		phase.WorkerID = s.workerId
	}
	s.eventRepo.PushContainerPhaseEvent(phase)
}

func populateContainerEventFromRequest(event *types.EventContainerEventSchema, request *types.ContainerRequest) {
	if event.ContainerID == "" {
		event.ContainerID = request.ContainerId
	}
	if event.StubID == "" {
		event.StubID = request.StubId
	}
	if event.StubType == "" {
		event.StubType = string(request.Stub.Type.Kind())
	}
	if event.WorkspaceID == "" {
		event.WorkspaceID = request.WorkspaceId
	}
	if event.TaskID == "" {
		event.TaskID = taskIDFromEnv(request.Env)
	}
}

func populateContainerPhaseFromRequest(phase *types.EventContainerPhaseSchema, request *types.ContainerRequest) {
	if phase.ContainerID == "" {
		phase.ContainerID = request.ContainerId
	}
	if phase.StubID == "" {
		phase.StubID = request.StubId
	}
	if phase.StubType == "" {
		phase.StubType = string(request.Stub.Type.Kind())
	}
	if phase.WorkspaceID == "" {
		phase.WorkspaceID = request.WorkspaceId
	}
	if phase.TaskID == "" {
		phase.TaskID = taskIDFromEnv(request.Env)
	}
}

func taskIDFromEnv(env []string) string {
	for _, entry := range env {
		if value, ok := strings.CutPrefix(entry, "TASK_ID="); ok {
			return value
		}
	}
	return ""
}

func containerPhaseFromDuration(id types.ContainerPhaseID, request *types.ContainerRequest, startedAt time.Time, duration time.Duration, success bool, attrs map[string]string) types.EventContainerPhaseSchema {
	endTime := startedAt.Add(duration)
	phase := types.EventContainerPhaseSchema{
		ID:         id,
		StartTime:  startedAt.UTC(),
		EndTime:    endTime.UTC(),
		DurationMs: duration.Milliseconds(),
		Success:    &success,
		Attrs:      attrs,
	}
	populateContainerPhaseFromRequest(&phase, request)
	return phase
}

func (s *Worker) recordStartupPhase(ctx context.Context, request *types.ContainerRequest, id types.ContainerPhaseID, startedAt time.Time, success bool, attrs map[string]string) {
	s.recordContainerPhase(ctx, request, containerPhaseFromDuration(id, request, startedAt, time.Since(startedAt), success, attrs))
}

func recordContainerLogLine(eventRepo interface {
	PushContainerLogEvent(types.EventContainerLogSchema)
}, entry types.EventContainerLogSchema) {
	if eventRepo == nil {
		return
	}
	eventRepo.PushContainerLogEvent(entry)
}
