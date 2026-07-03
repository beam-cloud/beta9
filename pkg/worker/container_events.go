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
	if event.MachineID == "" {
		event.MachineID = s.machineID
	}
	s.eventRepo.PushContainerEvent(event)
}

func (s *Worker) recordContainerLifecycle(ctx context.Context, request *types.ContainerRequest, lifecycle types.EventContainerLifecycleSchema) {
	if s.eventRepo == nil {
		return
	}
	if request != nil {
		populateContainerLifecycleFromRequest(&lifecycle, request)
	}
	if lifecycle.WorkerID == "" {
		lifecycle.WorkerID = s.workerId
	}
	if lifecycle.MachineID == "" {
		lifecycle.MachineID = s.machineID
	}
	s.eventRepo.PushContainerLifecycleEvent(lifecycle)
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
	if event.AppID == "" {
		event.AppID = request.AppId
	}
	if event.MachineID == "" {
		event.MachineID = request.MachineId
	}
	if event.TaskID == "" {
		event.TaskID = taskIDFromEnv(request.Env)
	}
	if event.CPU == 0 {
		event.CPU = request.Cpu
	}
	if event.GPUCount == 0 {
		event.GPUCount = request.GpuCount
	}
}

func populateContainerLifecycleFromRequest(lifecycle *types.EventContainerLifecycleSchema, request *types.ContainerRequest) {
	if lifecycle.ContainerID == "" {
		lifecycle.ContainerID = request.ContainerId
	}
	if lifecycle.StubID == "" {
		lifecycle.StubID = request.StubId
	}
	if lifecycle.StubType == "" {
		lifecycle.StubType = string(request.Stub.Type.Kind())
	}
	if lifecycle.WorkspaceID == "" {
		lifecycle.WorkspaceID = request.WorkspaceId
	}
	if lifecycle.AppID == "" {
		lifecycle.AppID = request.AppId
	}
	if lifecycle.MachineID == "" {
		lifecycle.MachineID = request.MachineId
	}
	if lifecycle.TaskID == "" {
		lifecycle.TaskID = taskIDFromEnv(request.Env)
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

func containerLifecycleFromDuration(id types.ContainerLifecycleID, request *types.ContainerRequest, startedAt time.Time, duration time.Duration, success bool, attrs map[string]string) types.EventContainerLifecycleSchema {
	endTime := startedAt.Add(duration)
	lifecycle := types.EventContainerLifecycleSchema{
		ID:         id,
		StartTime:  startedAt.UTC(),
		EndTime:    endTime.UTC(),
		DurationMs: duration.Milliseconds(),
		Success:    &success,
		Attrs:      attrs,
	}
	populateContainerLifecycleFromRequest(&lifecycle, request)
	return lifecycle
}

func (s *Worker) recordStartupLifecycle(ctx context.Context, request *types.ContainerRequest, id types.ContainerLifecycleID, startedAt time.Time, success bool, attrs map[string]string) {
	s.recordContainerLifecycle(ctx, request, containerLifecycleFromDuration(id, request, startedAt, time.Since(startedAt), success, attrs))
}
