package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/rs/zerolog/log"
)

type eventSink interface {
	PushEvent(event cloudevents.Event) error
}

type eventReader interface {
	GetContainerEvents(ctx context.Context, containerID string, query types.EventQuery) (*types.ContainerEventsResponse, error)
}

type EventClientRepo struct {
	sinks  []eventSink
	reader eventReader
}

var ErrEventReadUnsupported = errors.New("event read unsupported")

type eventMetadata struct {
	ContainerID string
	WorkspaceID string
	TaskID      string
	StubID      string
	WorkerID    string
}

func NewEventClientRepo(config types.AppConfig) EventRepository {
	sinks := []eventSink{}

	var reader eventReader
	if s2Sink, err := NewS2EventRepository(config.Database.S2); err != nil {
		log.Warn().Err(err).Msg("s2 event repository unavailable")
	} else if s2Sink != nil {
		sinks = append(sinks, s2Sink)
		reader = s2Sink
	}

	return &EventClientRepo{sinks: sinks, reader: reader}
}

func (r *EventClientRepo) createEventObject(eventName string, schemaVersion string, data interface{}) (cloudevents.Event, error) {
	objectId, err := common.GenerateObjectId()
	if err != nil {
		return cloudevents.Event{}, err
	}

	metadata := eventMetadataFromData(data)
	event := cloudevents.NewEvent()
	event.SetID(objectId)
	event.SetSource("beta9-cluster")
	event.SetType(eventName)
	event.SetSpecVersion(schemaVersion)
	event.SetTime(time.Now())
	if err := event.SetData(cloudevents.ApplicationJSON, data); err != nil {
		return cloudevents.Event{}, err
	}
	setEventExtensions(&event, metadata)

	return event, nil
}

func (r *EventClientRepo) pushEvent(eventName string, schemaVersion string, data interface{}) {
	if len(r.sinks) == 0 {
		return
	}

	event, err := r.createEventObject(eventName, schemaVersion, data)
	if err != nil {
		log.Error().Err(err).Msg("failed to create event object")
		return
	}

	for _, sink := range r.sinks {
		if err := sink.PushEvent(event); err != nil {
			log.Debug().Err(err).Str("event_type", event.Type()).Msg("failed to push event")
		}
	}
}

func (r *EventClientRepo) GetContainerEvents(ctx context.Context, containerID string, query types.EventQuery) (*types.ContainerEventsResponse, error) {
	if r.reader == nil {
		return nil, ErrEventReadUnsupported
	}
	return r.reader.GetContainerEvents(ctx, containerID, query)
}

func (r *EventClientRepo) PushContainerPhaseEvent(phase types.EventContainerPhaseSchema) {
	def := types.ContainerPhaseDefinitionFor(phase.ID)
	if phase.Domain == "" {
		phase.Domain = def.Domain
	}
	if phase.ParentID == "" {
		phase.ParentID = def.ParentID
	}
	if phase.EndTime.IsZero() {
		phase.EndTime = time.Now().UTC()
	}
	if phase.DurationMs == 0 && !phase.StartTime.IsZero() && !phase.EndTime.Before(phase.StartTime) {
		phase.DurationMs = phase.EndTime.Sub(phase.StartTime).Milliseconds()
	}

	r.pushEvent(types.EventContainerPhase, types.EventContainerPhaseSchemaVersion, phase)
}

func (r *EventClientRepo) PushContainerEvent(event types.EventContainerEventSchema) {
	if event.Domain == "" {
		event.Domain = types.ContainerEventDomain(event.ID)
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	event.Reason = types.NormalizeEventReason(event.Reason)

	r.pushEvent(types.EventContainerEvent, types.EventContainerEventSchemaVersion, event)
}

func (r *EventClientRepo) PushContainerLogEvent(entry types.EventContainerLogSchema) {
	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now().UTC()
	}

	r.pushEvent(types.EventContainerLog, types.EventContainerLogSchemaVersion, entry)
}

func (r *EventClientRepo) PushContainerRequestedEvent(request *types.ContainerRequest) {
	r.pushEvent(
		types.EventContainerLifecycle,
		types.EventContainerStatusRequestedSchemaVersion,
		types.EventContainerStatusRequestedSchema{
			ContainerID: request.ContainerId,
			WorkspaceID: request.WorkspaceId,
			Request:     sanitizeContainerRequest(request),
			StubID:      request.StubId,
			StubType:    string(request.Stub.Type.Kind()),
			TaskID:      taskIDFromRequestEnv(request),
			Status:      types.EventContainerLifecycleRequested,
		},
	)
}

func (r *EventClientRepo) PushContainerScheduledEvent(containerID string, workerID string, request *types.ContainerRequest) {
	r.pushEvent(
		types.EventContainerLifecycle,
		types.EventContainerLifecycleSchemaVersion,
		types.EventContainerLifecycleSchema{
			ContainerID: containerID,
			WorkerID:    workerID,
			WorkspaceID: request.WorkspaceId,
			Request:     sanitizeContainerRequest(request),
			StubID:      request.StubId,
			StubType:    string(request.Stub.Type.Kind()),
			TaskID:      taskIDFromRequestEnv(request),
			Status:      types.EventContainerLifecycleScheduled,
		},
	)
}

func (r *EventClientRepo) PushContainerStartedEvent(containerID string, workerID string, request *types.ContainerRequest) {
	r.pushEvent(
		types.EventContainerLifecycle,
		types.EventContainerLifecycleSchemaVersion,
		types.EventContainerLifecycleSchema{
			ContainerID: containerID,
			WorkerID:    workerID,
			WorkspaceID: request.WorkspaceId,
			Request:     sanitizeContainerRequest(request),
			StubID:      request.StubId,
			StubType:    string(request.Stub.Type.Kind()),
			TaskID:      taskIDFromRequestEnv(request),
			Status:      types.EventContainerLifecycleStarted,
		},
	)
}

func (r *EventClientRepo) PushContainerStoppedEvent(containerID string, workerID string, request *types.ContainerRequest, exitCode int) {
	r.pushEvent(
		types.EventContainerLifecycle,
		types.EventContainerLifecycleSchemaVersion,
		types.EventContainerStoppedSchema{
			EventContainerLifecycleSchema: types.EventContainerLifecycleSchema{
				ContainerID: containerID,
				WorkerID:    workerID,
				WorkspaceID: request.WorkspaceId,
				Request:     sanitizeContainerRequest(request),
				StubID:      request.StubId,
				StubType:    string(request.Stub.Type.Kind()),
				TaskID:      taskIDFromRequestEnv(request),
				Status:      types.EventContainerLifecycleStopped,
			},
			ExitCode: exitCode,
		},
	)
}

func (r *EventClientRepo) PushContainerOOMEvent(containerID string, workerID string, request *types.ContainerRequest) {
	r.pushEvent(
		types.EventContainerLifecycle,
		types.EventContainerLifecycleSchemaVersion,
		types.EventContainerLifecycleSchema{
			ContainerID: containerID,
			WorkerID:    workerID,
			WorkspaceID: request.WorkspaceId,
			StubID:      request.StubId,
			StubType:    string(request.Stub.Type.Kind()),
			TaskID:      taskIDFromRequestEnv(request),
			Request:     sanitizeContainerRequest(request),
			Status:      types.EventContainerLifecycleOOM,
		},
	)
}

func (r *EventClientRepo) PushWorkerStartedEvent(workerID string) {
	r.pushEvent(
		types.EventWorkerLifecycle,
		types.EventWorkerLifecycleSchemaVersion,
		types.EventWorkerLifecycleSchema{
			WorkerID: workerID,
			Status:   types.EventWorkerLifecycleStarted,
		},
	)
}

func (r *EventClientRepo) PushWorkerStoppedEvent(workerID string) {
	r.pushEvent(
		types.EventWorkerLifecycle,
		types.EventWorkerLifecycleSchemaVersion,
		types.EventWorkerLifecycleSchema{
			WorkerID: workerID,
			Status:   types.EventWorkerLifecycleStopped,
		},
	)
}

func (r *EventClientRepo) PushWorkerDeletedEvent(workerID, machineID, poolName string, reason types.DeletedWorkerReason) {
	r.pushEvent(
		types.EventWorkerLifecycle,
		types.EventWorkerLifecycleSchemaVersion,
		types.EventWorkerLifecycleSchema{
			WorkerID:  workerID,
			MachineID: machineID,
			PoolName:  poolName,
			Reason:    reason,
			Status:    types.EventWorkerLifecycleDeleted,
		},
	)
}

func (r *EventClientRepo) PushContainerResourceMetricsEvent(workerID string, request *types.ContainerRequest, metrics types.EventContainerMetricsData) {
	r.pushEvent(
		types.EventContainerMetrics,
		types.EventContainerMetricsSchemaVersion,
		types.EventContainerMetricsSchema{
			WorkerID:         workerID,
			ContainerID:      request.ContainerId,
			WorkspaceID:      request.WorkspaceId,
			StubID:           request.StubId,
			StubType:         string(request.Stub.Type.Kind()),
			ContainerMetrics: metrics,
		},
	)
}

func (r *EventClientRepo) PushDeployStubEvent(workspaceId string, stub *types.Stub) {
	r.pushEvent(
		types.EventStubDeploy,
		types.EventStubSchemaVersion,
		types.EventStubSchema{
			ID:          stub.ExternalId,
			StubType:    stub.Type,
			StubConfig:  stub.Config,
			WorkspaceID: workspaceId,
		},
	)
}

func (r *EventClientRepo) PushServeStubEvent(workspaceId string, stub *types.Stub) {
	r.pushEvent(
		types.EventStubServe,
		types.EventStubSchemaVersion,
		types.EventStubSchema{
			ID:          stub.ExternalId,
			StubType:    stub.Type,
			StubConfig:  stub.Config,
			WorkspaceID: workspaceId,
		},
	)
}

func (r *EventClientRepo) PushRunStubEvent(workspaceId string, stub *types.Stub) {
	r.pushEvent(
		types.EventStubRun,
		types.EventStubSchemaVersion,
		types.EventStubSchema{
			ID:          stub.ExternalId,
			StubType:    stub.Type,
			StubConfig:  stub.Config,
			WorkspaceID: workspaceId,
		},
	)
}

func (r *EventClientRepo) PushCloneStubEvent(workspaceId string, stub *types.Stub, parentStub *types.Stub) {
	r.pushEvent(
		types.EventStubClone,
		types.EventStubSchemaVersion,
		types.EventStubSchema{
			ID:           stub.ExternalId,
			StubType:     stub.Type,
			StubConfig:   stub.Config,
			WorkspaceID:  workspaceId,
			ParentStubID: parentStub.ExternalId,
		},
	)
}

func (r *EventClientRepo) PushTaskUpdatedEvent(task *types.TaskWithRelated) {
	event := types.EventTaskSchema{
		ID:          task.ExternalId,
		Status:      task.Status,
		ContainerID: task.ContainerId,
		CreatedAt:   task.CreatedAt.Time,
		StubID:      task.Stub.ExternalId,
		WorkspaceID: task.Workspace.ExternalId,
		AppID:       task.App.ExternalId,
	}

	if task.StartedAt.Valid {
		event.StartedAt = &task.StartedAt.Time
	}

	if task.EndedAt.Valid {
		event.EndedAt = &task.EndedAt.Time
	}

	if task.ExternalWorkspace != nil && task.ExternalWorkspace.ExternalId != nil {
		event.ExternalWorkspaceID = *task.ExternalWorkspace.ExternalId
	}

	r.pushEvent(
		types.EventTaskUpdated,
		types.EventTaskSchemaVersion,
		event,
	)
}

func (r *EventClientRepo) PushTaskCreatedEvent(task *types.TaskWithRelated) {
	event := types.EventTaskSchema{
		ID:          task.ExternalId,
		Status:      task.Status,
		ContainerID: task.ContainerId,
		CreatedAt:   task.CreatedAt.Time,
		StubID:      task.Stub.ExternalId,
		WorkspaceID: task.Workspace.ExternalId,
		AppID:       task.App.ExternalId,
	}

	if task.StartedAt.Valid {
		event.StartedAt = &task.StartedAt.Time
	}

	if task.EndedAt.Valid {
		event.EndedAt = &task.EndedAt.Time
	}

	if task.ExternalWorkspace != nil && task.ExternalWorkspace.ExternalId != nil {
		event.ExternalWorkspaceID = *task.ExternalWorkspace.ExternalId
	}

	r.pushEvent(
		types.EventTaskCreated,
		types.EventTaskSchemaVersion,
		event,
	)
}

func (r *EventClientRepo) PushStubStateUnhealthy(workspaceId string, stubId string, currentState string, previousState string, reason string, failedContainers []string) {
	r.pushEvent(
		fmt.Sprintf("stub.state.%s", strings.ToLower(currentState)),
		types.EventStubStateSchemaVersion,
		types.EventStubStateSchema{
			ID:               stubId,
			WorkspaceID:      workspaceId,
			State:            currentState,
			Reason:           reason,
			PreviousState:    previousState,
			FailedContainers: failedContainers,
		},
	)
}

func (r *EventClientRepo) PushWorkerPoolDegradedEvent(poolName string, reasons []string, poolState *types.WorkerPoolState) {
	r.pushEvent(
		types.EventWorkerPoolDegraded,
		types.EventWorkerPoolStateSchemaVersion,
		types.EventWorkerPoolStateSchema{
			PoolName:  poolName,
			Reasons:   reasons,
			Status:    string(types.WorkerPoolStatusDegraded),
			PoolState: poolState,
		},
	)
}

func (r *EventClientRepo) PushWorkerPoolHealthyEvent(poolName string, poolState *types.WorkerPoolState) {
	r.pushEvent(
		types.EventWorkerPoolHealthy,
		types.EventWorkerPoolStateSchemaVersion,
		types.EventWorkerPoolStateSchema{
			PoolName:  poolName,
			Status:    string(types.WorkerPoolStatusHealthy),
			PoolState: poolState,
		},
	)
}

func (r *EventClientRepo) PushGatewayEndpointCalledEvent(method, path, workspaceID string, statusCode int, userAgent, remoteIP, requestID, contentType, accept, errorMessage string) {
	r.pushEvent(
		types.EventGatewayEndpointCalled,
		types.EventGatewayEndpointSchemaVersion,
		types.EventGatewayEndpointSchema{
			Method:       method,
			Path:         path,
			WorkspaceID:  workspaceID,
			StatusCode:   statusCode,
			UserAgent:    userAgent,
			RemoteIP:     remoteIP,
			RequestID:    requestID,
			ContentType:  contentType,
			Accept:       accept,
			ErrorMessage: errorMessage,
		},
	)
}

func sanitizeContainerRequest(request *types.ContainerRequest) types.ContainerRequest {
	requestCopy := *request
	requestCopy.Env = nil
	requestCopy.EntryPoint = nil
	requestCopy.Stub = types.StubWithRelated{}
	requestCopy.Workspace = types.Workspace{}
	requestCopy.Mounts = nil
	requestCopy.PoolSelector = ""
	requestCopy.Checkpoint = nil
	requestCopy.ConfigPath = ""
	return requestCopy
}

func taskIDFromRequestEnv(request *types.ContainerRequest) string {
	if request == nil {
		return ""
	}
	for _, entry := range request.Env {
		if value, ok := strings.CutPrefix(entry, "TASK_ID="); ok {
			return value
		}
	}
	return ""
}

func eventMetadataFromData(data interface{}) eventMetadata {
	switch d := data.(type) {
	case types.EventContainerStatusRequestedSchema:
		workspaceID := d.WorkspaceID
		if workspaceID == "" {
			workspaceID = d.Request.WorkspaceId
		}
		return eventMetadata{ContainerID: d.ContainerID, StubID: d.StubID, TaskID: d.TaskID, WorkspaceID: workspaceID}
	case types.EventContainerLifecycleSchema:
		workspaceID := d.WorkspaceID
		if workspaceID == "" {
			workspaceID = d.Request.WorkspaceId
		}
		return eventMetadata{ContainerID: d.ContainerID, StubID: d.StubID, TaskID: d.TaskID, WorkerID: d.WorkerID, WorkspaceID: workspaceID}
	case types.EventContainerStoppedSchema:
		workspaceID := d.WorkspaceID
		if workspaceID == "" {
			workspaceID = d.Request.WorkspaceId
		}
		return eventMetadata{ContainerID: d.ContainerID, StubID: d.StubID, TaskID: d.TaskID, WorkerID: d.WorkerID, WorkspaceID: workspaceID}
	case types.EventContainerMetricsSchema:
		return eventMetadata{ContainerID: d.ContainerID, StubID: d.StubID, WorkerID: d.WorkerID, WorkspaceID: d.WorkspaceID}
	case types.EventContainerPhaseSchema:
		return eventMetadata{ContainerID: d.ContainerID, StubID: d.StubID, TaskID: d.TaskID, WorkerID: d.WorkerID, WorkspaceID: d.WorkspaceID}
	case types.EventContainerEventSchema:
		return eventMetadata{ContainerID: d.ContainerID, StubID: d.StubID, TaskID: d.TaskID, WorkerID: d.WorkerID, WorkspaceID: d.WorkspaceID}
	case types.EventContainerLogSchema:
		return eventMetadata{ContainerID: d.ContainerID, StubID: d.StubID, TaskID: d.TaskID, WorkerID: d.WorkerID, WorkspaceID: d.WorkspaceID}
	case types.EventTaskSchema:
		return eventMetadata{ContainerID: d.ContainerID, StubID: d.StubID, TaskID: d.ID, WorkspaceID: d.WorkspaceID}
	case types.EventStubSchema:
		return eventMetadata{StubID: d.ID, WorkspaceID: d.WorkspaceID}
	default:
		return eventMetadata{}
	}
}

func setEventExtensions(event *cloudevents.Event, metadata eventMetadata) {
	if metadata.ContainerID != "" {
		event.SetExtension("containerid", metadata.ContainerID)
	}
	if metadata.WorkspaceID != "" {
		event.SetExtension("workspaceid", metadata.WorkspaceID)
	}
	if metadata.TaskID != "" {
		event.SetExtension("taskid", metadata.TaskID)
	}
	if metadata.StubID != "" {
		event.SetExtension("stubid", metadata.StubID)
	}
	if metadata.WorkerID != "" {
		event.SetExtension("workerid", metadata.WorkerID)
	}
}
