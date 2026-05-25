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
	"github.com/redis/go-redis/v9"
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

func (r *EventClientRepo) PushContainerLifecycleEvent(lifecycle types.EventContainerLifecycleSchema) {
	def := types.ContainerLifecycleDefinitionFor(lifecycle.ID)
	if lifecycle.Domain == "" {
		lifecycle.Domain = def.Domain
	}
	if lifecycle.ParentID == "" {
		lifecycle.ParentID = def.ParentID
	}
	if lifecycle.EndTime.IsZero() {
		lifecycle.EndTime = time.Now().UTC()
	}
	if lifecycle.DurationMs == 0 && !lifecycle.StartTime.IsZero() && !lifecycle.EndTime.Before(lifecycle.StartTime) {
		lifecycle.DurationMs = lifecycle.EndTime.Sub(lifecycle.StartTime).Milliseconds()
	}
	if !lifecycle.StartTime.IsZero() && !lifecycle.EndTime.Before(lifecycle.StartTime) {
		if lifecycle.Attrs == nil {
			lifecycle.Attrs = map[string]string{}
		}
		if _, ok := lifecycle.Attrs[types.EventAttrDurationUs]; !ok {
			lifecycle.Attrs[types.EventAttrDurationUs] = fmt.Sprintf("%d", lifecycle.EndTime.Sub(lifecycle.StartTime).Microseconds())
		}
	}

	r.pushEvent(types.EventContainerLifecycle, types.EventContainerLifecycleSchemaVersion, lifecycle)
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

func (r *EventClientRepo) PushContainerRequestEvent(workerID string, request *types.ContainerRequest, eventID types.ContainerEventID, opts types.ContainerEventOptions) {
	if request == nil || request.ContainerId == "" {
		return
	}

	taskID := firstNonEmpty(opts.TaskID, taskIDFromRequestEnv(request))
	r.PushContainerEvent(types.EventContainerEventSchema{
		ID:          eventID,
		ContainerID: request.ContainerId,
		StubID:      request.StubId,
		StubType:    string(request.Stub.Type.Kind()),
		TaskID:      taskID,
		WorkspaceID: request.WorkspaceId,
		WorkerID:    workerID,
		Reason:      opts.Reason,
		Source:      opts.Source.String(),
		Message:     opts.Message.String(),
		Attrs:       copyEventAttrs(opts.Attrs),
	})
}

func (r *EventClientRepo) PushContainerRequestLifecycle(workerID string, request *types.ContainerRequest, lifecycleID types.ContainerLifecycleID, startedAt time.Time, duration time.Duration, success bool, opts types.ContainerLifecycleOptions) {
	if request == nil || request.ContainerId == "" || startedAt.IsZero() || duration < 0 {
		return
	}

	endTime := startedAt.Add(duration)
	attrs := copyEventAttrs(opts.Attrs)
	if opts.Source != "" {
		attrs[types.EventAttrSource] = opts.Source.String()
	}
	r.PushContainerLifecycleEvent(types.EventContainerLifecycleSchema{
		ID:          lifecycleID,
		StartTime:   startedAt.UTC(),
		EndTime:     endTime.UTC(),
		DurationMs:  duration.Milliseconds(),
		ContainerID: request.ContainerId,
		StubID:      request.StubId,
		StubType:    string(request.Stub.Type.Kind()),
		TaskID:      firstNonEmpty(opts.TaskID, taskIDFromRequestEnv(request)),
		WorkspaceID: request.WorkspaceId,
		WorkerID:    workerID,
		Success:     &success,
		Source:      opts.Source.String(),
		Attrs:       attrs,
	})
}

func (r *EventClientRepo) PushContainerTaskEvent(task *types.TaskWithRelated, eventID types.ContainerEventID, opts types.ContainerEventOptions) {
	if task == nil || task.ContainerId == "" {
		return
	}

	r.PushContainerEvent(types.EventContainerEventSchema{
		ID:          eventID,
		ContainerID: task.ContainerId,
		StubID:      task.Stub.ExternalId,
		StubType:    string(task.Stub.Type.Kind()),
		TaskID:      task.ExternalId,
		WorkspaceID: task.Workspace.ExternalId,
		Reason:      opts.Reason,
		Source:      opts.Source.String(),
		Message:     opts.Message.String(),
		Attrs:       copyEventAttrs(opts.Attrs),
	})
}

func (r *EventClientRepo) PushContainerFunctionTaskEvent(workspaceID string, task types.TaskInterface, eventID types.ContainerEventID, opts types.ContainerEventOptions) {
	if task == nil || workspaceID == "" {
		return
	}

	metadata := task.Metadata()
	if metadata.ContainerId == "" {
		return
	}

	r.PushContainerEvent(types.EventContainerEventSchema{
		ID:          eventID,
		ContainerID: metadata.ContainerId,
		StubID:      metadata.StubId,
		StubType:    string(types.StubTypeFunction),
		TaskID:      metadata.TaskId,
		WorkspaceID: workspaceID,
		Reason:      opts.Reason,
		Source:      opts.Source.String(),
		Message:     opts.Message.String(),
		Attrs:       copyEventAttrs(opts.Attrs),
	})
}

func (r *EventClientRepo) PushContainerTaskLifecycle(task *types.TaskWithRelated, lifecycleID types.ContainerLifecycleID, start time.Time, end time.Time, success bool, opts types.ContainerLifecycleOptions) {
	if task == nil || task.ContainerId == "" || start.IsZero() || end.Before(start) {
		return
	}

	attrs := copyEventAttrs(opts.Attrs)
	if opts.Source != "" {
		attrs[types.EventAttrSource] = opts.Source.String()
	}
	r.PushContainerLifecycleEvent(types.EventContainerLifecycleSchema{
		ID:          lifecycleID,
		StartTime:   start.UTC(),
		EndTime:     end.UTC(),
		DurationMs:  end.Sub(start).Milliseconds(),
		ContainerID: task.ContainerId,
		StubID:      task.Stub.ExternalId,
		StubType:    string(task.Stub.Type.Kind()),
		TaskID:      firstNonEmpty(opts.TaskID, task.ExternalId),
		WorkspaceID: task.Workspace.ExternalId,
		Success:     &success,
		Source:      opts.Source.String(),
		Attrs:       attrs,
	})
}

func (r *EventClientRepo) PushContainerFunctionTaskLifecycle(workspaceID string, task types.TaskInterface, lifecycleID types.ContainerLifecycleID, start time.Time, end time.Time, success bool, opts types.ContainerLifecycleOptions) {
	if task == nil || workspaceID == "" || start.IsZero() || end.Before(start) {
		return
	}

	metadata := task.Metadata()
	if metadata.ContainerId == "" {
		return
	}

	attrs := copyEventAttrs(opts.Attrs)
	if opts.Source != "" {
		attrs[types.EventAttrSource] = opts.Source.String()
	}
	r.PushContainerLifecycleEvent(types.EventContainerLifecycleSchema{
		ID:          lifecycleID,
		StartTime:   start.UTC(),
		EndTime:     end.UTC(),
		DurationMs:  end.Sub(start).Milliseconds(),
		ContainerID: metadata.ContainerId,
		StubID:      metadata.StubId,
		StubType:    string(types.StubTypeFunction),
		TaskID:      firstNonEmpty(opts.TaskID, metadata.TaskId),
		WorkspaceID: workspaceID,
		Success:     &success,
		Source:      opts.Source.String(),
		Attrs:       attrs,
	})
}

func (r *EventClientRepo) PushContainerTaskLifecycleSince(ctx context.Context, rdb *common.RedisClient, task *types.TaskWithRelated, lifecycleID types.ContainerLifecycleID, sincePhase string, end time.Time, success bool, opts types.ContainerLifecycleOptions) {
	if task == nil || task.ContainerId == "" {
		return
	}

	start, ok, err := taskPhaseTimestamp(ctx, rdb, task.Workspace.Name, task.ExternalId, sincePhase)
	if err != nil || !ok || end.Before(start) {
		return
	}

	r.PushContainerTaskLifecycle(task, lifecycleID, start, end, success, opts)
}

func (r *EventClientRepo) PushContainerRequestLogLine(workerID string, request *types.ContainerRequest, taskID string, stream string, line string) {
	if request == nil || request.ContainerId == "" || line == "" {
		return
	}

	r.PushContainerLogEvent(types.EventContainerLogSchema{
		Timestamp:   time.Now().UTC(),
		ContainerID: request.ContainerId,
		StubID:      request.StubId,
		StubType:    string(request.Stub.Type.Kind()),
		TaskID:      firstNonEmpty(taskID, taskIDFromRequestEnv(request)),
		WorkspaceID: request.WorkspaceId,
		WorkerID:    workerID,
		Stream:      stream,
		Line:        line,
	})
}

func (r *EventClientRepo) PushContainerRunnerEvent(workerID string, request *types.ContainerRequest, event *types.ContainerRunnerEvent) {
	if request == nil || request.ContainerId == "" || event == nil || event.ID == "" {
		return
	}

	if event.Attrs == nil {
		event.Attrs = map[string]string{}
	}

	switch event.Type {
	case types.RunnerEventTypeLifecycle:
		startTime, ok := parseRunnerEventTime(event.StartTime)
		if !ok {
			return
		}
		endTime, ok := parseRunnerEventTime(event.EndTime)
		if !ok || endTime.Before(startTime) {
			return
		}
		durationMs := event.DurationMs
		if durationMs == 0 {
			durationMs = endTime.Sub(startTime).Milliseconds()
		}
		success := true
		if event.Success != nil {
			success = *event.Success
		}
		r.PushContainerLifecycleEvent(types.EventContainerLifecycleSchema{
			ID:          types.ContainerLifecycleID(event.ID),
			StartTime:   startTime,
			EndTime:     endTime,
			DurationMs:  durationMs,
			ContainerID: firstNonEmpty(event.ContainerID, request.ContainerId),
			StubID:      firstNonEmpty(event.StubID, request.StubId),
			StubType:    firstNonEmpty(event.StubType, string(request.Stub.Type.Kind())),
			TaskID:      firstNonEmpty(event.TaskID, taskIDFromRequestEnv(request)),
			WorkspaceID: request.WorkspaceId,
			WorkerID:    workerID,
			Success:     &success,
			Source:      types.EventSourceRunnerStdout.String(),
			Attrs:       copyEventAttrs(event.Attrs),
		})
	case types.RunnerEventTypeEvent:
		timestamp, ok := parseRunnerEventTime(event.Timestamp)
		if !ok {
			return
		}
		domain := types.ContainerEventDomain(types.ContainerEventID(event.ID))
		if domain == "" {
			domain = types.EventDomainRunner
		}
		r.PushContainerEvent(types.EventContainerEventSchema{
			ID:          types.ContainerEventID(event.ID),
			Domain:      domain,
			Timestamp:   timestamp,
			ContainerID: firstNonEmpty(event.ContainerID, request.ContainerId),
			StubID:      firstNonEmpty(event.StubID, request.StubId),
			StubType:    firstNonEmpty(event.StubType, string(request.Stub.Type.Kind())),
			TaskID:      firstNonEmpty(event.TaskID, taskIDFromRequestEnv(request)),
			WorkspaceID: request.WorkspaceId,
			WorkerID:    workerID,
			Source:      types.EventSourceRunnerStdout.String(),
			Message:     event.Message,
			Attrs:       copyEventAttrs(event.Attrs),
		})
	}
}

func (r *EventClientRepo) PushFunctionResultLoaded(workspaceID string, task types.TaskInterface, exitCode int32, byteCount int) {
	r.PushContainerFunctionTaskEvent(workspaceID, task, types.ContainerEventResultLoadedByGateway, types.ContainerEventOptions{
		Source:  types.EventSourceGatewayFunctionStream,
		Message: types.EventMessageFunctionResultLoadedByGateway,
		Attrs:   resultEventAttrs(exitCode, byteCount),
	})
}

func (r *EventClientRepo) PushFunctionResultSent(workspaceID string, task types.TaskInterface, exitCode int32, byteCount int) {
	r.PushContainerFunctionTaskEvent(workspaceID, task, types.ContainerEventResultSentToClient, types.ContainerEventOptions{
		Source:  types.EventSourceGatewayFunctionStream,
		Message: types.EventMessageFunctionResultSentToClient,
		Attrs:   resultEventAttrs(exitCode, byteCount),
	})
}

func (r *EventClientRepo) PushFunctionResultDelivery(workspaceID string, task types.TaskInterface, startedAt time.Time, exitCode int32, byteCount int) {
	r.PushContainerFunctionTaskLifecycle(workspaceID, task, types.ContainerLifecycleResultDelivery, startedAt, time.Now(), true, types.ContainerLifecycleOptions{
		Source: types.EventSourceGatewayFunctionStream,
		Attrs:  resultEventAttrs(exitCode, byteCount),
	})
}

func (r *EventClientRepo) PushFunctionStreamCancelRequested(workspaceID string, task types.TaskInterface) {
	r.PushContainerFunctionTaskEvent(workspaceID, task, types.ContainerEventTaskCancelRequested, types.ContainerEventOptions{
		Source:  types.EventSourceGatewayFunctionStream,
		Message: types.EventMessageFunctionStreamCancelRequested,
		Reason:  string(types.TaskRequestCancelled),
		Attrs:   map[string]string{types.EventAttrCause: types.EventCauseClientContextDone},
	})
}

func (r *EventClientRepo) PushFunctionStreamCancelApplied(workspaceID string, task types.TaskInterface) {
	r.PushContainerFunctionTaskEvent(workspaceID, task, types.ContainerEventTaskCancelApplied, types.ContainerEventOptions{
		Source:  types.EventSourceGatewayFunctionStream,
		Message: types.EventMessageFunctionStreamCancelApplied,
		Reason:  string(types.TaskRequestCancelled),
		Attrs:   map[string]string{types.EventAttrReason: string(types.TaskRequestCancelled)},
	})
}

func (r *EventClientRepo) PushFunctionGetArgs(ctx context.Context, rdb *common.RedisClient, task *types.TaskWithRelated, at time.Time, byteCount int) {
	r.PushContainerTaskLifecycleSince(ctx, rdb, task, types.ContainerLifecycleRunnerStartToGetArgs, types.FunctionLifecycleCheckpointStartTask, at, true, types.ContainerLifecycleOptions{
		Source: types.EventSourceGatewayFunctionGetArgs,
		Attrs:  bytesEventAttrs(byteCount),
	})
	r.PushContainerTaskEvent(task, types.ContainerEventRunnerGetArgs, types.ContainerEventOptions{
		Source:  types.EventSourceGatewayFunctionGetArgs,
		Message: types.EventMessageRunnerLoadedFunctionArgs,
		Attrs:   bytesEventAttrs(byteCount),
	})
}

func (r *EventClientRepo) PushFunctionSetResult(ctx context.Context, rdb *common.RedisClient, task *types.TaskWithRelated, at time.Time, byteCount int) {
	r.PushContainerTaskLifecycleSince(ctx, rdb, task, types.ContainerLifecycleRunnerGetArgsToSetResult, types.FunctionLifecycleCheckpointGetArgs, at, true, types.ContainerLifecycleOptions{
		Source: types.EventSourceGatewayFunctionSetResult,
		Attrs:  bytesEventAttrs(byteCount),
	})
	r.PushContainerTaskLifecycleSince(ctx, rdb, task, types.ContainerLifecycleRunnerStartToSetResult, types.FunctionLifecycleCheckpointStartTask, at, true, types.ContainerLifecycleOptions{
		Source: types.EventSourceGatewayFunctionSetResult,
		Attrs:  bytesEventAttrs(byteCount),
	})
	r.PushContainerTaskEvent(task, types.ContainerEventResultSetResult, types.ContainerEventOptions{
		Source:  types.EventSourceGatewayFunctionSetResult,
		Message: types.EventMessageFunctionResultStored,
		Attrs:   bytesEventAttrs(byteCount),
	})
}

func (r *EventClientRepo) PushTaskStartEvents(ctx context.Context, rdb *common.RedisClient, task *types.TaskWithRelated, containerID string, startedAt time.Time) {
	r.PushContainerTaskLifecycleSince(ctx, rdb, task, types.ContainerLifecycleContainerRequestToStartTask, types.FunctionLifecycleCheckpointContainerRequestReady, startedAt, true, types.ContainerLifecycleOptions{
		Source: types.EventSourceGatewayStartTask,
	})
	r.PushContainerTaskEvent(task, types.ContainerEventRunnerStartTask, types.ContainerEventOptions{
		Source:  types.EventSourceGatewayStartTask,
		Message: types.EventMessageRunnerCalledStartTask,
		Attrs:   map[string]string{types.EventAttrContainerID: containerID},
	})
}

func (r *EventClientRepo) PushTaskEndEvents(ctx context.Context, rdb *common.RedisClient, task *types.TaskWithRelated, endedAt time.Time) {
	if task == nil {
		return
	}
	attrs := map[string]string{types.EventAttrStatus: string(task.Status)}
	r.PushContainerTaskLifecycleSince(ctx, rdb, task, types.ContainerLifecycleResultSetToEndTask, types.FunctionLifecycleCheckpointSetResult, endedAt, true, types.ContainerLifecycleOptions{
		Source: types.EventSourceGatewayEndTask,
		Attrs:  attrs,
	})
	r.PushContainerTaskLifecycleSince(ctx, rdb, task, types.ContainerLifecycleRunnerStartToEndTask, types.FunctionLifecycleCheckpointStartTask, endedAt, true, types.ContainerLifecycleOptions{
		Source: types.EventSourceGatewayEndTask,
		Attrs:  attrs,
	})
}

func (r *EventClientRepo) PushTaskEndPersisted(task *types.TaskWithRelated) {
	if task == nil {
		return
	}
	r.PushContainerTaskEvent(task, types.ContainerEventResultEndTask, types.ContainerEventOptions{
		Source:  types.EventSourceGatewayEndTask,
		Message: types.EventMessageTaskEndStatePersisted,
		Attrs:   map[string]string{types.EventAttrStatus: string(task.Status)},
	})
}

func (r *EventClientRepo) PushContainerRunningToStartTask(task *types.TaskWithRelated, runningAt time.Time, startedAt time.Time, status types.ContainerStatus) {
	r.PushContainerTaskLifecycle(task, types.ContainerLifecycleContainerRunningToStartTask, runningAt, startedAt, true, types.ContainerLifecycleOptions{
		Source: types.EventSourceGatewayStartTask,
		Attrs:  map[string]string{types.EventAttrContainerStatus: string(status)},
	})
}

func (r *EventClientRepo) PushTaskCancelRequested(task *types.TaskWithRelated, source types.EventSource, message types.EventMessage) {
	r.PushContainerTaskEvent(task, types.ContainerEventTaskCancelRequested, types.ContainerEventOptions{
		Source:  source,
		Message: message,
		Reason:  string(types.StopContainerReasonUser),
	})
}

func (r *EventClientRepo) PushTaskCancelApplied(task *types.TaskWithRelated, source types.EventSource, message types.EventMessage) {
	r.PushContainerTaskEvent(task, types.ContainerEventTaskCancelApplied, types.ContainerEventOptions{
		Source:  source,
		Message: message,
		Reason:  string(types.StopContainerReasonUser),
	})
}

func (r *EventClientRepo) PushContainerLogFlushCompleted(workerID string, request *types.ContainerRequest) {
	r.PushContainerRequestEvent(workerID, request, types.ContainerEventLogsFlushCompleted, types.ContainerEventOptions{
		Source:  types.EventSourceWorkerLogger,
		Message: types.EventMessageLogCaptureFlushed,
	})
}

func (r *EventClientRepo) PushContainerLogDropped(workerID string, request *types.ContainerRequest, message types.EventMessage, taskID string) {
	r.PushContainerRequestEvent(workerID, request, types.ContainerEventLogsDropped, types.ContainerEventOptions{
		Source:  types.EventSourceWorkerLogger,
		Message: message,
		TaskID:  taskID,
	})
}

func (r *EventClientRepo) PushContainerLogFirstByte(workerID string, request *types.ContainerRequest, taskID string) {
	r.PushContainerRequestEvent(workerID, request, types.ContainerEventLogsFirstByte, types.ContainerEventOptions{
		Source:  types.EventSourceWorkerLogger,
		Message: types.EventMessageLogCaptureReceivedFirstByte,
		TaskID:  taskID,
	})
}

func (r *EventClientRepo) PushContainerLogLastByte(workerID string, request *types.ContainerRequest) {
	r.PushContainerRequestEvent(workerID, request, types.ContainerEventLogsLastByte, types.ContainerEventOptions{
		Source:  types.EventSourceWorkerLogger,
		Message: types.EventMessageLogCaptureReceivedFinalByte,
	})
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

func taskPhaseTimestamp(ctx context.Context, rdb *common.RedisClient, workspaceName, taskID, phase string) (time.Time, bool, error) {
	if rdb == nil || workspaceName == "" || taskID == "" || phase == "" {
		return time.Time{}, false, nil
	}

	value, err := rdb.Get(ctx, common.RedisKeys.TaskPhase(workspaceName, taskID, phase)).Int64()
	if err == redis.Nil {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("failed to get task phase: %w", err)
	}
	if value <= 0 {
		return time.Time{}, false, nil
	}

	return time.UnixMilli(value), true, nil
}

func copyEventAttrs(attrs map[string]string) map[string]string {
	copied := map[string]string{}
	for key, value := range attrs {
		copied[key] = value
	}
	return copied
}

func bytesEventAttrs(byteCount int) map[string]string {
	return map[string]string{types.EventAttrBytes: fmt.Sprintf("%d", byteCount)}
}

func resultEventAttrs(exitCode int32, byteCount int) map[string]string {
	return map[string]string{
		types.EventAttrExitCode: fmt.Sprintf("%d", exitCode),
		types.EventAttrBytes:    fmt.Sprintf("%d", byteCount),
	}
}

func parseRunnerEventTime(value string) (time.Time, bool) {
	if value == "" {
		return time.Time{}, false
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, false
	}
	return parsed.UTC(), true
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func eventMetadataFromData(data interface{}) eventMetadata {
	switch d := data.(type) {
	case types.EventContainerMetricsSchema:
		return eventMetadata{ContainerID: d.ContainerID, StubID: d.StubID, WorkerID: d.WorkerID, WorkspaceID: d.WorkspaceID}
	case types.EventContainerLifecycleSchema:
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
