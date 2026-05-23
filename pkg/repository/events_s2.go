package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/rs/zerolog/log"
	"github.com/s2-streamstore/s2-sdk-go/s2"
)

const (
	defaultS2EventStreamPrefix = "events"
	defaultS2EventReadLimit    = 1000
	s2EventQueueSize           = 16384
	s2EventRetentionSeconds    = int64(7 * 24 * 60 * 60)
	s2EventWriteTimeout        = 5 * time.Second
)

type S2EventRepository struct {
	config       types.S2Config
	client       *s2.Client
	basin        *s2.BasinClient
	basinName    s2.BasinName
	streamPrefix string
	ensured      sync.Map
	basinMu      sync.Mutex
	basinEnsured bool
	queue        chan cloudevents.Event
}

func NewS2EventRepository(config types.S2Config) (*S2EventRepository, error) {
	if config.ApiKey == "" {
		return nil, nil
	}

	if config.Basin == "" {
		return nil, fmt.Errorf("s2 basin is required when s2 api key is configured")
	}

	streamPrefix := strings.Trim(config.StreamPrefix, "/")
	if streamPrefix == "" {
		streamPrefix = defaultS2EventStreamPrefix
	}

	client := s2.New(config.ApiKey, &s2.ClientOptions{
		RequestTimeout: s2EventWriteTimeout,
		RetryConfig: &s2.RetryConfig{
			MaxAttempts:       3,
			AppendRetryPolicy: s2.AppendRetryPolicyAll,
		},
	})

	repo := &S2EventRepository{
		config:       config,
		client:       client,
		basin:        client.Basin(config.Basin),
		basinName:    s2.BasinName(config.Basin),
		streamPrefix: streamPrefix,
		queue:        make(chan cloudevents.Event, s2EventQueueSize),
	}
	go repo.runWriter()

	return repo, nil
}

func (r *S2EventRepository) PushEvent(event cloudevents.Event) error {
	select {
	case r.queue <- event:
		return nil
	default:
		return fmt.Errorf("s2 event queue is full")
	}
}

func (r *S2EventRepository) runWriter() {
	for event := range r.queue {
		if err := r.appendEvent(event); err != nil {
			log.Debug().Err(err).Str("event_type", event.Type()).Msg("failed to append event to s2")
		}
	}
}

func (r *S2EventRepository) appendEvent(event cloudevents.Event) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal cloud event: %w", err)
	}

	metadata := eventMetadataFromCloudEvent(event)
	streamName := r.streamNameForEvent(event.Type(), metadata)
	if streamName == "" {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), s2EventWriteTimeout)
	defer cancel()

	if err := r.ensureBasin(ctx); err != nil {
		return err
	}
	if err := r.ensureStream(ctx, streamName); err != nil {
		return err
	}

	timestamp := uint64(event.Time().UnixMilli())
	_, err = r.basin.Stream(streamName).Append(ctx, &s2.AppendInput{
		Records: []s2.AppendRecord{
			{
				Timestamp: &timestamp,
				Headers:   s2HeadersForEvent(event, metadata),
				Body:      body,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("append event to s2 stream %q: %w", streamName, err)
	}

	return nil
}

func (r *S2EventRepository) GetContainerEvents(ctx context.Context, containerID string, query types.EventQuery) (*types.ContainerEventsResponse, error) {
	limit := query.Limit
	if limit == 0 {
		limit = defaultS2EventReadLimit
	}

	if err := r.ensureBasin(ctx); err != nil {
		return nil, err
	}

	streams, err := r.resolveContainerStreams(ctx, containerID, query)
	if err != nil {
		return nil, err
	}
	if len(streams) == 0 {
		return &types.ContainerEventsResponse{
			ContainerID: containerID,
			WorkspaceID: query.WorkspaceID,
			StubID:      query.StubID,
			Summary:     map[string]int64{},
			Events:      []types.ContainerEventRecord{},
			Missing:     requiredContainerPhaseIDs(nil),
			Streams:     []string{},
		}, nil
	}

	response := &types.ContainerEventsResponse{
		ContainerID: containerID,
		WorkspaceID: query.WorkspaceID,
		StubID:      query.StubID,
		Summary:     map[string]int64{},
		Events:      []types.ContainerEventRecord{},
		Streams:     make([]string, 0, len(streams)),
	}

	for _, streamName := range streams {
		if err := r.readContainerStream(ctx, streamName, limit, response); err != nil {
			return nil, err
		}
	}

	sortContainerEventRecords(response.Events)
	response.Summary = summarizeContainerPhaseDurations(response.Events)
	response.Missing = requiredContainerPhaseIDs(response.Events)
	return response, nil
}

func (r *S2EventRepository) resolveContainerStreams(ctx context.Context, containerID string, query types.EventQuery) ([]s2.StreamName, error) {
	if query.WorkspaceID != "" && query.StubID != "" {
		streamName := r.containerStreamName(query.WorkspaceID, query.StubID, containerID)
		exists, err := r.streamExists(ctx, streamName)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, nil
		}
		return []s2.StreamName{streamName}, nil
	}

	if query.WorkspaceID == "" {
		return nil, nil
	}

	prefix := r.workspaceStubPrefix(query.WorkspaceID)
	limit := 1000
	resp, err := r.basin.Streams.List(ctx, &s2.ListStreamsArgs{
		Prefix: prefix,
		Limit:  &limit,
	})
	if err != nil {
		return nil, fmt.Errorf("list s2 container event streams for workspace %q: %w", query.WorkspaceID, err)
	}

	suffix := "/containers/" + eventStreamPart(containerID)
	streams := make([]s2.StreamName, 0, 1)
	for _, stream := range resp.Streams {
		name := string(stream.Name)
		if strings.HasSuffix(name, suffix) {
			streams = append(streams, stream.Name)
		}
	}

	return streams, nil
}

func (r *S2EventRepository) streamExists(ctx context.Context, streamName s2.StreamName) (bool, error) {
	limit := 1
	resp, err := r.basin.Streams.List(ctx, &s2.ListStreamsArgs{
		Prefix: string(streamName),
		Limit:  &limit,
	})
	if err != nil {
		return false, fmt.Errorf("list s2 event stream %q: %w", streamName, err)
	}
	for _, stream := range resp.Streams {
		if stream.Name == streamName {
			return true, nil
		}
	}
	return false, nil
}

func (r *S2EventRepository) readContainerStream(ctx context.Context, streamName s2.StreamName, limit uint64, response *types.ContainerEventsResponse) error {
	seqNum := uint64(0)
	batch, err := r.basin.Stream(streamName).Read(ctx, &s2.ReadOptions{
		SeqNum: &seqNum,
		Count:  &limit,
	})
	if err != nil {
		return fmt.Errorf("read container events from s2 stream %q: %w", streamName, err)
	}

	response.Streams = append(response.Streams, string(streamName))
	for _, record := range batch.Records {
		eventRecord := types.ContainerEventRecord{
			SeqNum:     record.SeqNum,
			StoredAtNs: record.Timestamp,
			CloudEvent: append([]byte(nil), record.Body...),
		}

		var envelope struct {
			Type string          `json:"type"`
			Time time.Time       `json:"time"`
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(record.Body, &envelope); err != nil {
			log.Debug().Err(err).Str("container_id", response.ContainerID).Msg("failed to unmarshal cloud event envelope")
			response.Events = append(response.Events, eventRecord)
			continue
		}

		eventRecord.Type = envelope.Type
		eventRecord.Timestamp = envelope.Time
		eventRecord.Data = envelope.Data
		augmentContainerEventResponse(response, &eventRecord)
		response.Events = append(response.Events, eventRecord)
	}
	return nil
}

func (r *S2EventRepository) ensureBasin(ctx context.Context) error {
	r.basinMu.Lock()
	defer r.basinMu.Unlock()

	if r.basinEnsured {
		return nil
	}

	_, err := r.client.Basins.Ensure(ctx, s2.EnsureBasinArgs{
		Basin: r.basinName,
	})
	if err != nil {
		return fmt.Errorf("ensure s2 event basin %q: %w", r.basinName, err)
	}

	r.basinEnsured = true
	return nil
}

func (r *S2EventRepository) ensureStream(ctx context.Context, streamName s2.StreamName) error {
	if _, ok := r.ensured.Load(streamName); ok {
		return nil
	}

	retention := s2EventRetentionSeconds
	_, err := r.basin.Streams.Ensure(ctx, s2.EnsureStreamArgs{
		Stream: streamName,
		Config: &s2.StreamConfig{
			RetentionPolicy: &s2.RetentionPolicy{Age: &retention},
		},
	})
	if err != nil {
		return fmt.Errorf("ensure s2 event stream %q: %w", streamName, err)
	}

	r.ensured.Store(streamName, struct{}{})
	return nil
}

func (r *S2EventRepository) streamNameForEvent(eventType string, metadata eventMetadata) s2.StreamName {
	switch {
	case metadata.ContainerID != "" && metadata.WorkspaceID != "" && metadata.StubID != "":
		return r.containerStreamName(metadata.WorkspaceID, metadata.StubID, metadata.ContainerID)
	case strings.HasPrefix(eventType, "container.") && metadata.ContainerID != "":
		return ""
	case metadata.TaskID != "":
		return r.taskStreamName(metadata.TaskID)
	case metadata.WorkerID != "":
		return r.workerStreamName(metadata.WorkerID)
	case metadata.WorkspaceID != "" && metadata.StubID != "":
		return r.stubStreamName(metadata.WorkspaceID, metadata.StubID)
	case metadata.WorkspaceID != "":
		return r.workspaceStreamName(metadata.WorkspaceID)
	default:
		return r.typeStreamName(eventType)
	}
}

func (r *S2EventRepository) containerStreamName(workspaceID, stubID, containerID string) s2.StreamName {
	return s2.StreamName(fmt.Sprintf(
		"%s/workspaces/%s/stubs/%s/containers/%s",
		r.streamPrefix,
		eventStreamPart(workspaceID),
		eventStreamPart(stubID),
		eventStreamPart(containerID),
	))
}

func (r *S2EventRepository) workspaceStubPrefix(workspaceID string) string {
	return fmt.Sprintf("%s/workspaces/%s/stubs/", r.streamPrefix, eventStreamPart(workspaceID))
}

func (r *S2EventRepository) taskStreamName(taskID string) s2.StreamName {
	return s2.StreamName(fmt.Sprintf("%s/tasks/%s", r.streamPrefix, taskID))
}

func (r *S2EventRepository) workerStreamName(workerID string) s2.StreamName {
	return s2.StreamName(fmt.Sprintf("%s/workers/%s", r.streamPrefix, workerID))
}

func (r *S2EventRepository) workspaceStreamName(workspaceID string) s2.StreamName {
	return s2.StreamName(fmt.Sprintf("%s/workspaces/%s", r.streamPrefix, workspaceID))
}

func (r *S2EventRepository) stubStreamName(workspaceID, stubID string) s2.StreamName {
	return s2.StreamName(fmt.Sprintf("%s/workspaces/%s/stubs/%s", r.streamPrefix, eventStreamPart(workspaceID), eventStreamPart(stubID)))
}

func (r *S2EventRepository) typeStreamName(eventType string) s2.StreamName {
	return s2.StreamName(fmt.Sprintf("%s/types/%s", r.streamPrefix, strings.ReplaceAll(eventType, ".", "-")))
}

func eventStreamPart(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, "/")
	return strings.ReplaceAll(value, "/", "_")
}

func eventMetadataFromCloudEvent(event cloudevents.Event) eventMetadata {
	extensions := event.Extensions()
	return eventMetadata{
		ContainerID: extensionString(extensions, "containerid"),
		WorkspaceID: extensionString(extensions, "workspaceid"),
		TaskID:      extensionString(extensions, "taskid"),
		StubID:      extensionString(extensions, "stubid"),
		WorkerID:    extensionString(extensions, "workerid"),
	}
}

func extensionString(extensions map[string]interface{}, key string) string {
	if value, ok := extensions[key]; ok {
		return fmt.Sprintf("%v", value)
	}
	return ""
}

func s2HeadersForEvent(event cloudevents.Event, metadata eventMetadata) []s2.Header {
	headers := []s2.Header{
		s2.NewHeader("type", event.Type()),
		s2.NewHeader("id", event.ID()),
	}
	if metadata.ContainerID != "" {
		headers = append(headers, s2.NewHeader("container_id", metadata.ContainerID))
	}
	if metadata.WorkspaceID != "" {
		headers = append(headers, s2.NewHeader("workspace_id", metadata.WorkspaceID))
	}
	if metadata.TaskID != "" {
		headers = append(headers, s2.NewHeader("task_id", metadata.TaskID))
	}
	if metadata.StubID != "" {
		headers = append(headers, s2.NewHeader("stub_id", metadata.StubID))
	}
	if metadata.WorkerID != "" {
		headers = append(headers, s2.NewHeader("worker_id", metadata.WorkerID))
	}
	return headers
}

func augmentContainerEventResponse(response *types.ContainerEventsResponse, record *types.ContainerEventRecord) {
	switch record.Type {
	case types.EventContainerPhase:
		var phase types.EventContainerPhaseSchema
		if err := json.Unmarshal(record.Data, &phase); err != nil {
			return
		}
		record.EventID = string(phase.ID)
		record.Domain = string(phase.Domain)
		record.StartTime = phase.StartTime
		record.EndTime = phase.EndTime
		record.DurationMs = phase.DurationMs
		record.Success = phase.Success
		record.Source = phase.Source
		record.Attrs = phase.Attrs
		record.ContainerID = phase.ContainerID
		record.StubID = phase.StubID
		record.StubType = phase.StubType
		record.TaskID = phase.TaskID
		record.WorkspaceID = phase.WorkspaceID
		record.WorkerID = phase.WorkerID
		if response.WorkspaceID == "" {
			response.WorkspaceID = phase.WorkspaceID
		}
		if response.StubID == "" {
			response.StubID = phase.StubID
		}
	case types.EventContainerEvent:
		var event types.EventContainerEventSchema
		if err := json.Unmarshal(record.Data, &event); err != nil {
			return
		}
		record.EventID = string(event.ID)
		record.Domain = string(event.Domain)
		record.Timestamp = event.Timestamp
		record.Reason = event.Reason
		record.Source = event.Source
		record.Message = event.Message
		record.Attrs = event.Attrs
		record.ContainerID = event.ContainerID
		record.StubID = event.StubID
		record.StubType = event.StubType
		record.TaskID = event.TaskID
		record.WorkspaceID = event.WorkspaceID
		record.WorkerID = event.WorkerID
		if response.WorkspaceID == "" {
			response.WorkspaceID = event.WorkspaceID
		}
		if response.StubID == "" {
			response.StubID = event.StubID
		}
		if event.Reason != "" && event.Reason != "UNKNOWN" {
			response.StopReason = event.Reason
		}
		if response.RootCauseEvent == "" && types.IsContainerRootCauseCandidate(event.ID) {
			response.RootCauseEvent = string(event.ID)
		}
	case types.EventContainerLog:
		var entry types.EventContainerLogSchema
		if err := json.Unmarshal(record.Data, &entry); err != nil {
			return
		}
		record.ContainerID = entry.ContainerID
		record.Timestamp = entry.Timestamp
		record.StubID = entry.StubID
		record.StubType = entry.StubType
		record.TaskID = entry.TaskID
		record.WorkspaceID = entry.WorkspaceID
		record.WorkerID = entry.WorkerID
		record.Stream = entry.Stream
		record.Line = entry.Line
		if response.WorkspaceID == "" {
			response.WorkspaceID = entry.WorkspaceID
		}
		if response.StubID == "" {
			response.StubID = entry.StubID
		}
	case types.EventContainerLifecycle:
		var lifecycle types.EventContainerLifecycleSchema
		if err := json.Unmarshal(record.Data, &lifecycle); err != nil {
			return
		}
		record.ContainerID = lifecycle.ContainerID
		record.StubID = lifecycle.StubID
		record.StubType = lifecycle.StubType
		record.TaskID = lifecycle.TaskID
		record.WorkerID = lifecycle.WorkerID
		record.WorkspaceID = lifecycle.WorkspaceID
		if record.WorkspaceID == "" {
			record.WorkspaceID = lifecycle.Request.WorkspaceId
		}
		if response.WorkspaceID == "" {
			response.WorkspaceID = record.WorkspaceID
		}
		if response.StubID == "" {
			response.StubID = lifecycle.StubID
		}
		response.Status = lifecycle.Status
	}
}

func summarizeContainerPhaseDurations(events []types.ContainerEventRecord) map[string]int64 {
	summary := map[string]int64{}
	var firstEventAt time.Time
	var queueStartAt time.Time
	var workerReceiveAt time.Time
	var runningAt time.Time
	var startTaskAt time.Time
	var runnerProcessStartedAt time.Time
	var runnerModuleLoadedAt time.Time
	var runnerMainEnteredAt time.Time
	var firstLogAfterRunning time.Time
	var firstLogAfterStartTask time.Time

	for _, event := range events {
		eventAt := containerEventRecordTime(event)
		if !eventAt.IsZero() && firstEventAt.IsZero() {
			firstEventAt = eventAt
		}

		switch event.Type {
		case types.EventContainerPhase:
			if event.EventID == string(types.ContainerPhaseStartup) && !event.EndTime.IsZero() {
				runningAt = event.EndTime
			}
			if event.EventID == string(types.ContainerPhaseSchedulerQueuePush) && !event.StartTime.IsZero() && queueStartAt.IsZero() {
				queueStartAt = event.StartTime
			}
			if event.EventID == string(types.ContainerPhaseWorkerQueueReceive) && !event.StartTime.IsZero() && workerReceiveAt.IsZero() {
				workerReceiveAt = event.StartTime
			}
			if event.EventID == "" || event.DurationMs <= 0 {
				continue
			}
			id := types.ContainerPhaseID(event.EventID)
			setMaxDuration(summary, types.EventSummaryKeyForPhase(id), event.DurationMs)
		case types.EventContainerEvent:
			if event.EventID == string(types.ContainerEventRunnerStartTask) && !event.Timestamp.IsZero() {
				startTaskAt = event.Timestamp
			}
			if event.EventID == string(types.ContainerEventRunnerProcessStarted) && !event.Timestamp.IsZero() {
				runnerProcessStartedAt = event.Timestamp
			}
			if event.EventID == string(types.ContainerEventRunnerModuleLoaded) && !event.Timestamp.IsZero() {
				runnerModuleLoadedAt = event.Timestamp
			}
			if event.EventID == string(types.ContainerEventRunnerMainEntered) && !event.Timestamp.IsZero() {
				runnerMainEnteredAt = event.Timestamp
			}
		case types.EventContainerLog:
			if event.Timestamp.IsZero() {
				continue
			}
			if !runningAt.IsZero() && event.Timestamp.After(runningAt) && firstLogAfterRunning.IsZero() {
				firstLogAfterRunning = event.Timestamp
			}
			if !startTaskAt.IsZero() && event.Timestamp.After(startTaskAt) && firstLogAfterStartTask.IsZero() {
				firstLogAfterStartTask = event.Timestamp
			}
		}
	}

	setSummedDuration(summary, "scheduler_ms",
		"scheduler_queue_push_ms",
		"scheduler_backlog_ms",
		"scheduler_worker_selection_ms",
		"scheduler_reservation_ms",
		"scheduler_provision_worker_ms",
	)
	setSummedDuration(summary, "worker_ms",
		"worker_queue_ms",
		"worker_set_worker_address_ms",
		"worker_port_allocation_ms",
		"worker_read_bundle_config_ms",
		"worker_spec_from_request_ms",
		"worker_set_container_address_ms",
		"worker_set_address_map_ms",
		"worker_gpu_assignment_ms",
		"worker_start_queue_wait_ms",
	)
	setSummedDuration(summary, "mount_ms",
		"mount_setup_ms",
		"mount_overlay_setup_ms",
	)
	setSummedDuration(summary, "network_ms",
		"network_setup_ms",
		"network_expose_ports_ms",
	)
	if summary["runtime_ms"] == 0 {
		setMaxDuration(summary, "runtime_ms", summary["container_startup_ms"])
	}
	if summary["runner_start_to_end_task_ms"] > 0 {
		setMaxDuration(summary, "runner_ms", summary["runner_start_to_end_task_ms"])
	}
	if summary["result_ms"] == 0 {
		setMaxDuration(summary, "result_ms", summary["result_set_to_end_task_ms"])
	}
	if summary["container_request_to_start_task_ms"] > 0 {
		setMaxDuration(summary, "task_ms", summary["container_request_to_start_task_ms"])
	}
	if !runningAt.IsZero() {
		if !firstEventAt.IsZero() {
			setPositiveDuration(summary, "container_request_to_running_ms", firstEventAt, runningAt)
			setPositiveDuration(summary, "to_running_ms", firstEventAt, runningAt)
		}
		setPositiveDuration(summary, "scheduler_queue_to_running_ms", queueStartAt, runningAt)
		setPositiveDuration(summary, "worker_receive_to_running_ms", workerReceiveAt, runningAt)
		setPositiveDuration(summary, "running_to_runner_process_started_ms", runningAt, runnerProcessStartedAt)
		setPositiveDuration(summary, "running_to_runner_main_ms", runningAt, runnerMainEnteredAt)
	}
	setPositiveDuration(summary, "runner_process_to_module_loaded_ms", runnerProcessStartedAt, runnerModuleLoadedAt)
	setPositiveDuration(summary, "runner_module_loaded_to_main_ms", runnerModuleLoadedAt, runnerMainEnteredAt)
	setPositiveDuration(summary, "runner_main_to_start_task_ms", runnerMainEnteredAt, startTaskAt)
	if !runningAt.IsZero() && !firstLogAfterRunning.IsZero() {
		setMaxDuration(summary, "running_to_first_log_ms", firstLogAfterRunning.Sub(runningAt).Milliseconds())
	}
	if !startTaskAt.IsZero() && !firstLogAfterStartTask.IsZero() {
		setMaxDuration(summary, "start_task_to_first_log_ms", firstLogAfterStartTask.Sub(startTaskAt).Milliseconds())
	}

	return summary
}

func setPositiveDuration(summary map[string]int64, key string, start time.Time, end time.Time) {
	if start.IsZero() || end.IsZero() || end.Before(start) {
		return
	}
	setMaxDuration(summary, key, end.Sub(start).Milliseconds())
}

func setMaxDuration(summary map[string]int64, key string, duration int64) {
	if key == "" || duration <= 0 {
		return
	}
	if current, ok := summary[key]; !ok || duration > current {
		summary[key] = duration
	}
}

func setSummedDuration(summary map[string]int64, key string, parts ...string) {
	total := int64(0)
	for _, part := range parts {
		total += summary[part]
	}
	setMaxDuration(summary, key, total)
}

func sortContainerEventRecords(events []types.ContainerEventRecord) {
	sort.SliceStable(events, func(i, j int) bool {
		left := containerEventRecordTime(events[i])
		right := containerEventRecordTime(events[j])
		if !left.Equal(right) {
			return left.Before(right)
		}
		return events[i].SeqNum < events[j].SeqNum
	})
}

func containerEventRecordTime(event types.ContainerEventRecord) time.Time {
	switch {
	case !event.StartTime.IsZero():
		return event.StartTime
	case !event.Timestamp.IsZero():
		return event.Timestamp
	case !event.EndTime.IsZero():
		return event.EndTime
	case event.StoredAtNs != 0:
		return time.Unix(0, int64(event.StoredAtNs)).UTC()
	default:
		return time.Time{}
	}
}

func requiredContainerPhaseIDs(events []types.ContainerEventRecord) []string {
	seen := map[string]struct{}{}
	for _, event := range events {
		if event.Type == types.EventContainerPhase && event.EventID != "" {
			seen[event.EventID] = struct{}{}
		}
	}

	missing := []string{}
	for id, def := range types.ContainerPhaseDefinitions {
		if !def.Required {
			continue
		}
		if _, ok := seen[string(id)]; !ok {
			missing = append(missing, string(id))
		}
	}
	sort.Strings(missing)
	return missing
}
