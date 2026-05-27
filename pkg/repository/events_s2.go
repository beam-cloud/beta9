package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
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
	defaultS2EventReadLimit    = 10000
	s2EventQueueSize           = 16384
	s2EventBatchSize           = 256
	s2EventRetentionSeconds    = int64(7 * 24 * 60 * 60)
	s2EventWriteTimeout        = 5 * time.Second
	s2EventFlushInterval       = 100 * time.Millisecond
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
	ticker := time.NewTicker(s2EventFlushInterval)
	defer ticker.Stop()

	batch := make([]cloudevents.Event, 0, s2EventBatchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := r.appendEventBatch(batch); err != nil {
			log.Debug().Err(err).Int("event_count", len(batch)).Msg("failed to append event batch to s2")
		}
		batch = batch[:0]
	}

	for {
		select {
		case event, ok := <-r.queue:
			if !ok {
				flush()
				return
			}
			batch = append(batch, event)
			if len(batch) >= s2EventBatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (r *S2EventRepository) appendEvent(event cloudevents.Event) error {
	return r.appendEventBatch([]cloudevents.Event{event})
}

func (r *S2EventRepository) appendEventBatch(events []cloudevents.Event) error {
	if len(events) == 0 {
		return nil
	}

	if err := r.ensureBasinForWrite(); err != nil {
		return err
	}

	recordsByStream := map[s2.StreamName][]s2.AppendRecord{}
	for _, event := range events {
		record, streamName, err := r.appendRecordForEvent(event)
		if err != nil {
			log.Debug().Err(err).Str("event_type", event.Type()).Msg("failed to build s2 event record")
			continue
		}
		if streamName == "" {
			continue
		}
		recordsByStream[streamName] = append(recordsByStream[streamName], record)
	}

	var streamErrs []error
	for streamName, records := range recordsByStream {
		if len(records) == 0 {
			continue
		}
		if err := r.ensureStreamForWrite(streamName); err != nil {
			if isS2EventStreamDeletionPending(err) {
				r.ensured.Delete(streamName)
				log.Debug().Err(err).Str("stream", string(streamName)).Msg("dropping event batch for stream pending deletion")
				continue
			}
			streamErrs = append(streamErrs, fmt.Errorf("ensure s2 stream %q: %w", streamName, err))
			continue
		}
		if err := r.appendRecordsForWrite(streamName, records); err != nil {
			if isS2EventStreamDeletionPending(err) {
				r.ensured.Delete(streamName)
				log.Debug().Err(err).Str("stream", string(streamName)).Msg("dropping event batch for stream pending deletion")
				continue
			}
			streamErrs = append(streamErrs, fmt.Errorf("append %d events to s2 stream %q: %w", len(records), streamName, err))
		}
	}

	return errors.Join(streamErrs...)
}

func (r *S2EventRepository) appendRecordForEvent(event cloudevents.Event) (s2.AppendRecord, s2.StreamName, error) {
	body, err := json.Marshal(event)
	if err != nil {
		return s2.AppendRecord{}, "", fmt.Errorf("marshal cloud event: %w", err)
	}

	metadata := eventMetadataFromCloudEvent(event)
	streamName := r.streamNameForEvent(event.Type(), metadata)
	if streamName == "" {
		return s2.AppendRecord{}, "", nil
	}

	timestamp := uint64(event.Time().UnixMilli())
	return s2.AppendRecord{
		Timestamp: &timestamp,
		Headers:   s2HeadersForEvent(event, metadata),
		Body:      body,
	}, streamName, nil
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
			Missing:     requiredContainerLifecycleIDs(nil),
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
		if err := r.readContainerStream(ctx, streamName, limit, query, response); err != nil {
			return nil, err
		}
	}

	sortContainerEventRecords(response.Events)
	setContainerStopCause(response)
	response.Summary = summarizeContainerLifecycleDurations(response.Events)
	response.Missing = requiredContainerLifecycleIDs(response.Events)
	return response, nil
}

func (r *S2EventRepository) StreamContainerEvents(ctx context.Context, containerID string, query types.EventQuery) (EventStream, error) {
	if query.WorkspaceID == "" || query.StubID == "" {
		return nil, fmt.Errorf("workspace id and stub id are required to stream container events")
	}

	if err := r.ensureBasin(ctx); err != nil {
		return nil, err
	}

	streamName := r.containerStreamName(query.WorkspaceID, query.StubID, containerID)
	if err := r.ensureStream(ctx, streamName); err != nil {
		return nil, err
	}

	opts := &s2.ReadOptions{
		SeqNum:     query.SeqNum,
		TailOffset: query.TailOffset,
		Count:      countOption(query.Limit),
		Wait:       query.WaitSeconds,
		Clamp:      query.Clamp,
	}
	session, err := r.basin.Stream(streamName).ReadSession(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("stream container events from s2 stream %q: %w", streamName, err)
	}

	return &s2ContainerEventStream{
		session: session,
		query:   query,
		response: &types.ContainerEventsResponse{
			ContainerID: containerID,
			WorkspaceID: query.WorkspaceID,
			StubID:      query.StubID,
		},
	}, nil
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
	suffix := "/containers/" + eventStreamPart(containerID)
	streams := make([]s2.StreamName, 0, 1)
	iter := r.basin.Streams.Iter(ctx, &s2.ListStreamsArgs{Prefix: prefix})
	for iter.Next() {
		stream := iter.Value()
		name := string(stream.Name)
		if strings.HasSuffix(name, suffix) {
			streams = append(streams, stream.Name)
		}
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("list s2 container event streams for workspace %q: %w", query.WorkspaceID, err)
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

func (r *S2EventRepository) readContainerStream(ctx context.Context, streamName s2.StreamName, limit uint64, query types.EventQuery, response *types.ContainerEventsResponse) error {
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
		eventRecord, ok := containerEventRecordFromS2(record, query, response)
		if !ok {
			continue
		}
		response.Events = append(response.Events, eventRecord)
	}
	return nil
}

type s2ContainerEventStream struct {
	session *s2.ReadSession
	query   types.EventQuery

	response *types.ContainerEventsResponse
	current  types.ContainerEventRecord
}

func (s *s2ContainerEventStream) Next() bool {
	for s.session.Next() {
		eventRecord, ok := containerEventRecordFromS2(s.session.Record(), s.query, s.response)
		if !ok {
			continue
		}
		s.current = eventRecord
		return true
	}
	return false
}

func (s *s2ContainerEventStream) Record() types.ContainerEventRecord {
	return s.current
}

func (s *s2ContainerEventStream) Err() error {
	return s.session.Err()
}

func (s *s2ContainerEventStream) Close() error {
	return s.session.Close()
}

func containerEventRecordFromS2(record s2.SequencedRecord, query types.EventQuery, response *types.ContainerEventsResponse) (types.ContainerEventRecord, bool) {
	eventRecord := types.ContainerEventRecord{
		SeqNum:     record.SeqNum,
		StoredAtNs: s2TimestampMillisToNanos(record.Timestamp),
		CloudEvent: append([]byte(nil), record.Body...),
	}

	var envelope struct {
		Type        string          `json:"type"`
		Time        time.Time       `json:"time"`
		Data        json.RawMessage `json:"data"`
		ContainerID string          `json:"containerid"`
		WorkspaceID string          `json:"workspaceid"`
		TaskID      string          `json:"taskid"`
		StubID      string          `json:"stubid"`
		WorkerID    string          `json:"workerid"`
	}
	if err := json.Unmarshal(record.Body, &envelope); err != nil {
		log.Debug().Err(err).Str("container_id", response.ContainerID).Msg("failed to unmarshal cloud event envelope")
		if query.TaskID != "" {
			return types.ContainerEventRecord{}, false
		}
		return eventRecord, true
	}

	eventRecord.Type = envelope.Type
	eventRecord.Timestamp = envelope.Time
	eventRecord.Data = envelope.Data
	eventRecord.ContainerID = envelope.ContainerID
	eventRecord.WorkspaceID = envelope.WorkspaceID
	eventRecord.TaskID = envelope.TaskID
	eventRecord.StubID = envelope.StubID
	eventRecord.WorkerID = envelope.WorkerID
	if !eventQueryAllowsType(query, eventRecord.Type) {
		return types.ContainerEventRecord{}, false
	}
	augmentContainerEventResponse(response, &eventRecord)
	if eventRecord.ContainerID == "" {
		eventRecord.ContainerID = envelope.ContainerID
	}
	if eventRecord.WorkspaceID == "" {
		eventRecord.WorkspaceID = envelope.WorkspaceID
	}
	if eventRecord.TaskID == "" {
		eventRecord.TaskID = envelope.TaskID
	}
	if eventRecord.StubID == "" {
		eventRecord.StubID = envelope.StubID
	}
	if eventRecord.WorkerID == "" {
		eventRecord.WorkerID = envelope.WorkerID
	}
	if query.TaskID != "" && eventRecord.TaskID != query.TaskID {
		return types.ContainerEventRecord{}, false
	}
	return eventRecord, true
}

func countOption(limit uint64) *uint64 {
	if limit == 0 {
		return nil
	}
	return &limit
}

func eventQueryAllowsType(query types.EventQuery, eventType string) bool {
	if len(query.EventTypes) == 0 {
		return true
	}
	for _, allowed := range query.EventTypes {
		if strings.TrimSpace(allowed) == eventType {
			return true
		}
	}
	return false
}

func s2TimestampMillisToNanos(timestamp uint64) uint64 {
	if timestamp == 0 {
		return 0
	}
	if timestamp >= uint64(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()) {
		return timestamp
	}
	return timestamp * uint64(time.Millisecond)
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

func (r *S2EventRepository) ensureBasinForWrite() error {
	ctx, cancel := s2EventWriteContext()
	defer cancel()
	return r.ensureBasin(ctx)
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

func (r *S2EventRepository) ensureStreamForWrite(streamName s2.StreamName) error {
	ctx, cancel := s2EventWriteContext()
	defer cancel()
	return r.ensureStream(ctx, streamName)
}

func (r *S2EventRepository) appendRecordsForWrite(streamName s2.StreamName, records []s2.AppendRecord) error {
	ctx, cancel := s2EventWriteContext()
	defer cancel()
	_, err := r.basin.Stream(streamName).Append(ctx, &s2.AppendInput{Records: records})
	return err
}

func s2EventWriteContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), s2EventWriteTimeout)
}

func isS2EventStreamDeletionPending(err error) bool {
	var s2Err *s2.S2Error
	return errors.As(err, &s2Err) && s2Err.Code == "stream_deletion_pending"
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
	case metadata.PoolName != "":
		return r.workerPoolStreamName(metadata.PoolName)
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

func (r *S2EventRepository) workerPoolStreamName(poolName string) s2.StreamName {
	return s2.StreamName(fmt.Sprintf("%s/worker-pools/%s", r.streamPrefix, eventStreamPart(poolName)))
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
		PoolName:    extensionString(extensions, "poolname"),
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
	if metadata.PoolName != "" {
		headers = append(headers, s2.NewHeader("pool_name", metadata.PoolName))
	}
	return headers
}

func augmentContainerEventResponse(response *types.ContainerEventsResponse, record *types.ContainerEventRecord) {
	switch record.Type {
	case types.EventContainerLifecycle:
		var lifecycle types.EventContainerLifecycleSchema
		if err := json.Unmarshal(record.Data, &lifecycle); err != nil {
			return
		}
		record.EventID = string(lifecycle.ID)
		record.Domain = string(lifecycle.Domain)
		record.ParentID = string(lifecycle.ParentID)
		record.StartTime = lifecycle.StartTime
		record.EndTime = lifecycle.EndTime
		record.DurationMs = lifecycle.DurationMs
		record.Success = lifecycle.Success
		record.Source = lifecycle.Source
		record.Attrs = lifecycle.Attrs
		record.ContainerID = lifecycle.ContainerID
		record.StubID = lifecycle.StubID
		record.StubType = lifecycle.StubType
		record.TaskID = lifecycle.TaskID
		record.WorkspaceID = lifecycle.WorkspaceID
		record.WorkerID = lifecycle.WorkerID
		if response.WorkspaceID == "" {
			response.WorkspaceID = lifecycle.WorkspaceID
		}
		if response.StubID == "" {
			response.StubID = lifecycle.StubID
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
	}
}

func setContainerStopCause(response *types.ContainerEventsResponse) {
	response.StopReason = ""
	response.RootCauseEvent = ""
	for _, event := range response.Events {
		if event.Type != types.EventContainerEvent {
			continue
		}
		if event.Reason != "" && event.Reason != "UNKNOWN" {
			response.StopReason = event.Reason
		}
		if response.RootCauseEvent == "" && types.IsContainerRootCauseCandidate(types.ContainerEventID(event.EventID)) {
			response.RootCauseEvent = event.EventID
		}
	}
}

func summarizeContainerLifecycleDurations(events []types.ContainerEventRecord) map[string]int64 {
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
		case types.EventContainerLifecycle:
			if event.EventID == string(types.ContainerLifecycleStartup) && !event.EndTime.IsZero() {
				runningAt = event.EndTime
			}
			if event.EventID == string(types.ContainerLifecycleSchedulerQueuePush) && !event.StartTime.IsZero() && queueStartAt.IsZero() {
				queueStartAt = event.StartTime
			}
			if event.EventID == string(types.ContainerLifecycleWorkerQueueReceive) && !event.StartTime.IsZero() && workerReceiveAt.IsZero() {
				workerReceiveAt = event.StartTime
			}
			durationUs := containerEventRecordDurationUs(event)
			durationMs := event.DurationMs
			if durationMs <= 0 && durationUs > 0 {
				durationMs = durationUsToMilliseconds(durationUs)
			}
			if event.EventID == "" || (durationMs <= 0 && durationUs <= 0) {
				continue
			}
			id := types.ContainerLifecycleID(event.EventID)
			summaryKey := types.EventSummaryKeyForLifecycle(id)
			setMaxDuration(summary, summaryKey, durationMs)
			if strings.HasPrefix(event.EventID, "clip.") {
				baseKey := strings.TrimSuffix(summaryKey, "_ms")
				if durationUs > 0 {
					addDuration(summary, baseKey+"_total_us", durationUs)
					setMaxDuration(summary, baseKey+"_max_us", durationUs)
					durationMs = durationUsToMilliseconds(durationUs)
				}
				addDuration(summary, baseKey+"_total_ms", durationMs)
				incrementCount(summary, baseKey+"_count")
				setMaxDuration(summary, baseKey+"_max_ms", durationMs)
			}
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
	if summary["clip_read_total_us"] > 0 {
		setMaxDuration(summary, "clip_us", summary["clip_read_total_us"])
		setMaxDuration(summary, "clip_ms", durationUsToMilliseconds(summary["clip_read_total_us"]))
	} else if summary["clip_oci_read_total_us"] > 0 {
		setMaxDuration(summary, "clip_us", summary["clip_oci_read_total_us"])
		setMaxDuration(summary, "clip_ms", durationUsToMilliseconds(summary["clip_oci_read_total_us"]))
	} else if summary["clip_read_total_ms"] > 0 {
		setMaxDuration(summary, "clip_ms", summary["clip_read_total_ms"])
	} else if summary["clip_oci_read_total_ms"] > 0 {
		setMaxDuration(summary, "clip_ms", summary["clip_oci_read_total_ms"])
	}
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
	setPositiveDuration(summary, "scheduler_queue_to_worker_receive_ms", queueStartAt, workerReceiveAt)
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
	setMaxDuration(summary, key, durationToMilliseconds(end.Sub(start)))
}

func setMaxDuration(summary map[string]int64, key string, duration int64) {
	if key == "" || duration <= 0 {
		return
	}
	if current, ok := summary[key]; !ok || duration > current {
		summary[key] = duration
	}
}

func addDuration(summary map[string]int64, key string, duration int64) {
	if key == "" || duration <= 0 {
		return
	}
	summary[key] += duration
}

func incrementCount(summary map[string]int64, key string) {
	if key == "" {
		return
	}
	summary[key]++
}

func containerEventRecordDurationUs(event types.ContainerEventRecord) int64 {
	if event.Attrs != nil {
		if durationUs, err := strconv.ParseInt(event.Attrs[types.EventAttrDurationUs], 10, 64); err == nil && durationUs > 0 {
			return durationUs
		}
		if durationNs, err := strconv.ParseInt(event.Attrs[types.EventAttrDurationNs], 10, 64); err == nil && durationNs > 0 {
			return durationNs / 1000
		}
	}
	return event.DurationMs * 1000
}

func durationUsToMilliseconds(durationUs int64) int64 {
	if durationUs <= 0 {
		return 0
	}
	return (durationUs + 999) / 1000
}

func durationToMilliseconds(duration time.Duration) int64 {
	if duration <= 0 {
		return 0
	}
	return (duration.Nanoseconds() + int64(time.Millisecond) - 1) / int64(time.Millisecond)
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
		if event.StoredAtNs < uint64(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()) {
			return time.UnixMilli(int64(event.StoredAtNs)).UTC()
		}
		return time.Unix(0, int64(event.StoredAtNs)).UTC()
	default:
		return time.Time{}
	}
}

func requiredContainerLifecycleIDs(events []types.ContainerEventRecord) []string {
	seen := map[string]struct{}{}
	for _, event := range events {
		if event.Type == types.EventContainerLifecycle && event.EventID != "" {
			seen[event.EventID] = struct{}{}
		}
	}

	missing := []string{}
	for id, def := range types.ContainerLifecycleDefinitions {
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
