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
	"sync/atomic"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/rs/zerolog/log"
	"github.com/s2-streamstore/s2-sdk-go/s2"
)

const (
	defaultS2EventStreamPrefix = "events"
	defaultS2EventReadLimit    = 10000
	// maxStubCacheReadRecords bounds how many records ReadStubCacheRequiredContent
	// will page through. Required content is coalesced to a small number of events
	// per stub, so this is far above realistic sizes; it exists only to guarantee
	// the read terminates even if the stream is continuously appended.
	maxStubCacheReadRecords   = 200000
	s2EventHistoryReadLimit   = uint64(1000)
	s2EventQueueSize          = 16384
	s2EventBatchSize          = 256
	s2EventWriteTimeout       = 5 * time.Second
	s2EventEnqueueTimeout     = 250 * time.Millisecond
	s2EventFlushInterval      = 100 * time.Millisecond
	s2StreamResumeDelay       = time.Second
	s2ScopedWriteWarnEvery    = time.Minute
	s2EventAggregateScanLimit = 50000
)

var (
	lastS2WriteWarning       atomic.Int64
	lastScopedS2WriteWarning atomic.Int64
)

type S2EventRepository struct {
	basin              *s2.BasinClient
	streamPrefix       string
	queue              chan cloudevents.Event
	stubCacheContentMu sync.Mutex
	stubCacheContent   map[s2.StreamName]*stubCacheRequiredContentState
}

type ScopedS2EventRepository struct {
	streamPrefix string
	targets      []scopedS2EventTarget
	queue        chan cloudevents.Event
}

type scopedS2EventTarget struct {
	name   string
	prefix string
	basin  *s2.BasinClient
}

type stubCacheRequiredContentState struct {
	mu         sync.Mutex
	nextSeqNum uint64
	items      map[string]types.CacheRequiredContentItem
}

func NewScopedS2EventRepository(config types.S2Config) (*ScopedS2EventRepository, error) {
	if config.LogApiKey == "" && config.EventApiKey == "" {
		return nil, nil
	}
	if config.Basin == "" {
		return nil, fmt.Errorf("s2 basin is required when scoped s2 api keys are configured")
	}

	streamPrefix := strings.Trim(config.StreamPrefix, "/")
	if streamPrefix == "" {
		streamPrefix = defaultS2EventStreamPrefix
	}

	targets := make([]scopedS2EventTarget, 0, 2)
	addTarget := func(name, token, prefix string) {
		token = strings.TrimSpace(token)
		prefix = strings.Trim(strings.TrimSpace(prefix), "/")
		if token == "" || prefix == "" {
			return
		}
		client := s2.New(token, &s2.ClientOptions{
			RequestTimeout: s2EventWriteTimeout,
			RetryConfig: &s2.RetryConfig{
				MaxAttempts:       3,
				AppendRetryPolicy: s2.AppendRetryPolicyAll,
			},
		})
		targets = append(targets, scopedS2EventTarget{
			name:   name,
			prefix: prefix,
			basin:  client.Basin(config.Basin),
		})
	}
	addTarget("logs", config.LogApiKey, config.LogStreamPrefix)
	addTarget("events", config.EventApiKey, config.EventStreamPrefix)
	if len(targets) == 0 {
		return nil, nil
	}

	repo := &ScopedS2EventRepository{
		streamPrefix: streamPrefix,
		targets:      targets,
		queue:        make(chan cloudevents.Event, s2EventQueueSize),
	}
	go repo.runWriter()
	return repo, nil
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
		basin:            client.Basin(config.Basin),
		streamPrefix:     streamPrefix,
		queue:            make(chan cloudevents.Event, s2EventQueueSize),
		stubCacheContent: map[s2.StreamName]*stubCacheRequiredContentState{},
	}
	go repo.runWriter()

	return repo, nil
}

func (r *ScopedS2EventRepository) PushEvent(event cloudevents.Event) error {
	select {
	case r.queue <- event:
		return nil
	default:
	}

	timer := time.NewTimer(s2EventEnqueueTimeout)
	defer timer.Stop()

	select {
	case r.queue <- event:
		return nil
	case <-timer.C:
		return fmt.Errorf("scoped s2 event queue is full")
	}
}

func (r *ScopedS2EventRepository) runWriter() {
	ticker := time.NewTicker(s2EventFlushInterval)
	defer ticker.Stop()

	batch := make([]cloudevents.Event, 0, s2EventBatchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := r.appendEventBatch(batch); err != nil {
			warnScopedS2WriteFailure(err, len(batch))
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

func warnScopedS2WriteFailure(err error, eventCount int) {
	warnS2WriteFailureWith(&lastScopedS2WriteWarning, err, eventCount, "failed to append scoped event batch to s2")
}

func warnS2WriteFailure(err error, eventCount int) {
	warnS2WriteFailureWith(&lastS2WriteWarning, err, eventCount, "failed to append event batch to s2")
}

func warnS2WriteFailureWith(lastWarning *atomic.Int64, err error, eventCount int, message string) {
	now := time.Now()
	last := lastWarning.Load()
	if last != 0 && now.Sub(time.Unix(0, last)) < s2ScopedWriteWarnEvery {
		return
	}
	if !lastWarning.CompareAndSwap(last, now.UnixNano()) {
		return
	}

	log.Warn().Err(err).Int("event_count", eventCount).Msg(message)
}

func (r *ScopedS2EventRepository) appendEventBatch(events []cloudevents.Event) error {
	if len(events) == 0 {
		return nil
	}

	planner := &S2EventRepository{streamPrefix: r.streamPrefix}
	recordsByTarget := map[*scopedS2EventTarget]map[s2.StreamName][]s2.AppendRecord{}
	for _, event := range events {
		record, streamNames, err := planner.appendRecordForEvent(event)
		if err != nil {
			log.Debug().Err(err).Str("event_type", event.Type()).Msg("failed to build scoped s2 event record")
			continue
		}
		for _, streamName := range streamNames {
			target := r.targetForStream(streamName)
			if target == nil {
				continue
			}
			if recordsByTarget[target] == nil {
				recordsByTarget[target] = map[s2.StreamName][]s2.AppendRecord{}
			}
			recordsByTarget[target][streamName] = append(recordsByTarget[target][streamName], record)
		}
	}

	var streamErrs []error
	for target, recordsByStream := range recordsByTarget {
		for streamName, records := range recordsByStream {
			if len(records) == 0 {
				continue
			}
			if err := appendScopedS2Records(target.basin, streamName, records); err != nil {
				if isS2EventStreamDeletionPending(err) {
					log.Debug().Err(err).Str("stream", string(streamName)).Msg("dropping scoped event batch for stream pending deletion")
					continue
				}
				streamErrs = append(streamErrs, fmt.Errorf("append %d scoped events to s2 stream %q: %w", len(records), streamName, err))
			}
		}
	}
	return errors.Join(streamErrs...)
}

func (r *ScopedS2EventRepository) PushEventSync(event cloudevents.Event) error {
	return r.appendEventBatch([]cloudevents.Event{event})
}

func (r *ScopedS2EventRepository) targetForStream(streamName s2.StreamName) *scopedS2EventTarget {
	stream := strings.Trim(string(streamName), "/")
	for i := range r.targets {
		target := &r.targets[i]
		if stream == target.prefix || strings.HasPrefix(stream, target.prefix+"/") {
			return target
		}
	}
	return nil
}

func appendScopedS2Records(basin *s2.BasinClient, streamName s2.StreamName, records []s2.AppendRecord) error {
	ctx, cancel := s2EventWriteContext()
	defer cancel()
	_, err := basin.Stream(streamName).Append(ctx, &s2.AppendInput{Records: records})
	return err
}

func (r *S2EventRepository) PushEvent(event cloudevents.Event) error {
	select {
	case r.queue <- event:
		return nil
	default:
	}

	timer := time.NewTimer(s2EventEnqueueTimeout)
	defer timer.Stop()

	select {
	case r.queue <- event:
		return nil
	case <-timer.C:
		return fmt.Errorf("s2 event queue is full")
	}
}

func (r *S2EventRepository) PushEventSync(event cloudevents.Event) error {
	return r.appendEvent(event)
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
			warnS2WriteFailure(err, len(batch))
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

	recordsByStream := map[s2.StreamName][]s2.AppendRecord{}
	for _, event := range events {
		record, streamNames, err := r.appendRecordForEvent(event)
		if err != nil {
			log.Debug().Err(err).Str("event_type", event.Type()).Msg("failed to build s2 event record")
			continue
		}
		if len(streamNames) == 0 {
			continue
		}
		for _, streamName := range streamNames {
			recordsByStream[streamName] = append(recordsByStream[streamName], record)
		}
	}

	var streamErrs []error
	for streamName, records := range recordsByStream {
		if len(records) == 0 {
			continue
		}
		if err := r.appendRecordsForWrite(streamName, records); err != nil {
			if isS2EventStreamDeletionPending(err) {
				log.Debug().Err(err).Str("stream", string(streamName)).Msg("dropping event batch for stream pending deletion")
				continue
			}
			streamErrs = append(streamErrs, fmt.Errorf("append %d events to s2 stream %q: %w", len(records), streamName, err))
		}
	}

	return errors.Join(streamErrs...)
}

func (r *S2EventRepository) appendRecordForEvent(event cloudevents.Event) (s2.AppendRecord, []s2.StreamName, error) {
	body, err := json.Marshal(event)
	if err != nil {
		return s2.AppendRecord{}, nil, fmt.Errorf("marshal cloud event: %w", err)
	}

	metadata := eventMetadataFromCloudEvent(event)
	streamNames := r.streamNamesForEvent(event.Type(), metadata)
	if len(streamNames) == 0 {
		return s2.AppendRecord{}, nil, nil
	}

	timestamp := uint64(event.Time().UnixMilli())
	return s2.AppendRecord{
		Timestamp: &timestamp,
		Headers:   s2HeadersForEvent(event, metadata),
		Body:      body,
	}, streamNames, nil
}

func (r *S2EventRepository) GetContainerEvents(ctx context.Context, containerID string, query types.EventQuery) (*types.ContainerEventsResponse, error) {
	limit := query.Limit
	if limit == 0 {
		limit = defaultS2EventReadLimit
	}
	query.ContainerID = containerID

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

func (r *S2EventRepository) GetEventHistory(ctx context.Context, query types.EventQuery) (*types.EventHistoryResponse, error) {
	limit := query.Limit
	if limit == 0 {
		limit = defaultS2EventReadLimit
	}

	// Task history reads come from the multiplexed stub task stream, which
	// also carries task log records; keep them out of event history unless
	// the caller explicitly asked for them.
	if query.TaskID != "" && len(query.EventTypes) == 0 {
		query.ExcludeEventTypes = append(query.ExcludeEventTypes, types.EventContainerLog)
	}

	streams, err := r.resolveEventHistoryStreams(ctx, query)
	if err != nil {
		return nil, err
	}

	response := &types.EventHistoryResponse{
		Events:  []types.ContainerEventRecord{},
		Streams: make([]string, 0, len(streams)),
	}
	for _, streamName := range streams {
		if uint64(len(response.Events)) >= limit {
			break
		}
		if err := r.readEventHistoryStream(ctx, streamName, query, limit, response); err != nil {
			return nil, err
		}
	}

	// The dense compute stream may be empty for workspaces whose events
	// predate it; fall back to scanning the full workspace stream.
	if len(response.Events) == 0 && query.WorkspaceID != "" && allComputeEventTypes(query.EventTypes) {
		workspaceStream := r.workspaceStreamName(query.WorkspaceID)
		if !responseReadStream(response.Streams, workspaceStream) {
			if err := r.readEventHistoryStream(ctx, workspaceStream, query, limit, response); err != nil {
				return nil, err
			}
		}
	}

	// App-scoped container lifecycle/event records were originally available
	// through the workspace stream. Keep that path as a fallback so app
	// dashboards can read older sandbox/container history without per-stub scans.
	if len(response.Events) == 0 && query.AppID != "" && query.WorkspaceID != "" && allWorkspaceContainerRealtimeEventTypes(query.EventTypes) {
		workspaceStream := r.workspaceStreamName(query.WorkspaceID)
		if !responseReadStream(response.Streams, workspaceStream) {
			if err := r.readEventHistoryStream(ctx, workspaceStream, query, limit, response); err != nil {
				return nil, err
			}
		}
	}

	// Tasks that predate the multiplexed stub task stream only have records
	// in the legacy per-task stream.
	if len(response.Events) == 0 && query.TaskID != "" {
		legacyStream := r.legacyTaskStreamName(query.TaskID)
		if !responseReadStream(response.Streams, legacyStream) {
			if err := r.readEventHistoryStream(ctx, legacyStream, query, limit, response); err != nil {
				return nil, err
			}
		}
	}

	sortContainerEventRecords(response.Events)
	return response, nil
}

func responseReadStream(streams []string, streamName s2.StreamName) bool {
	for _, stream := range streams {
		if stream == string(streamName) {
			return true
		}
	}
	return false
}

// streamHasRecords reports whether the stream exists and holds at least one
// record. It is used to detect tasks whose records still live in legacy
// per-task streams.
func (r *S2EventRepository) streamHasRecords(ctx context.Context, streamName s2.StreamName) bool {
	tail, err := r.basin.Stream(streamName).CheckTail(ctx)
	if err != nil {
		return false
	}
	return tail.Tail.SeqNum > 0
}

func (r *S2EventRepository) StreamContainerEvents(ctx context.Context, containerID string, query types.EventQuery) (EventStream, error) {
	if query.WorkspaceID == "" || query.StubID == "" {
		return nil, fmt.Errorf("workspace id and stub id are required to stream container events")
	}

	query.ContainerID = containerID
	streamName := r.containerStreamName(query.WorkspaceID, query.StubID, containerID)
	return r.streamEvents(ctx, streamName, containerID, query)
}

func (r *S2EventRepository) StreamStubEvents(ctx context.Context, query types.EventQuery) (EventStream, error) {
	if query.WorkspaceID == "" || query.StubID == "" {
		return nil, fmt.Errorf("workspace id and stub id are required to stream stub events")
	}

	streamName := r.stubStreamName(query.WorkspaceID, query.StubID)
	return r.streamEvents(ctx, streamName, "", query)
}

func (r *S2EventRepository) StreamTaskEvents(ctx context.Context, query types.EventQuery) (EventStream, error) {
	if query.TaskID == "" {
		return nil, fmt.Errorf("task id is required to stream task events")
	}

	// Task records are multiplexed into the per-stub task stream and
	// demultiplexed by the task_id record header. Log records share the
	// stream, so exclude them unless the caller explicitly asked for them.
	if len(query.EventTypes) == 0 {
		query.ExcludeEventTypes = append(query.ExcludeEventTypes, types.EventContainerLog)
	}

	streamName := r.stubTaskStreamName(query.WorkspaceID, query.StubID)
	if query.WorkspaceID == "" || query.StubID == "" {
		streamName = r.legacyTaskStreamName(query.TaskID)
	} else if r.streamHasRecords(ctx, r.legacyTaskStreamName(query.TaskID)) {
		// Tasks that predate the multiplexed stub task stream have their
		// records in the legacy per-task stream.
		streamName = r.legacyTaskStreamName(query.TaskID)
	}
	return r.streamEvents(ctx, streamName, "", query)
}

func (r *S2EventRepository) StreamWorkspaceEvents(ctx context.Context, query types.EventQuery) (EventStream, error) {
	if query.WorkspaceID == "" {
		return nil, fmt.Errorf("workspace id is required to stream workspace events")
	}

	streamName := r.workspaceStreamName(query.WorkspaceID)
	return r.streamEvents(ctx, streamName, "", query)
}

func (r *S2EventRepository) StreamAppEvents(ctx context.Context, query types.EventQuery) (EventStream, error) {
	if query.WorkspaceID == "" || query.AppID == "" {
		return nil, fmt.Errorf("workspace id and app id are required to stream app events")
	}

	streamName := r.appStreamName(query.WorkspaceID, query.AppID)
	return r.streamEvents(ctx, streamName, "", query)
}

func (r *S2EventRepository) streamEvents(ctx context.Context, streamName s2.StreamName, containerID string, query types.EventQuery) (EventStream, error) {
	opts := s2EventReadOptions(query)
	session, err := r.basin.Stream(streamName).ReadSession(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("stream events from s2 stream %q: %w", streamName, err)
	}

	return &s2EventStream{
		ctx:        ctx,
		basin:      r.basin,
		streamName: streamName,
		session:    session,
		query:      query,
		response: &types.ContainerEventsResponse{
			ContainerID: containerID,
			WorkspaceID: query.WorkspaceID,
			StubID:      query.StubID,
		},
	}, nil
}

func (r *S2EventRepository) resolveContainerStreams(ctx context.Context, containerID string, query types.EventQuery) ([]s2.StreamName, error) {
	if query.WorkspaceID != "" && query.StubID != "" {
		return []s2.StreamName{r.containerStreamName(query.WorkspaceID, query.StubID, containerID)}, nil
	}

	if query.WorkspaceID == "" {
		return nil, nil
	}
	if stubID, ok := common.ExtractStubIdFromStubScopedContainerId(containerID); ok {
		return []s2.StreamName{r.containerStreamName(query.WorkspaceID, stubID, containerID)}, nil
	}
	return []s2.StreamName{r.containerAliasStreamName(query.WorkspaceID, containerID)}, nil
}

func (r *S2EventRepository) resolveEventHistoryStreams(ctx context.Context, query types.EventQuery) ([]s2.StreamName, error) {
	addKnown := func(streamName s2.StreamName) ([]s2.StreamName, error) {
		if streamName == "" {
			return nil, nil
		}
		return []s2.StreamName{streamName}, nil
	}

	switch {
	case query.ContainerID != "" && query.WorkspaceID != "":
		return r.resolveContainerStreams(ctx, query.ContainerID, query)
	case query.TaskID != "" && query.WorkspaceID != "" && query.StubID != "":
		return addKnown(r.stubTaskStreamName(query.WorkspaceID, query.StubID))
	case query.TaskID != "":
		return addKnown(r.legacyTaskStreamName(query.TaskID))
	case query.AppID != "" && query.WorkspaceID != "":
		return addKnown(r.appStreamName(query.WorkspaceID, query.AppID))
	case query.StubID != "" && query.WorkspaceID != "":
		return addKnown(r.stubStreamName(query.WorkspaceID, query.StubID))
	case query.WorkspaceID != "" && allComputeEventTypes(query.EventTypes):
		return addKnown(r.workspaceComputeStreamName(query.WorkspaceID))
	case query.WorkspaceID != "":
		return addKnown(r.workspaceStreamName(query.WorkspaceID))
	default:
		return nil, nil
	}
}

// allComputeEventTypes reports whether the query is scoped exclusively to
// compute.* event types, so it can be served from the dense compute stream.
func allComputeEventTypes(eventTypes []string) bool {
	if len(eventTypes) == 0 {
		return false
	}
	for _, eventType := range eventTypes {
		if !strings.HasPrefix(strings.TrimSpace(eventType), "compute.") {
			return false
		}
	}
	return true
}

func allWorkspaceContainerRealtimeEventTypes(eventTypes []string) bool {
	if len(eventTypes) == 0 {
		return false
	}
	for _, eventType := range eventTypes {
		eventType = strings.TrimSpace(eventType)
		if eventType != types.EventContainerMetrics &&
			eventType != types.EventContainerEvent &&
			eventType != types.EventContainerLifecycle {
			return false
		}
	}
	return true
}

func (r *S2EventRepository) readContainerStream(ctx context.Context, streamName s2.StreamName, limit uint64, query types.EventQuery, response *types.ContainerEventsResponse) error {
	response.Streams = append(response.Streams, string(streamName))

	tail, err := r.basin.Stream(streamName).CheckTail(ctx)
	if err != nil {
		if isS2ReadEmpty(err) {
			return nil
		}
		return fmt.Errorf("check tail for container events s2 stream %q: %w", streamName, err)
	}
	if tail.Tail.SeqNum == 0 {
		return nil
	}

	chunkSize := uint64(s2EventHistoryReadLimit)
	if chunkSize == 0 {
		chunkSize = 1000
	}
	scanLimit := uint64(s2EventAggregateScanLimit)
	if limit > scanLimit {
		scanLimit = limit
	}

	var recordsScanned uint64
	var scannedFromTail uint64
	for scannedFromTail < tail.Tail.SeqNum && recordsScanned < scanLimit && uint64(len(response.Events)) < limit {
		tailOffset, count := nextTailReadWindow(scannedFromTail, tail.Tail.SeqNum, chunkSize)
		tailOffsetValue := int64(tailOffset)
		batch, err := r.basin.Stream(streamName).Read(ctx, &s2.ReadOptions{
			TailOffset: &tailOffsetValue,
			Count:      &count,
		})
		if err != nil {
			if isS2ReadEmpty(err) {
				return nil
			}
			return fmt.Errorf("read container events from s2 stream %q: %w", streamName, err)
		}
		if len(batch.Records) == 0 {
			return nil
		}
		recordsScanned += uint64(len(batch.Records))
		scannedFromTail = tailOffset

		for i := len(batch.Records) - 1; i >= 0; i-- {
			if eventRecordHeadersSkip(batch.Records[i], query) {
				continue
			}
			eventRecord, ok := containerEventRecordFromS2(batch.Records[i], query, response)
			if !ok || !eventRecordMatchesQuery(eventRecord, query) {
				continue
			}
			augmentContainerEventResponse(response, &eventRecord)
			response.Events = append(response.Events, eventRecord)
			if uint64(len(response.Events)) >= limit {
				break
			}
		}
	}
	return nil
}

func (r *S2EventRepository) readEventHistoryStream(ctx context.Context, streamName s2.StreamName, query types.EventQuery, limit uint64, response *types.EventHistoryResponse) error {
	response.Streams = append(response.Streams, string(streamName))

	if eventHistoryQueryReadsFromTail(query) {
		return r.readEventHistoryStreamFromTail(ctx, streamName, query, limit, response)
	}

	var nextSeqNum *uint64
	if query.SeqNum != nil {
		seq := *query.SeqNum
		nextSeqNum = &seq
	}
	var startTimestamp *uint64
	if query.StartTime != nil {
		ts := uint64(query.StartTime.UTC().UnixMilli())
		startTimestamp = &ts
	} else if query.Timestamp != nil {
		ts := *query.Timestamp
		startTimestamp = &ts
	}
	var until *uint64
	if query.EndTime != nil {
		ts := uint64(query.EndTime.UTC().UnixMilli())
		until = &ts
	} else if query.Until != nil {
		ts := *query.Until
		until = &ts
	}

	for uint64(len(response.Events)) < limit {
		count := s2EventHistoryReadLimit
		if remaining := limit - uint64(len(response.Events)); remaining < count {
			count = remaining
		}
		opts := &s2.ReadOptions{
			Count: &count,
			Until: until,
		}
		if nextSeqNum != nil {
			opts.SeqNum = nextSeqNum
		} else if startTimestamp != nil {
			opts.Timestamp = startTimestamp
		} else {
			seq := uint64(0)
			opts.SeqNum = &seq
		}

		batch, err := r.basin.Stream(streamName).Read(ctx, opts)
		if err != nil {
			if isS2ReadEmpty(err) {
				return nil
			}
			return fmt.Errorf("read event history from s2 stream %q: %w", streamName, err)
		}
		if len(batch.Records) == 0 {
			return nil
		}

		for _, record := range batch.Records {
			if eventRecordHeadersSkip(record, query) {
				continue
			}
			eventRecord, ok := containerEventRecordFromS2(record, query, &types.ContainerEventsResponse{})
			if !ok || !eventRecordMatchesQuery(eventRecord, query) {
				continue
			}
			augmentContainerEventResponse(&types.ContainerEventsResponse{}, &eventRecord)
			response.Events = append(response.Events, eventRecord)
			if uint64(len(response.Events)) >= limit {
				break
			}
		}

		last := batch.Records[len(batch.Records)-1]
		seq := last.SeqNum + 1
		nextSeqNum = &seq
		startTimestamp = nil
		if len(batch.Records) < int(count) {
			return nil
		}
	}
	return nil
}

// eventHistoryQueryReadsFromTail reports whether a history query has no
// explicit start/end selector, in which case callers want the most recent
// events rather than the oldest records in the stream.
func eventHistoryQueryReadsFromTail(query types.EventQuery) bool {
	return query.SeqNum == nil &&
		query.StartTime == nil &&
		query.Timestamp == nil &&
		query.EndTime == nil &&
		query.Until == nil
}

// readEventHistoryStreamFromTail scans the stream newest-first so a limited
// history read returns the latest matching events instead of the first
// records ever written to the stream.
func (r *S2EventRepository) readEventHistoryStreamFromTail(ctx context.Context, streamName s2.StreamName, query types.EventQuery, limit uint64, response *types.EventHistoryResponse) error {
	tail, err := r.basin.Stream(streamName).CheckTail(ctx)
	if err != nil {
		if isS2ReadEmpty(err) {
			return nil
		}
		return fmt.Errorf("check tail for event history s2 stream %q: %w", streamName, err)
	}
	if tail.Tail.SeqNum == 0 {
		return nil
	}

	chunkSize := uint64(s2EventHistoryReadLimit)
	if chunkSize == 0 {
		chunkSize = 1000
	}
	scanLimit := uint64(s2EventAggregateScanLimit)
	if limit > scanLimit {
		scanLimit = limit
	}

	var recordsScanned uint64
	var scannedFromTail uint64
	for scannedFromTail < tail.Tail.SeqNum && recordsScanned < scanLimit && uint64(len(response.Events)) < limit {
		tailOffset, count := nextTailReadWindow(scannedFromTail, tail.Tail.SeqNum, chunkSize)
		tailOffsetValue := int64(tailOffset)
		batch, err := r.basin.Stream(streamName).Read(ctx, &s2.ReadOptions{
			TailOffset: &tailOffsetValue,
			Count:      &count,
		})
		if err != nil {
			if isS2ReadEmpty(err) {
				return nil
			}
			return fmt.Errorf("read event history from s2 stream %q: %w", streamName, err)
		}
		if len(batch.Records) == 0 {
			return nil
		}
		recordsScanned += uint64(len(batch.Records))
		scannedFromTail = tailOffset

		for i := len(batch.Records) - 1; i >= 0; i-- {
			if eventRecordHeadersSkip(batch.Records[i], query) {
				continue
			}
			eventRecord, ok := containerEventRecordFromS2(batch.Records[i], query, &types.ContainerEventsResponse{})
			if !ok || !eventRecordMatchesQuery(eventRecord, query) {
				continue
			}
			augmentContainerEventResponse(&types.ContainerEventsResponse{}, &eventRecord)
			response.Events = append(response.Events, eventRecord)
			if uint64(len(response.Events)) >= limit {
				break
			}
		}
	}
	return nil
}

type s2EventStream struct {
	ctx     context.Context
	basin   *s2.BasinClient
	session *s2.ReadSession

	streamName s2.StreamName
	query      types.EventQuery

	response   *types.ContainerEventsResponse
	nextSeqNum *uint64
	current    types.ContainerEventRecord
	err        error
}

func (s *s2EventStream) Next() bool {
	for {
		for s.session.Next() {
			record := s.session.Record()
			s.setNextSeqNum(record.SeqNum + 1)
			if eventRecordHeadersSkip(record, s.query) {
				continue
			}
			eventRecord, ok := containerEventRecordFromS2(record, s.query, s.response)
			if !ok || !eventRecordMatchesQuery(eventRecord, s.query) {
				continue
			}
			augmentContainerEventResponse(s.response, &eventRecord)
			s.current = eventRecord
			return true
		}

		if err := s.session.Err(); err != nil {
			s.err = err
			return false
		}
		if !s.reopenCleanSession() {
			return false
		}
	}
}

func (s *s2EventStream) Record() types.ContainerEventRecord {
	return s.current
}

func (s *s2EventStream) Err() error {
	return s.err
}

func (s *s2EventStream) Close() error {
	return s.session.Close()
}

func (s *s2EventStream) setNextSeqNum(seqNum uint64) {
	s.nextSeqNum = &seqNum
}

func (s *s2EventStream) reopenCleanSession() bool {
	if !s.shouldResumeCleanSession() {
		return false
	}

	nextSeqNum := s.resumeSeqNum()
	if nextSeqNum == nil {
		return false
	}
	if !sleepWithContext(s.ctx, s2StreamResumeDelay) {
		return false
	}

	query := s.query
	query.SeqNum = nextSeqNum
	query.Timestamp = nil
	query.TailOffset = nil
	clamp := true
	query.Clamp = &clamp

	session, err := s.basin.Stream(s.streamName).ReadSession(s.ctx, s2EventReadOptions(query))
	if err != nil {
		s.err = err
		return false
	}

	_ = s.session.Close()
	s.session = session
	s.query = query
	return true
}

func (s *s2EventStream) shouldResumeCleanSession() bool {
	if s.ctx.Err() != nil {
		return false
	}
	return s.query.Limit == 0 && s.query.Until == nil && s.query.WaitSeconds == nil
}

func (s *s2EventStream) resumeSeqNum() *uint64 {
	if s.nextSeqNum != nil {
		seqNum := *s.nextSeqNum
		return &seqNum
	}
	if position := s.session.NextReadPosition(); position != nil {
		seqNum := position.SeqNum
		return &seqNum
	}
	if tail := s.session.LastObservedTail(); tail != nil {
		seqNum := tail.SeqNum
		return &seqNum
	}
	return nil
}

func s2EventReadOptions(query types.EventQuery) *s2.ReadOptions {
	return &s2.ReadOptions{
		SeqNum:     query.SeqNum,
		Timestamp:  query.Timestamp,
		TailOffset: query.TailOffset,
		Count:      countOption(query.Limit),
		Until:      query.Until,
		Wait:       query.WaitSeconds,
		Clamp:      query.Clamp,
	}
}

func sleepWithContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// s2RecordHeader returns the value of the named record header, if present.
func s2RecordHeader(record s2.SequencedRecord, name string) (string, bool) {
	for _, header := range record.Headers {
		if string(header.Name) == name {
			return string(header.Value), true
		}
	}
	return "", false
}

// eventRecordHeadersSkip reports whether a record can be rejected for the
// query from its headers alone, without unmarshaling the JSON body. This is
// how individual tasks are demultiplexed cheaply out of the shared per-stub
// task stream. A record missing a header falls through to body-based
// filtering, so legacy records written without headers are still handled.
func eventRecordHeadersSkip(record s2.SequencedRecord, query types.EventQuery) bool {
	if eventType, ok := s2RecordHeader(record, "type"); ok && !eventQueryAllowsType(query, eventType) {
		return true
	}
	if query.TaskID != "" {
		if taskID, ok := s2RecordHeader(record, "task_id"); ok && taskID != query.TaskID {
			return true
		}
	}
	if query.ContainerID != "" {
		if containerID, ok := s2RecordHeader(record, "container_id"); ok && containerID != query.ContainerID {
			return true
		}
	}
	if query.StubID != "" {
		if stubID, ok := s2RecordHeader(record, "stub_id"); ok && stubID != query.StubID {
			return true
		}
	}
	if query.AppID != "" {
		if appID, ok := s2RecordHeader(record, "app_id"); ok && appID != query.AppID {
			return true
		}
	}
	return false
}

// logRecordHeadersSkip is the log-query analog of eventRecordHeadersSkip.
func logRecordHeadersSkip(record s2.SequencedRecord, query types.LogQuery) bool {
	if eventType, ok := s2RecordHeader(record, "type"); ok &&
		eventType != types.EventContainerLog && eventType != types.EventPlatformLog {
		return true
	}
	if query.TaskID != "" {
		if taskID, ok := s2RecordHeader(record, "task_id"); ok && taskID != query.TaskID {
			return true
		}
	}
	if query.ContainerID != "" {
		if containerID, ok := s2RecordHeader(record, "container_id"); ok && containerID != query.ContainerID {
			return true
		}
	}
	return false
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
		AppID       string          `json:"appid"`
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
	eventRecord.AppID = envelope.AppID
	eventRecord.WorkerID = envelope.WorkerID
	if !eventQueryAllowsType(query, eventRecord.Type) {
		return types.ContainerEventRecord{}, false
	}
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
	if eventRecord.AppID == "" {
		eventRecord.AppID = envelope.AppID
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
	if eventTypeMatchesAny(query.ExcludeEventTypes, eventType) {
		return false
	}
	if len(query.EventTypes) == 0 {
		return true
	}
	return eventTypeMatchesAny(query.EventTypes, eventType)
}

func eventTypeMatchesAny(patterns []string, eventType string) bool {
	for _, pattern := range patterns {
		pattern = strings.TrimSpace(pattern)
		if pattern == eventType {
			return true
		}
		if strings.HasSuffix(pattern, "*") && strings.HasPrefix(eventType, strings.TrimSuffix(pattern, "*")) {
			return true
		}
	}
	return false
}

func eventRecordMatchesQuery(record types.ContainerEventRecord, query types.EventQuery) bool {
	if query.WorkspaceID != "" && record.WorkspaceID != query.WorkspaceID {
		return false
	}
	if len(query.ExcludeActions) > 0 && eventRecordActionExcluded(record, query.ExcludeActions) {
		return false
	}
	if query.StubID != "" && record.StubID != query.StubID {
		return false
	}
	if query.AppID != "" && record.AppID != query.AppID {
		return false
	}
	if query.TaskID != "" && record.TaskID != query.TaskID {
		return false
	}
	if query.ContainerID != "" && record.ContainerID != query.ContainerID {
		return false
	}
	eventTime := record.Timestamp
	if eventTime.IsZero() {
		eventTime = record.StartTime
	}
	if query.StartTime != nil && !eventTime.IsZero() && eventTime.Before(query.StartTime.UTC()) {
		return false
	}
	if query.EndTime != nil && !eventTime.IsZero() && !eventTime.Before(query.EndTime.UTC()) {
		return false
	}
	return true
}

// eventRecordActionExcluded reports whether the record's payload "action"
// field matches one of the excluded actions.
func eventRecordActionExcluded(record types.ContainerEventRecord, excludeActions []string) bool {
	if len(record.Data) == 0 {
		return false
	}
	var payload struct {
		Action string `json:"action"`
	}
	if err := json.Unmarshal(record.Data, &payload); err != nil || payload.Action == "" {
		return false
	}
	for _, action := range excludeActions {
		if strings.TrimSpace(action) == payload.Action {
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
	if eventType == types.EventContainerLog {
		return r.primaryLogStreamName(metadata)
	}
	if eventType == types.EventPlatformLog {
		return r.platformLogStreamName(metadata)
	}
	if eventType == types.EventStubCacheRequiredContent {
		if metadata.WorkspaceID != "" && metadata.StubID != "" {
			return r.stubCacheStreamName(metadata.WorkspaceID, metadata.StubID)
		}
		return ""
	}
	if eventType == types.EventPlatformCache {
		return r.platformCacheStreamName()
	}

	switch {
	case isTaskEvent(eventType) && metadata.TaskID != "":
		return r.taskEventStreamName(metadata)
	case metadata.ContainerID != "" && metadata.WorkspaceID != "" && metadata.StubID != "":
		return r.containerStreamName(metadata.WorkspaceID, metadata.StubID, metadata.ContainerID)
	case strings.HasPrefix(eventType, "container.") && metadata.ContainerID != "":
		return ""
	case metadata.TaskID != "":
		return r.taskEventStreamName(metadata)
	case isComputeEvent(eventType) && metadata.WorkspaceID != "":
		return r.workspaceStreamName(metadata.WorkspaceID)
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

func (r *S2EventRepository) streamNamesForEvent(eventType string, metadata eventMetadata) []s2.StreamName {
	if eventType == types.EventContainerLog {
		return r.logStreamNamesForEvent(metadata)
	}
	if eventType == types.EventPlatformLog {
		stream := r.platformLogStreamName(metadata)
		if stream == "" {
			return nil
		}
		streams := []s2.StreamName{stream}
		if metadata.WorkspaceID != "" {
			streams = append(streams, r.workspaceLogStreamName(metadata.WorkspaceID))
		}
		return streams
	}
	// Required-content reports are persisted only to the dedicated stub cache
	// stream; they must not fan out to the stub/workspace event streams.
	if eventType == types.EventStubCacheRequiredContent {
		stream := r.streamNameForEvent(eventType, metadata)
		if stream == "" {
			return nil
		}
		return []s2.StreamName{stream}
	}
	if eventType == types.EventPlatformCache {
		return []s2.StreamName{r.platformCacheStreamName()}
	}

	streams := []s2.StreamName{}
	add := func(stream s2.StreamName) {
		if stream == "" {
			return
		}
		for _, existing := range streams {
			if existing == stream {
				return
			}
		}
		streams = append(streams, stream)
	}

	add(r.streamNameForEvent(eventType, metadata))
	if shouldWriteContainerAlias(metadata) {
		add(r.containerAliasStreamName(metadata.WorkspaceID, metadata.ContainerID))
	}
	if isTaskEvent(eventType) && metadata.ContainerID != "" && metadata.WorkspaceID != "" && metadata.StubID != "" {
		add(r.containerStreamName(metadata.WorkspaceID, metadata.StubID, metadata.ContainerID))
	}
	if metadata.WorkspaceID != "" && metadata.StubID != "" {
		add(r.stubStreamName(metadata.WorkspaceID, metadata.StubID))
	}
	if isStubEvent(eventType) && metadata.WorkspaceID != "" {
		add(r.workspaceStreamName(metadata.WorkspaceID))
	}
	if isWorkspaceContainerRealtimeEvent(eventType) && metadata.WorkspaceID != "" {
		add(r.workspaceStreamName(metadata.WorkspaceID))
		if metadata.AppID != "" {
			add(r.appStreamName(metadata.WorkspaceID, metadata.AppID))
		}
	}
	if isComputeEvent(eventType) && metadata.WorkspaceID != "" {
		// Workspace stream powers the live workspace SSE; the compute stream keeps
		// a dense history for fast compute-only history queries. Heartbeats fire
		// every few seconds per machine and would drown out lifecycle events in
		// the history stream, so they only go to the live workspace stream.
		add(r.workspaceStreamName(metadata.WorkspaceID))
		if metadata.Action != types.EventComputeActionMachineHeartbeat {
			add(r.workspaceComputeStreamName(metadata.WorkspaceID))
		}
	}
	if isTaskEvent(eventType) && metadata.WorkspaceID != "" {
		add(r.workspaceStreamName(metadata.WorkspaceID))
		if metadata.AppID != "" {
			add(r.appStreamName(metadata.WorkspaceID, metadata.AppID))
		}
	}
	return streams
}

func isTaskEvent(eventType string) bool {
	return eventType == types.EventTaskCreated || eventType == types.EventTaskUpdated
}

func isStubEvent(eventType string) bool {
	return strings.HasPrefix(eventType, "stub.")
}

func isWorkspaceContainerRealtimeEvent(eventType string) bool {
	return eventType == types.EventContainerMetrics ||
		eventType == types.EventContainerEvent ||
		eventType == types.EventContainerLifecycle
}

func isComputeEvent(eventType string) bool {
	return strings.HasPrefix(eventType, "compute.")
}

func (r *S2EventRepository) primaryLogStreamName(metadata eventMetadata) s2.StreamName {
	switch {
	case metadata.ContainerID != "" && metadata.WorkspaceID != "" && metadata.StubID != "":
		return r.containerLogStreamName(metadata.WorkspaceID, metadata.StubID, metadata.ContainerID)
	case metadata.TaskID != "" && metadata.WorkspaceID != "" && metadata.StubID != "":
		return r.stubTaskStreamName(metadata.WorkspaceID, metadata.StubID)
	case metadata.TaskID != "" && metadata.WorkspaceID != "":
		return r.workspaceLogStreamName(metadata.WorkspaceID)
	case metadata.StubID != "" && metadata.WorkspaceID != "":
		return r.stubLogStreamName(metadata.WorkspaceID, metadata.StubID)
	case metadata.AppID != "" && metadata.WorkspaceID != "":
		return r.appLogStreamName(metadata.WorkspaceID, metadata.AppID)
	case metadata.WorkspaceID != "":
		return r.workspaceLogStreamName(metadata.WorkspaceID)
	default:
		return ""
	}
}

func (r *S2EventRepository) logStreamNamesForEvent(metadata eventMetadata) []s2.StreamName {
	streams := []s2.StreamName{}
	add := func(stream s2.StreamName) {
		if stream == "" {
			return
		}
		for _, existing := range streams {
			if existing == stream {
				return
			}
		}
		streams = append(streams, stream)
	}

	if metadata.WorkspaceID == "" {
		return streams
	}
	if metadata.ContainerID != "" && metadata.StubID != "" {
		add(r.containerLogStreamName(metadata.WorkspaceID, metadata.StubID, metadata.ContainerID))
	}
	if shouldWriteContainerAlias(metadata) {
		add(r.containerLogAliasStreamName(metadata.WorkspaceID, metadata.ContainerID))
	}
	if metadata.StubID != "" {
		add(r.stubLogStreamName(metadata.WorkspaceID, metadata.StubID))
	}
	// Task-attributed logs are multiplexed into the per-stub task stream
	// (demultiplexed on read via the task_id record header) instead of a
	// per-task stream, so stream creation does not scale with task count.
	if metadata.TaskID != "" && metadata.StubID != "" {
		add(r.stubTaskStreamName(metadata.WorkspaceID, metadata.StubID))
	}
	if metadata.AppID != "" {
		add(r.appLogStreamName(metadata.WorkspaceID, metadata.AppID))
	}
	add(r.workspaceLogStreamName(metadata.WorkspaceID))
	return streams
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

func shouldWriteContainerAlias(metadata eventMetadata) bool {
	if metadata.ContainerID == "" || metadata.WorkspaceID == "" {
		return false
	}

	stubID, ok := common.ExtractStubIdFromStubScopedContainerId(metadata.ContainerID)
	if !ok {
		return true
	}
	return metadata.StubID == "" || metadata.StubID != stubID
}

func (r *S2EventRepository) containerAliasStreamName(workspaceID, containerID string) s2.StreamName {
	return s2.StreamName(fmt.Sprintf(
		"%s/workspaces/%s/containers/%s",
		r.streamPrefix,
		eventStreamPart(workspaceID),
		eventStreamPart(containerID),
	))
}

// stubTaskStreamName is the multiplexed per-stub stream carrying all task
// records (lifecycle events and task-attributed logs) for every task of a
// stub. Individual tasks are demultiplexed on read via the task_id and type
// record headers, so stream count scales with stubs instead of tasks.
func (r *S2EventRepository) stubTaskStreamName(workspaceID, stubID string) s2.StreamName {
	return s2.StreamName(fmt.Sprintf("%s/workspaces/%s/stubs/%s/tasks", r.streamPrefix, eventStreamPart(workspaceID), eventStreamPart(stubID)))
}

// taskEventStreamName routes a task-scoped event to the multiplexed stub task
// stream, falling back to the workspace stream when stub metadata is missing.
func (r *S2EventRepository) taskEventStreamName(metadata eventMetadata) s2.StreamName {
	if metadata.WorkspaceID != "" && metadata.StubID != "" {
		return r.stubTaskStreamName(metadata.WorkspaceID, metadata.StubID)
	}
	if metadata.WorkspaceID != "" {
		return r.workspaceStreamName(metadata.WorkspaceID)
	}
	return ""
}

// legacyTaskStreamName is the retired per-task event stream; it is only used
// as a read fallback for tasks that predate the multiplexed stub task stream.
func (r *S2EventRepository) legacyTaskStreamName(taskID string) s2.StreamName {
	return s2.StreamName(fmt.Sprintf("%s/tasks/%s", r.streamPrefix, eventStreamPart(taskID)))
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

// workspaceComputeStreamName is a dedicated, compute-only stream so that compute
// event history reads stay dense and fast instead of paging through the busy
// shared workspace stream (which also carries container metrics/lifecycle).
func (r *S2EventRepository) workspaceComputeStreamName(workspaceID string) s2.StreamName {
	return s2.StreamName(fmt.Sprintf("%s/workspaces/%s/compute", r.streamPrefix, eventStreamPart(workspaceID)))
}

func (r *S2EventRepository) stubStreamName(workspaceID, stubID string) s2.StreamName {
	return s2.StreamName(fmt.Sprintf("%s/workspaces/%s/stubs/%s", r.streamPrefix, eventStreamPart(workspaceID), eventStreamPart(stubID)))
}

func (r *S2EventRepository) appStreamName(workspaceID, appID string) s2.StreamName {
	return s2.StreamName(fmt.Sprintf("%s/workspaces/%s/apps/%s", r.streamPrefix, eventStreamPart(workspaceID), eventStreamPart(appID)))
}

func (r *S2EventRepository) stubCacheStreamName(workspaceID, stubID string) s2.StreamName {
	return s2.StreamName(fmt.Sprintf("%s/workspaces/%s/stubs/%s/cache", r.streamPrefix, eventStreamPart(workspaceID), eventStreamPart(stubID)))
}

func (r *S2EventRepository) platformCacheStreamName() s2.StreamName {
	return s2.StreamName(fmt.Sprintf("%s/platform/cache", r.streamPrefix))
}

// ReadStubCacheRequiredContent reads the coalesced required-content set for a
// stub from the dedicated S2 cache stream. Items are merged by
// (hash, routing_key) keeping the most recent report for each.
func (r *S2EventRepository) ReadStubCacheRequiredContent(ctx context.Context, workspaceID, stubID string) ([]types.CacheRequiredContentItem, error) {
	if workspaceID == "" || stubID == "" {
		return nil, fmt.Errorf("workspace id and stub id are required to read stub cache required content")
	}

	streamName := r.stubCacheStreamName(workspaceID, stubID)
	state := r.stubCacheRequiredContentState(streamName)
	state.mu.Lock()
	defer state.mu.Unlock()

	if state.items == nil {
		state.items = map[string]types.CacheRequiredContentItem{}
	}

	// Read only records appended since this repository last coalesced the stub.
	// Reconciliation calls this repeatedly for recent stubs; restarting from
	// sequence 0 every cycle makes read bytes grow with stream age.
	recordsRead := 0
	for recordsRead < maxStubCacheReadRecords {
		count := uint64(defaultS2EventReadLimit)
		batch, err := r.basin.Stream(streamName).Read(ctx, &s2.ReadOptions{
			SeqNum: &state.nextSeqNum,
			Count:  &count,
		})
		if err != nil {
			if isS2ReadEmpty(err) {
				break
			}
			return nil, fmt.Errorf("read stub cache required content from s2 stream %q: %w", streamName, err)
		}
		if len(batch.Records) == 0 {
			break
		}
		recordsRead += len(batch.Records)

		for _, record := range batch.Records {
			mergeStubCacheRequiredContentRecord(state.items, record.Body)
		}

		state.nextSeqNum = batch.Records[len(batch.Records)-1].SeqNum + 1
		if uint64(len(batch.Records)) < count {
			break
		}
	}
	if recordsRead >= maxStubCacheReadRecords {
		log.Warn().Str("stream", string(streamName)).Int("records_read", recordsRead).Msg("stub cache required-content read hit record cap; result may be partial")
	}

	return stubCacheRequiredContentItems(state.items), nil
}

func (r *S2EventRepository) stubCacheRequiredContentState(streamName s2.StreamName) *stubCacheRequiredContentState {
	r.stubCacheContentMu.Lock()
	defer r.stubCacheContentMu.Unlock()

	if r.stubCacheContent == nil {
		r.stubCacheContent = map[s2.StreamName]*stubCacheRequiredContentState{}
	}
	state := r.stubCacheContent[streamName]
	if state == nil {
		state = &stubCacheRequiredContentState{}
		r.stubCacheContent[streamName] = state
	}
	return state
}

func mergeStubCacheRequiredContentRecord(merged map[string]types.CacheRequiredContentItem, body []byte) {
	var envelope struct {
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return
	}
	if envelope.Type != types.EventStubCacheRequiredContent {
		return
	}
	var schema types.EventStubCacheRequiredContentSchema
	if err := json.Unmarshal(envelope.Data, &schema); err != nil {
		return
	}
	for _, item := range schema.Items {
		if item.Hash == "" {
			continue
		}
		if item.Kind == "" {
			item.Kind = schema.Kind
		}
		merged[item.Hash+"\x00"+item.RoutingKey] = item
	}
}

func stubCacheRequiredContentItems(merged map[string]types.CacheRequiredContentItem) []types.CacheRequiredContentItem {
	items := make([]types.CacheRequiredContentItem, 0, len(merged))
	for _, item := range merged {
		items = append(items, item)
	}
	return items
}

func (r *S2EventRepository) containerLogStreamName(workspaceID, stubID, containerID string) s2.StreamName {
	return s2.StreamName(fmt.Sprintf(
		"%s/logs/workspaces/%s/stubs/%s/containers/%s",
		r.streamPrefix,
		eventStreamPart(workspaceID),
		eventStreamPart(stubID),
		eventStreamPart(containerID),
	))
}

func (r *S2EventRepository) containerLogAliasStreamName(workspaceID, containerID string) s2.StreamName {
	return s2.StreamName(fmt.Sprintf(
		"%s/logs/workspaces/%s/containers/%s",
		r.streamPrefix,
		eventStreamPart(workspaceID),
		eventStreamPart(containerID),
	))
}

func (r *S2EventRepository) stubLogStreamName(workspaceID, stubID string) s2.StreamName {
	return s2.StreamName(fmt.Sprintf("%s/logs/workspaces/%s/stubs/%s", r.streamPrefix, eventStreamPart(workspaceID), eventStreamPart(stubID)))
}

// legacyTaskLogStreamName is the retired per-task log stream; it is only used
// as a read fallback for tasks that predate the multiplexed stub task stream.
func (r *S2EventRepository) legacyTaskLogStreamName(workspaceID, taskID string) s2.StreamName {
	return s2.StreamName(fmt.Sprintf("%s/logs/workspaces/%s/tasks/%s", r.streamPrefix, eventStreamPart(workspaceID), eventStreamPart(taskID)))
}

func (r *S2EventRepository) appLogStreamName(workspaceID, appID string) s2.StreamName {
	return s2.StreamName(fmt.Sprintf("%s/logs/workspaces/%s/apps/%s", r.streamPrefix, eventStreamPart(workspaceID), eventStreamPart(appID)))
}

func (r *S2EventRepository) workspaceLogStreamName(workspaceID string) s2.StreamName {
	return s2.StreamName(fmt.Sprintf("%s/logs/workspaces/%s", r.streamPrefix, eventStreamPart(workspaceID)))
}

func (r *S2EventRepository) platformLogStreamName(metadata eventMetadata) s2.StreamName {
	switch {
	case metadata.WorkerID != "":
		return s2.StreamName(fmt.Sprintf("%s/logs/platform/workers/%s", r.streamPrefix, eventStreamPart(metadata.WorkerID)))
	case metadata.ServiceName != "" && metadata.InstanceID != "":
		return s2.StreamName(fmt.Sprintf("%s/logs/platform/services/%s/%s", r.streamPrefix, eventStreamPart(metadata.ServiceName), eventStreamPart(metadata.InstanceID)))
	case metadata.ServiceName != "":
		return s2.StreamName(fmt.Sprintf("%s/logs/platform/services/%s", r.streamPrefix, eventStreamPart(metadata.ServiceName)))
	default:
		return ""
	}
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
		MachineID:   extensionString(extensions, "machineid"),
		RouteID:     extensionString(extensions, "routeid"),
		AppID:       extensionString(extensions, "appid"),
		ServiceName: extensionString(extensions, "servicename"),
		InstanceID:  extensionString(extensions, "instanceid"),
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
	if metadata.MachineID != "" {
		headers = append(headers, s2.NewHeader("machine_id", metadata.MachineID))
	}
	if metadata.RouteID != "" {
		headers = append(headers, s2.NewHeader("route_id", metadata.RouteID))
	}
	if metadata.ServiceName != "" {
		headers = append(headers, s2.NewHeader("service", metadata.ServiceName))
	}
	if metadata.InstanceID != "" {
		headers = append(headers, s2.NewHeader("instance_id", metadata.InstanceID))
	}
	if metadata.AppID != "" {
		headers = append(headers, s2.NewHeader("app_id", metadata.AppID))
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
		record.AppID = firstNonEmpty(lifecycle.AppID, record.AppID)
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
		record.AppID = firstNonEmpty(event.AppID, record.AppID)
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
		record.AppID = entry.AppID
		record.WorkerID = entry.WorkerID
		record.Stream = entry.Stream
		record.Line = entry.Line
		record.PID = entry.PID
		record.ProcessArgs = entry.ProcessArgs
		record.ProcessCwd = entry.ProcessCwd
		record.ProcessSeq = entry.ProcessSeq
		if response.WorkspaceID == "" {
			response.WorkspaceID = entry.WorkspaceID
		}
		if response.StubID == "" {
			response.StubID = entry.StubID
		}
	case types.EventContainerMetrics:
		var metrics types.EventContainerMetricsSchema
		if err := json.Unmarshal(record.Data, &metrics); err != nil {
			return
		}
		record.ContainerID = metrics.ContainerID
		record.StubID = metrics.StubID
		record.StubType = metrics.StubType
		record.WorkspaceID = metrics.WorkspaceID
		record.WorkerID = metrics.WorkerID
		if response.WorkspaceID == "" {
			response.WorkspaceID = metrics.WorkspaceID
		}
		if response.StubID == "" {
			response.StubID = metrics.StubID
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
			if event.EventID == string(types.ContainerEventLogsFirstByte) && !event.Timestamp.IsZero() {
				if !runningAt.IsZero() && event.Timestamp.After(runningAt) && firstLogAfterRunning.IsZero() {
					firstLogAfterRunning = event.Timestamp
				}
				if !startTaskAt.IsZero() && event.Timestamp.After(startTaskAt) && firstLogAfterStartTask.IsZero() {
					firstLogAfterStartTask = event.Timestamp
				}
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
