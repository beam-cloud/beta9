package repository

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/s2-streamstore/s2-sdk-go/s2"
	"k8s.io/utils/ptr"
)

const (
	historySkipTTL        = 10 * time.Minute
	historySkipMaxEntries = 4096
	historySkipMaxRanges  = 16
)

func (r *S2EventRepository) GetContainerEvents(ctx context.Context, containerID string, query types.EventQuery) (*types.ContainerEventsResponse, error) {
	query.ContainerID = containerID
	response := &types.ContainerEventsResponse{
		ContainerID: containerID,
		WorkspaceID: query.WorkspaceID,
		StubID:      query.StubID,
		Events:      []types.ContainerEventRecord{},
		Streams:     []string{},
	}

	if streamName := r.containerEventStream(query.WorkspaceID, query.StubID, containerID); streamName != "" {
		response.Streams = append(response.Streams, string(streamName))
		events, err := r.readNewestFirst(ctx, streamName, query, cmp.Or(query.Limit, defaultS2EventReadLimit), response)
		if err != nil {
			return nil, fmt.Errorf("read container events from s2 stream %q: %w", streamName, err)
		}
		response.Events = append(response.Events, events...)
	}

	sortContainerEventRecords(response.Events)
	setContainerStopCause(response)
	response.Summary = summarizeContainerLifecycleDurations(response.Events)
	response.Missing = requiredContainerLifecycleIDs(response.Events)
	return response, nil
}

func (r *S2EventRepository) GetEventHistory(ctx context.Context, query types.EventQuery) (*types.EventHistoryResponse, error) {
	limit := cmp.Or(query.Limit, defaultS2EventReadLimit)

	// Task history reads come from the multiplexed stub task stream, which
	// also carries task log records; keep them out of event history unless
	// the caller explicitly asked for them.
	if query.TaskID != "" && len(query.EventTypes) == 0 {
		query.ExcludeEventTypes = append(query.ExcludeEventTypes, types.EventContainerLog)
	}

	response := &types.EventHistoryResponse{
		Events:  []types.ContainerEventRecord{},
		Streams: []string{},
	}
	for _, streamName := range r.eventHistoryStreams(query) {
		if len(response.Events) > 0 {
			break
		}
		if err := r.readEventHistoryStream(ctx, streamName, query, limit, response); err != nil {
			return nil, err
		}
	}

	sortContainerEventRecords(response.Events)
	return response, nil
}

// eventHistoryStream returns the stream that serves a history query.
func (r *S2EventRepository) eventHistoryStream(query types.EventQuery) s2.StreamName {
	workspaceID := query.WorkspaceID
	switch {
	case query.ContainerID != "" && workspaceID != "":
		return r.containerEventStream(workspaceID, query.StubID, query.ContainerID)
	case query.TaskID != "" && workspaceID != "" && query.StubID != "":
		return r.stubTaskStreamName(workspaceID, query.StubID)
	case query.TaskID != "":
		return r.legacyTaskStreamName(query.TaskID)
	case query.AppID != "" && workspaceID != "":
		return r.appNamespaceStreamName(workspaceID, query.AppID)
	// Unlike compute history, there is no workspace-stream fallback when this
	// stream is empty: no failures is the usual answer, and dashboards poll it.
	case workspaceID != "" && allEventTypes(query.EventTypes, isStubStateEvent):
		return r.workspaceStubStateStreamName(workspaceID)
	case query.StubID != "" && workspaceID != "":
		return r.stubStreamName(workspaceID, query.StubID)
	case workspaceID != "" && allEventTypes(query.EventTypes, isComputeEvent):
		return r.workspaceComputeStreamName(workspaceID)
	case workspaceID != "":
		return r.workspaceStreamName(workspaceID)
	default:
		return ""
	}
}

// eventHistoryStreams lists the streams a history query reads, in order: the
// stream that serves it, then streams that held its events before that one
// existed. Each is read only while nothing has been found.
func (r *S2EventRepository) eventHistoryStreams(query types.EventQuery) []s2.StreamName {
	streams := []s2.StreamName{r.eventHistoryStream(query)}
	// The dense compute stream may be empty for workspaces whose events
	// predate it, and app namespace container records were originally only in
	// the workspace stream.
	if query.WorkspaceID != "" && (allEventTypes(query.EventTypes, isComputeEvent) ||
		query.AppID != "" && allEventTypes(query.EventTypes, isWorkspaceContainerRealtimeEvent)) {
		streams = append(streams, r.workspaceStreamName(query.WorkspaceID))
	}
	// Tasks that predate the multiplexed stub task stream only have records
	// in the legacy per-task stream.
	if query.TaskID != "" {
		streams = append(streams, r.legacyTaskStreamName(query.TaskID))
	}
	return uniqueStreams(streams...)
}

// allEventTypes reports whether a query names event types and all are of the
// kind is reports, so a stream holding only that kind can serve it.
func allEventTypes(eventTypes []string, is func(string) bool) bool {
	return len(eventTypes) > 0 && !slices.ContainsFunc(eventTypes, func(eventType string) bool {
		return !is(strings.TrimSpace(eventType))
	})
}

func (r *S2EventRepository) readEventHistoryStream(ctx context.Context, streamName s2.StreamName, query types.EventQuery, limit uint64, response *types.EventHistoryResponse) error {
	response.Streams = append(response.Streams, string(streamName))

	if eventHistoryQueryReadsFromTail(query) {
		events, err := r.readNewestFirst(ctx, streamName, query, limit-uint64(len(response.Events)), &types.ContainerEventsResponse{})
		if err != nil {
			return fmt.Errorf("read event history from s2 stream %q: %w", streamName, err)
		}
		response.Events = append(response.Events, events...)
		return nil
	}

	scan, err := r.startHistoryScan(ctx, streamName, query)
	if err != nil {
		return fmt.Errorf("read event history from s2 stream %q: %w", streamName, err)
	}
	defer scan.flush()

	opts := historyReadStart(query)
	for uint64(len(response.Events)) < limit {
		count := min(s2EventHistoryReadLimit, limit-uint64(len(response.Events)))
		if opts.SeqNum != nil {
			start, unchecked := scan.resume(*opts.SeqNum)
			opts.SeqNum, count = &start, min(count, unchecked)
		}
		opts.Count = &count

		batch, err := r.basin.Stream(streamName).Read(ctx, opts)
		if isS2ReadEmpty(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read event history from s2 stream %q: %w", streamName, err)
		}
		if len(batch.Records) == 0 {
			return nil
		}

		for _, record := range batch.Records {
			eventRecord, ok := eventRecordPassingFilters(record, query)
			scan.observe(record, ok)
			if !ok || !eventRecordInQueryWindow(eventRecord, query) {
				continue
			}
			augmentContainerEventResponse(&types.ContainerEventsResponse{}, &eventRecord)
			response.Events = append(response.Events, eventRecord)
			if uint64(len(response.Events)) >= limit {
				break
			}
		}

		next := batch.Records[len(batch.Records)-1].SeqNum + 1
		opts.SeqNum, opts.Timestamp = &next, nil
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

// historyReadStart positions a forward history read at the query's start, by
// sequence number or else by time, and ends it at the query's end.
func historyReadStart(query types.EventQuery) *s2.ReadOptions {
	opts := &s2.ReadOptions{Timestamp: query.Timestamp, Until: query.Until}
	if query.StartTime != nil {
		opts.Timestamp = ptr.To(uint64(query.StartTime.UTC().UnixMilli()))
	}
	if query.EndTime != nil {
		opts.Until = ptr.To(uint64(query.EndTime.UTC().UnixMilli()))
	}
	switch {
	case query.SeqNum != nil:
		opts.SeqNum, opts.Timestamp = ptr.To(*query.SeqNum), nil
	case opts.Timestamp == nil:
		opts.SeqNum = ptr.To(uint64(0))
	}
	return opts
}

// readNewestFirst returns up to limit events matching query, newest first.
func (r *S2EventRepository) readNewestFirst(ctx context.Context, streamName s2.StreamName, query types.EventQuery, limit uint64, response *types.ContainerEventsResponse) ([]types.ContainerEventRecord, error) {
	if limit == 0 {
		return nil, nil
	}

	var events []types.ContainerEventRecord
	err := r.scanStreamFromTail(ctx, streamName, max(limit, s2EventAggregateScanLimit), func(record s2.SequencedRecord) bool {
		if eventRecord, ok := matchEventRecord(record, query, response); ok {
			events = append(events, eventRecord)
		}
		return uint64(len(events)) < limit
	})
	return events, err
}

// scanStreamFromTail visits a stream's records newest-first; see scanSeqFromTail.
func (r *S2EventRepository) scanStreamFromTail(ctx context.Context, streamName s2.StreamName, scanLimit uint64, visit func(s2.SequencedRecord) bool) error {
	stream := r.basin.Stream(streamName)
	tail, err := stream.CheckTail(ctx)
	if err != nil {
		if isS2ReadEmpty(err) {
			return nil
		}
		return fmt.Errorf("check tail: %w", err)
	}
	read := func(ctx context.Context, seqNum, count uint64) (*s2.ReadBatch, error) {
		return stream.Read(ctx, &s2.ReadOptions{SeqNum: &seqNum, Count: &count})
	}
	return scanSeqFromTail(ctx, read, tail.Tail.SeqNum, s2EventHistoryReadLimit, scanLimit, visit)
}

// historySkips remembers, per stream and history query filters, sequence
// ranges in which no record passes the filters. Records never change once
// appended, so later reads with the same filters jump over these ranges
// instead of reading them again. Without this, a client polling from far
// behind the tail re-reads everything up to it on every poll when its filters
// match little or nothing there.
type historySkips struct {
	mu      sync.Mutex
	entries map[string]*historySkipEntry
}

type historySkipEntry struct {
	ranges seqRanges
	// probe is a record read along with the ranges. A stream deleted and
	// recreated under the same name reuses sequence numbers, so the ranges
	// only hold while the stream still has this record.
	probe  s2.StreamPosition
	usedAt time.Time
}

// seqRange is the half-open sequence range [from, to).
type seqRange struct{ from, to uint64 }

// seqRanges is sorted, disjoint and not adjacent.
type seqRanges []seqRange

// historyScan is one forward read of a stream. It jumps over the ranges known
// when it started and remembers each stretch it finds in which no record
// passes the filters.
type historyScan struct {
	skips  *historySkips
	key    string
	ranges seqRanges
	probe  s2.StreamPosition
	run    seqRange
	inRun  bool
}

// historySkipKey identifies the stream and filters ranges are found for. The
// time window, start position and limit only decide where a read starts and
// stops, so they are left out.
func historySkipKey(streamName s2.StreamName, query types.EventQuery) string {
	query.Limit = 0
	query.StartTime, query.EndTime = nil, nil
	query.SeqNum, query.Timestamp, query.TailOffset, query.Until = nil, nil, nil, nil
	query.WaitSeconds, query.Clamp = nil, nil
	filters, _ := json.Marshal(query)
	return string(streamName) + "\x00" + string(filters)
}

// startHistoryScan starts a forward read of streamName for query, dropping the
// known ranges if the stream no longer has the record they were found with.
func (r *S2EventRepository) startHistoryScan(ctx context.Context, streamName s2.StreamName, query types.EventQuery) (*historyScan, error) {
	scan := r.historySkips.scan(historySkipKey(streamName, query))
	if len(scan.ranges) == 0 {
		return scan, nil
	}
	held, err := r.streamHolds(ctx, streamName, scan.probe)
	if err != nil {
		return nil, err
	}
	if !held {
		r.historySkips.forget(scan.key)
		scan.ranges = nil
	}
	return scan, nil
}

// streamHolds reports whether the stream still has the record at position.
func (r *S2EventRepository) streamHolds(ctx context.Context, streamName s2.StreamName, position s2.StreamPosition) (bool, error) {
	count := uint64(1)
	batch, err := r.basin.Stream(streamName).Read(ctx, &s2.ReadOptions{SeqNum: &position.SeqNum, Count: &count})
	if isS2ReadEmpty(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return len(batch.Records) == 1 &&
		batch.Records[0].SeqNum == position.SeqNum &&
		batch.Records[0].Timestamp == position.Timestamp, nil
}

func (h *historySkips) scan(key string) *historyScan {
	h.mu.Lock()
	defer h.mu.Unlock()

	scan := &historyScan{skips: h, key: key}
	entry, ok := h.entries[key]
	if !ok {
		return scan
	}
	now := time.Now()
	if now.Sub(entry.usedAt) > historySkipTTL {
		delete(h.entries, key)
		return scan
	}
	entry.usedAt = now
	scan.ranges, scan.probe = slices.Clone(entry.ranges), entry.probe
	return scan
}

// add records that no record in r passes the filters for key, found while the
// stream had probe.
func (h *historySkips) add(key string, r seqRange, probe s2.StreamPosition) {
	h.mu.Lock()
	defer h.mu.Unlock()

	now := time.Now()
	entry, ok := h.entries[key]
	if !ok {
		if h.entries == nil {
			h.entries = map[string]*historySkipEntry{}
		}
		if len(h.entries) >= historySkipMaxEntries {
			h.evictLocked(now)
		}
		entry = &historySkipEntry{}
		h.entries[key] = entry
	}
	entry.ranges = entry.ranges.with(r)
	entry.probe, entry.usedAt = probe, now
}

func (h *historySkips) forget(key string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.entries, key)
}

// evictLocked makes room for an entry: expired entries go first, otherwise
// the least recently used.
func (h *historySkips) evictLocked(now time.Time) {
	oldestKey, oldestUse := "", now
	for key, entry := range h.entries {
		if now.Sub(entry.usedAt) > historySkipTTL {
			delete(h.entries, key)
			continue
		}
		if !entry.usedAt.After(oldestUse) {
			oldestKey, oldestUse = key, entry.usedAt
		}
	}
	if len(h.entries) >= historySkipMaxEntries {
		delete(h.entries, oldestKey)
	}
}

// next returns where a read meant to start at seq can start, past the range
// covering seq if any, and how many records it can read before the next range.
func (rs seqRanges) next(seq uint64) (start, count uint64) {
	start = seq
	for _, r := range rs {
		if start < r.from {
			return start, r.from - start
		}
		start = max(start, r.to)
	}
	return start, math.MaxUint64 - start
}

// with returns the ranges merged with r. Past historySkipMaxRanges the
// shortest is dropped, since it saves the least reading.
func (rs seqRanges) with(r seqRange) seqRanges {
	merged := make(seqRanges, 0, len(rs)+1)
	for _, known := range rs {
		if known.to < r.from || r.to < known.from {
			merged = append(merged, known)
			continue
		}
		r = seqRange{from: min(r.from, known.from), to: max(r.to, known.to)}
	}
	merged = append(merged, r)
	slices.SortFunc(merged, func(a, b seqRange) int { return cmp.Compare(a.from, b.from) })
	if len(merged) > historySkipMaxRanges {
		shortest := slices.MinFunc(merged, func(a, b seqRange) int { return cmp.Compare(a.to-a.from, b.to-b.from) })
		merged = slices.DeleteFunc(merged, func(r seqRange) bool { return r == shortest })
	}
	return merged
}

// resume returns where a read meant to start at seq should start and how many
// records it should read before the next known range. Records jumped over
// join the current stretch.
func (s *historyScan) resume(seq uint64) (start, count uint64) {
	start, count = s.ranges.next(seq)
	if start > seq {
		s.extend(seq, start)
	}
	return start, count
}

// observe records whether record passed the filters. A passing record ends
// the current stretch.
func (s *historyScan) observe(record s2.SequencedRecord, passed bool) {
	s.probe = s2.StreamPosition{SeqNum: record.SeqNum, Timestamp: record.Timestamp}
	if passed {
		s.flush()
		return
	}
	s.extend(record.SeqNum, record.SeqNum+1)
}

// flush remembers the current stretch.
func (s *historyScan) flush() {
	if s.inRun {
		s.skips.add(s.key, s.run, s.probe)
		s.inRun = false
	}
}

func (s *historyScan) extend(from, to uint64) {
	if !s.inRun {
		s.run, s.inRun = seqRange{from: from}, true
	}
	s.run.to = to
}

func (r *S2EventRepository) StreamContainerEvents(ctx context.Context, containerID string, query types.EventQuery) (EventStream, error) {
	if query.WorkspaceID == "" || query.StubID == "" {
		return nil, fmt.Errorf("workspace id and stub id are required to stream container events")
	}

	query.ContainerID = containerID
	return r.streamEvents(ctx, r.containerStreamName(query.WorkspaceID, query.StubID, containerID), containerID, query)
}

func (r *S2EventRepository) StreamStubEvents(ctx context.Context, query types.EventQuery) (EventStream, error) {
	if query.WorkspaceID == "" || query.StubID == "" {
		return nil, fmt.Errorf("workspace id and stub id are required to stream stub events")
	}
	return r.streamEvents(ctx, r.stubStreamName(query.WorkspaceID, query.StubID), "", query)
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
	return r.streamEvents(ctx, r.workspaceStreamName(query.WorkspaceID), "", query)
}

func (r *S2EventRepository) StreamAppNamespaceEvents(ctx context.Context, query types.EventQuery) (EventStream, error) {
	if query.WorkspaceID == "" || query.AppID == "" {
		return nil, fmt.Errorf("workspace id and app id are required to stream app namespace events")
	}
	return r.streamEvents(ctx, r.appNamespaceStreamName(query.WorkspaceID, query.AppID), "", query)
}

func (r *S2EventRepository) streamEvents(ctx context.Context, streamName s2.StreamName, containerID string, query types.EventQuery) (EventStream, error) {
	response := &types.ContainerEventsResponse{ContainerID: containerID, WorkspaceID: query.WorkspaceID, StubID: query.StubID}
	resume := query.Limit == 0 && query.Until == nil && query.WaitSeconds == nil
	stream, err := r.openSessionStream(ctx, streamName, s2EventReadOptions(query), resume, func(record s2.SequencedRecord) (types.ContainerEventRecord, bool) {
		return matchEventRecord(record, query, response)
	})
	if err != nil {
		return nil, fmt.Errorf("stream events from s2 stream %q: %w", streamName, err)
	}
	return stream, nil
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

// s2SessionStream is an EventStream over an S2 read session, yielding the
// records match accepts. If resume is set, a session that ends cleanly is
// reopened from the next record until the context is done.
type s2SessionStream struct {
	ctx     context.Context
	stream  *s2.StreamClient
	session *s2.ReadSession
	resume  bool
	match   func(s2.SequencedRecord) (types.ContainerEventRecord, bool)

	nextSeqNum *uint64
	current    types.ContainerEventRecord
	err        error
}

func (r *S2EventRepository) openSessionStream(ctx context.Context, streamName s2.StreamName, opts *s2.ReadOptions, resume bool, match func(s2.SequencedRecord) (types.ContainerEventRecord, bool)) (*s2SessionStream, error) {
	stream := r.basin.Stream(streamName)
	session, err := stream.ReadSession(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &s2SessionStream{ctx: ctx, stream: stream, session: session, resume: resume, match: match}, nil
}

func (s *s2SessionStream) Next() bool {
	for {
		for s.session.Next() {
			record := s.session.Record()
			s.nextSeqNum = ptr.To(record.SeqNum + 1)
			if current, ok := s.match(record); ok {
				s.current = current
				return true
			}
		}

		if err := s.session.Err(); err != nil {
			s.err = err
			return false
		}
		if !s.reopen() {
			return false
		}
	}
}

func (s *s2SessionStream) Record() types.ContainerEventRecord {
	return s.current
}

func (s *s2SessionStream) Err() error {
	return s.err
}

func (s *s2SessionStream) Close() error {
	return s.session.Close()
}

func (s *s2SessionStream) reopen() bool {
	if !s.resume || s.ctx.Err() != nil {
		return false
	}
	nextSeqNum := s.resumeSeqNum()
	if nextSeqNum == nil || !sleepWithContext(s.ctx, s2StreamResumeDelay) {
		return false
	}

	session, err := s.stream.ReadSession(s.ctx, &s2.ReadOptions{SeqNum: nextSeqNum, Clamp: ptr.To(true)})
	if err != nil {
		s.err = err
		return false
	}
	_ = s.session.Close()
	s.session = session
	return true
}

func (s *s2SessionStream) resumeSeqNum() *uint64 {
	if s.nextSeqNum != nil {
		return ptr.To(*s.nextSeqNum)
	}
	if position := s.session.NextReadPosition(); position != nil {
		return ptr.To(position.SeqNum)
	}
	if tail := s.session.LastObservedTail(); tail != nil {
		return ptr.To(tail.SeqNum)
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

func countOption(limit uint64) *uint64 {
	if limit == 0 {
		return nil
	}
	return &limit
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
