package repository

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/s2-streamstore/s2-sdk-go/s2"
	"k8s.io/utils/ptr"
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
