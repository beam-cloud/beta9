package repository

import (
	"encoding/json"
	"sort"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
	"github.com/s2-streamstore/s2-sdk-go/s2"
)

// matchEventRecord parses record if it matches the query, filling in the
// fields its payload adds.
func matchEventRecord(record s2.SequencedRecord, query types.EventQuery, response *types.ContainerEventsResponse) (types.ContainerEventRecord, bool) {
	eventRecord, ok := eventRecordPassingFilters(record, query)
	if !ok || !eventRecordInQueryWindow(eventRecord, query) {
		return types.ContainerEventRecord{}, false
	}
	augmentContainerEventResponse(response, &eventRecord)
	return eventRecord, true
}

// eventRecordPassingFilters parses record if it passes the query's filters,
// ignoring the time window.
func eventRecordPassingFilters(record s2.SequencedRecord, query types.EventQuery) (types.ContainerEventRecord, bool) {
	if eventRecordHeadersSkip(record, query) {
		return types.ContainerEventRecord{}, false
	}
	eventRecord, ok := containerEventRecordFromS2(record, query)
	return eventRecord, ok && eventRecordMatchesFilters(eventRecord, query)
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

// s2HeaderMismatch reports whether the query wants a value for the named
// header and the record has a different one.
func s2HeaderMismatch(record s2.SequencedRecord, name, want string) bool {
	if want == "" {
		return false
	}
	value, ok := s2RecordHeader(record, name)
	return ok && value != want
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
	return s2HeaderMismatch(record, "task_id", query.TaskID) ||
		s2HeaderMismatch(record, "container_id", query.ContainerID) ||
		s2HeaderMismatch(record, "stub_id", query.StubID) ||
		s2HeaderMismatch(record, "app_id", query.AppID)
}

// logRecordHeadersSkip is the log-query analog of eventRecordHeadersSkip.
func logRecordHeadersSkip(record s2.SequencedRecord, query types.LogQuery) bool {
	if eventType, ok := s2RecordHeader(record, "type"); ok &&
		eventType != types.EventContainerLog && eventType != types.EventPlatformLog {
		return true
	}
	return s2HeaderMismatch(record, "task_id", query.TaskID) ||
		s2HeaderMismatch(record, "container_id", query.ContainerID)
}

// containerEventRecordFromS2 parses a stored cloud event, rejecting types the
// query excludes and, when the query names a task, records of other tasks.
func containerEventRecordFromS2(record s2.SequencedRecord, query types.EventQuery) (types.ContainerEventRecord, bool) {
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
		log.Debug().Err(err).Str("container_id", query.ContainerID).Msg("failed to unmarshal cloud event envelope")
		return eventRecord, query.TaskID == ""
	}
	if !eventQueryAllowsType(query, envelope.Type) || (query.TaskID != "" && envelope.TaskID != query.TaskID) {
		return types.ContainerEventRecord{}, false
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
	return eventRecord, true
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

// eventRecordMatchesFilters checks everything but the query's time window,
// so its answer for a record never changes.
func eventRecordMatchesFilters(record types.ContainerEventRecord, query types.EventQuery) bool {
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
	return true
}

func eventRecordInQueryWindow(record types.ContainerEventRecord, query types.EventQuery) bool {
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

// augmentContainerEventResponse fills in the record fields its payload adds,
// and the response's workspace and stub if they are unset.
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
		record.MachineID = lifecycle.MachineID
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
		record.MachineID = event.MachineID
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
		record.MachineID = entry.MachineID
		record.Stream = entry.Stream
		record.Line = entry.Line
		record.PID = entry.PID
		record.ProcessArgs = entry.ProcessArgs
		record.ProcessCwd = entry.ProcessCwd
		record.ProcessSeq = entry.ProcessSeq
	case types.EventContainerMetrics:
		var metrics types.EventContainerMetricsSchema
		if err := json.Unmarshal(record.Data, &metrics); err != nil {
			return
		}
		record.ContainerID = metrics.ContainerID
		record.StubID = metrics.StubID
		record.StubType = metrics.StubType
		record.WorkspaceID = metrics.WorkspaceID
		record.AppID = metrics.AppID
		record.WorkerID = metrics.WorkerID
		record.MachineID = metrics.MachineID
	default:
		return
	}

	if response.WorkspaceID == "" {
		response.WorkspaceID = record.WorkspaceID
	}
	if response.StubID == "" {
		response.StubID = record.StubID
	}
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
