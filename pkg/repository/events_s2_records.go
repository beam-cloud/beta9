package repository

import (
	"cmp"
	"encoding/json"
	"sort"
	"strconv"
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

// summarizeContainerLifecycleDurations summarizes sorted container events as
// durations in milliseconds, keyed by phase.
func summarizeContainerLifecycleDurations(events []types.ContainerEventRecord) map[string]int64 {
	summary := map[string]int64{}
	var timeline containerTimeline
	for _, event := range events {
		timeline.observe(event)
		if event.Type == types.EventContainerLifecycle {
			addLifecycleDuration(summary, event)
		}
	}
	setRollupDurations(summary)
	timeline.setDurations(summary)
	return summary
}

// containerTimeline holds the moments in a container's startup that summary
// durations are measured between.
type containerTimeline struct {
	firstEvent     time.Time
	queuePush      time.Time
	workerReceive  time.Time
	running        time.Time
	processStarted time.Time
	moduleLoaded   time.Time
	mainEntered    time.Time
	startTask      time.Time

	firstLogAfterRunning   time.Time
	firstLogAfterStartTask time.Time
}

func (t *containerTimeline) observe(event types.ContainerEventRecord) {
	if t.firstEvent.IsZero() {
		t.firstEvent = containerEventRecordTime(event)
	}

	switch event.Type {
	case types.EventContainerLifecycle:
		switch types.ContainerLifecycleID(event.EventID) {
		case types.ContainerLifecycleStartup:
			if !event.EndTime.IsZero() {
				t.running = event.EndTime
			}
		case types.ContainerLifecycleSchedulerQueuePush:
			if t.queuePush.IsZero() {
				t.queuePush = event.StartTime
			}
		case types.ContainerLifecycleWorkerQueueReceive:
			if t.workerReceive.IsZero() {
				t.workerReceive = event.StartTime
			}
		}
	case types.EventContainerEvent:
		if event.Timestamp.IsZero() {
			return
		}
		switch types.ContainerEventID(event.EventID) {
		case types.ContainerEventRunnerStartTask:
			t.startTask = event.Timestamp
		case types.ContainerEventLogsFirstByte:
			t.observeLog(event.Timestamp)
		case types.ContainerEventRunnerProcessStarted:
			t.processStarted = event.Timestamp
		case types.ContainerEventRunnerModuleLoaded:
			t.moduleLoaded = event.Timestamp
		case types.ContainerEventRunnerMainEntered:
			t.mainEntered = event.Timestamp
		}
	case types.EventContainerLog:
		t.observeLog(event.Timestamp)
	}
}

func (t *containerTimeline) observeLog(at time.Time) {
	if t.firstLogAfterRunning.IsZero() && !t.running.IsZero() && at.After(t.running) {
		t.firstLogAfterRunning = at
	}
	if t.firstLogAfterStartTask.IsZero() && !t.startTask.IsZero() && at.After(t.startTask) {
		t.firstLogAfterStartTask = at
	}
}

func (t containerTimeline) setDurations(summary map[string]int64) {
	setPositiveDuration(summary, "container_request_to_running_ms", t.firstEvent, t.running)
	setPositiveDuration(summary, "to_running_ms", t.firstEvent, t.running)
	setPositiveDuration(summary, "scheduler_queue_to_running_ms", t.queuePush, t.running)
	setPositiveDuration(summary, "worker_receive_to_running_ms", t.workerReceive, t.running)
	setPositiveDuration(summary, "running_to_runner_process_started_ms", t.running, t.processStarted)
	setPositiveDuration(summary, "running_to_runner_main_ms", t.running, t.mainEntered)
	setPositiveDuration(summary, "scheduler_queue_to_worker_receive_ms", t.queuePush, t.workerReceive)
	setPositiveDuration(summary, "runner_process_to_module_loaded_ms", t.processStarted, t.moduleLoaded)
	setPositiveDuration(summary, "runner_module_loaded_to_main_ms", t.moduleLoaded, t.mainEntered)
	setPositiveDuration(summary, "runner_main_to_start_task_ms", t.mainEntered, t.startTask)
	// Unlike the durations above, these round down.
	if !t.firstLogAfterRunning.IsZero() {
		setMaxDuration(summary, "running_to_first_log_ms", t.firstLogAfterRunning.Sub(t.running).Milliseconds())
	}
	if !t.firstLogAfterStartTask.IsZero() {
		setMaxDuration(summary, "start_task_to_first_log_ms", t.firstLogAfterStartTask.Sub(t.startTask).Milliseconds())
	}
}

// addLifecycleDuration records a lifecycle span under its summary key, and
// for clip spans also their total, count and maximum.
func addLifecycleDuration(summary map[string]int64, event types.ContainerEventRecord) {
	durationUs := containerEventRecordDurationUs(event)
	durationMs := event.DurationMs
	if durationMs <= 0 && durationUs > 0 {
		durationMs = durationUsToMilliseconds(durationUs)
	}
	if event.EventID == "" || (durationMs <= 0 && durationUs <= 0) {
		return
	}

	summaryKey := types.EventSummaryKeyForLifecycle(types.ContainerLifecycleID(event.EventID))
	setMaxDuration(summary, summaryKey, durationMs)
	if !strings.HasPrefix(event.EventID, "clip.") {
		return
	}

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

// setRollupDurations sets the phase totals that are built from other keys.
func setRollupDurations(summary map[string]int64) {
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

	if clipUs := cmp.Or(summary["clip_read_total_us"], summary["clip_oci_read_total_us"]); clipUs > 0 {
		setMaxDuration(summary, "clip_us", clipUs)
		setMaxDuration(summary, "clip_ms", durationUsToMilliseconds(clipUs))
	} else {
		setMaxDuration(summary, "clip_ms", cmp.Or(summary["clip_read_total_ms"], summary["clip_oci_read_total_ms"]))
	}
	if summary["runtime_ms"] == 0 {
		setMaxDuration(summary, "runtime_ms", summary["container_startup_ms"])
	}
	setMaxDuration(summary, "runner_ms", summary["runner_start_to_end_task_ms"])
	if summary["result_ms"] == 0 {
		setMaxDuration(summary, "result_ms", summary["result_set_to_end_task_ms"])
	}
	setMaxDuration(summary, "task_ms", summary["container_request_to_start_task_ms"])
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

func setSummedDuration(summary map[string]int64, key string, parts ...string) {
	total := int64(0)
	for _, part := range parts {
		total += summary[part]
	}
	setMaxDuration(summary, key, total)
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
