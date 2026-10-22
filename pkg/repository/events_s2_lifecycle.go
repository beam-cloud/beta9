package repository

import (
	"cmp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

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
