package types

var (
	// Scheduler keys
	MetricsSchedulerContainerScheduled = "container_scheduled_count"
	MetricsSchedulerContainerRequested = "container_requested_count"

	// Worker keys
	MetricsWorkerContainerDuration = "container_duration_milliseconds"
	MetricsWorkerContainerCost     = "container_cost_cents"
)

type TaskMetrics struct {
	TaskByStatusCounts map[string]int `json:"task_by_status_counts"`
}
