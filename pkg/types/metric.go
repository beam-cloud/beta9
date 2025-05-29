package types

var (
	// Scheduler keys
	UsageMetricsSchedulerContainerScheduled = "container_scheduled_count"
	UsageMetricsSchedulerContainerRequested = "container_requested_count"

	// Worker keys
	UsageMetricsWorkerContainerDuration = "container_duration_milliseconds"
	UsageMetricsWorkerContainerCost     = "container_cost_cents"

	// Gateway keys
	UsageMetricsPublicTaskCost  = "public_task_cost_cents"
	UsageMetricsPublicTaskCount = "public_task_count"
)

type TaskMetrics struct {
	TaskByStatusCounts map[string]int `json:"task_by_status_counts"`
}
