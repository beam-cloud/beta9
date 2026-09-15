package types

var (
	// Scheduler keys
	UsageMetricsSchedulerContainerScheduled = "container_scheduled_count"
	UsageMetricsSchedulerContainerRequested = "container_requested_count"

	// Worker keys
	UsageMetricsWorkerContainerDuration = "container_duration_milliseconds"
	UsageMetricsWorkerContainerCost     = "container_cost_cents"

	// Gateway keys
	UsageMetricsManagedComputeReservationSeconds = "managed_compute_reservation_seconds"
	UsageMetricsManagedComputeReservationCost    = "managed_compute_reservation_cost_cents"
	UsageMetricsNodeUsage                        = "node_usage"

	// Managed endpoint accounting: one event per settled charge and party
	// (spend for the caller, earned for the provider), carrying every Usage
	// field. The meter for each field is EndpointUsageMeter(field).
	UsageMetricsEndpointUsage = "endpoint_usage"
)

type TaskMetrics struct {
	TaskByStatusCounts map[string]int `json:"task_by_status_counts"`
}
