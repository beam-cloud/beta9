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

	// Managed endpoint (/v1 route) keys
	UsageMetricsEndpointPromptTokens     = "endpoint_prompt_tokens"
	UsageMetricsEndpointCompletionTokens = "endpoint_completion_tokens"
	UsageMetricsEndpointImages           = "endpoint_images"
	UsageMetricsEndpointRequests         = "endpoint_requests"
	UsageMetricsEndpointCost             = "endpoint_cost_cents"
	// Credited to the provider workspace whose machine served the request.
	UsageMetricsEndpointProviderEarnings = "endpoint_provider_earnings_cents"
)

type TaskMetrics struct {
	TaskByStatusCounts map[string]int `json:"task_by_status_counts"`
}
