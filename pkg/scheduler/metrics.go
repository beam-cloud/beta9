package scheduler

import (
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/metrics"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	sourceLabel                     = "scheduler"
	metricRequestSchedulingDuration = "scheduler_request_scheduling_duration_ms"
	metricRequestRetries            = "scheduler_request_retries"
)

type SchedulerMetrics struct {
	metricsRegistry *metrics.MetricsRegistry
}

func NewSchedulerMetrics(config types.VictoriaMetricsConfig) *SchedulerMetrics {
	metricsRegistry := metrics.NewMetricsRegistry(config, sourceLabel)
	return &SchedulerMetrics{
		metricsRegistry: metricsRegistry,
	}
}

func (sm *SchedulerMetrics) RecordRequestSchedulingDuration(duration time.Duration, request *types.ContainerRequest) {
	log.Info().Interface("request", request).Msg("recording request scheduling duration")
	metricName := fmt.Sprintf("%s{gpu=\"%s\",gpu_count=\"%d\",cpu=\"%d\",memory=\"%d\"}",
		metricRequestSchedulingDuration,
		request.Gpu,
		request.GpuCount,
		request.Cpu,
		request.Memory)

	sm.metricsRegistry.GetOrCreateHistogram(metricName).Update(float64(duration.Milliseconds()))
}

func (sm *SchedulerMetrics) RecordRequestRetry(request *types.ContainerRequest) {
	metricName := fmt.Sprintf("%s{container_id=\"%s\",gpu=\"%s\",gpu_count=\"%d\",cpu=\"%d\",memory=\"%d\",retry_count=\"%d\"}",
		metricRequestRetries,
		request.ContainerId,
		request.Gpu,
		request.GpuCount,
		request.Cpu,
		request.Memory,
		request.RetryCount)

	sm.metricsRegistry.GetOrCreateCounter(metricName).Inc()
}
