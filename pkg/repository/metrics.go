package repository

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	metricRequestSchedulingDuration = "scheduler_request_scheduling_duration_ms"
	metricRequestRetries            = "scheduler_request_retries"
	metricImagePullTime             = "worker_image_pull_time_seconds"
	metricImageBuildSpeed           = "worker_image_build_speed_mbps"
	metricImageUnpackSpeed          = "worker_image_unpack_speed_mbps"
	metricImageCopySpeed            = "worker_image_copy_speed_mbps"
	metricImageArchiveSpeed         = "worker_image_archive_speed_mbps"
	metricImagePushSpeed            = "worker_image_push_speed_mbps"
)

type MetricsRepository struct {
	set *metrics.Set
}

func NewMetricsRepository(config types.VictoriaMetricsConfig) *MetricsRepository {
	set := metrics.NewSet()

	// ENV to collect for default labels
	workerPoolName := os.Getenv("WORKER_POOL_NAME")
	gpuType := os.Getenv("GPU_TYPE")
	podHostname := os.Getenv("POD_HOSTNAME")

	opts := &metrics.PushOptions{
		Headers: []string{
			fmt.Sprintf("Authorization: Bearer %s", config.AuthToken),
		},
	}

	if workerPoolName != "" && podHostname != "" {
		opts.ExtraLabels = fmt.Sprintf(`worker_pool_name="%s",pod_hostname="%s"`, workerPoolName, podHostname)
	}

	if gpuType != "" {
		opts.ExtraLabels = fmt.Sprintf(`%s,gpu_type="%s"`, opts.ExtraLabels, gpuType)
	}

	set.InitPushWithOptions(
		context.Background(),
		config.PushURL,
		time.Duration(config.PushSecs)*time.Second,
		opts,
	)

	return &MetricsRepository{
		set: set,
	}
}

func (m *MetricsRepository) RecordRequestSchedulingDuration(duration time.Duration, request *types.ContainerRequest) {
	log.Info().Interface("request", request).Msg("recording request scheduling duration")
	metricName := fmt.Sprintf("%s{gpu=\"%s\",gpu_count=\"%d\",cpu=\"%d\",memory=\"%d\"}",
		metricRequestSchedulingDuration,
		request.Gpu,
		request.GpuCount,
		request.Cpu,
		request.Memory)

	m.set.GetOrCreateHistogram(metricName).Update(float64(duration.Milliseconds()))
}

func (m *MetricsRepository) RecordRequestRetry(request *types.ContainerRequest) {
	metricName := fmt.Sprintf("%s{container_id=\"%s\",gpu=\"%s\",gpu_count=\"%d\",cpu=\"%d\",memory=\"%d\",retry_count=\"%d\"}",
		metricRequestRetries,
		request.ContainerId,
		request.Gpu,
		request.GpuCount,
		request.Cpu,
		request.Memory,
		request.RetryCount)

	m.set.GetOrCreateCounter(metricName).Inc()
}

func (m *MetricsRepository) RecordImagePullTime(duration time.Duration) {
	m.set.GetOrCreateHistogram(metricImagePullTime).Update(duration.Seconds())
}

func (m *MetricsRepository) RecordImageBuildSpeed(sizeInMB float64, duration time.Duration) {
	m.set.GetOrCreateHistogram(metricImageBuildSpeed).Update(sizeInMB / duration.Seconds())
}

func (m *MetricsRepository) RecordImageUnpackSpeed(sizeInMB float64, duration time.Duration) {
	m.set.GetOrCreateHistogram(metricImageUnpackSpeed).Update(sizeInMB / duration.Seconds())
}

func (m *MetricsRepository) RecordImageCopySpeed(sizeInMB float64, duration time.Duration) {
	m.set.GetOrCreateHistogram(metricImageCopySpeed).Update(sizeInMB / duration.Seconds())
}

func (m *MetricsRepository) RecordImageArchiveSpeed(sizeInMB float64, duration time.Duration) {
	m.set.GetOrCreateHistogram(metricImageArchiveSpeed).Update(sizeInMB / duration.Seconds())
}

func (m *MetricsRepository) RecordImagePushSpeed(sizeInMB float64, duration time.Duration) {
	m.set.GetOrCreateHistogram(metricImagePushSpeed).Update(sizeInMB / duration.Seconds())
}
