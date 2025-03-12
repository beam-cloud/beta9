package metrics

import (
	"context"
	"fmt"
	"os"
	"time"

	"encoding/base64"

	vmetrics "github.com/VictoriaMetrics/metrics"
	"github.com/beam-cloud/beta9/pkg/types"
	"tailscale.com/tstime/rate"
)

const (
	// Rate limiter that allows 2 records per second
	dialMetricLimiterRate  = 2
	dialMetricLimiterBurst = 2
)

var (
	dialMetricLimiter *rate.Limiter
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
	metricS3PutSpeed                = "s3_put_speed_mbps"
	metricS3GetSpeed                = "s3_get_speed_mbps"
	metricDialTime                  = "dial_time_ms"
	metricContainerStartLatency     = "container_start_latency_ms"
)

func InitializeMetricsRepository(config types.VictoriaMetricsConfig) {
	workerPoolName := os.Getenv("WORKER_POOL_NAME")
	gpuType := os.Getenv("GPU_TYPE")
	podHostname := os.Getenv("POD_HOSTNAME")

	creds := fmt.Sprintf("vmagent:%s", config.AuthToken)
	encodedCreds := base64.StdEncoding.EncodeToString([]byte(creds))
	opts := &vmetrics.PushOptions{
		Headers: []string{
			fmt.Sprintf("Authorization: Basic %s", encodedCreds),
		},
	}

	if workerPoolName != "" && podHostname != "" {
		opts.ExtraLabels = fmt.Sprintf(`worker_pool_name="%s",pod_hostname="%s"`, workerPoolName, podHostname)
	}

	if gpuType != "" {
		opts.ExtraLabels = fmt.Sprintf(`%s,gpu_type="%s"`, opts.ExtraLabels, gpuType)
	}

	vmetrics.GetDefaultSet().InitPushWithOptions(
		context.Background(),
		config.PushURL,
		time.Duration(config.PushSecs)*time.Second,
		opts,
	)

	dialMetricLimiter = rate.NewLimiter(dialMetricLimiterRate, dialMetricLimiterBurst)
}

func RecordRequestSchedulingDuration(duration time.Duration, request *types.ContainerRequest) {
	metricName := fmt.Sprintf("%s{gpu=\"%s\",gpu_count=\"%d\",cpu=\"%d\",memory=\"%d\"}",
		metricRequestSchedulingDuration,
		request.Gpu,
		request.GpuCount,
		request.Cpu,
		request.Memory)

	vmetrics.GetDefaultSet().GetOrCreateHistogram(metricName).Update(float64(duration.Milliseconds()))
}

func RecordRequestRetry(request *types.ContainerRequest) {
	metricName := fmt.Sprintf("%s{container_id=\"%s\",gpu=\"%s\",gpu_count=\"%d\",cpu=\"%d\",memory=\"%d\",retry_count=\"%d\"}",
		metricRequestRetries,
		request.ContainerId,
		request.Gpu,
		request.GpuCount,
		request.Cpu,
		request.Memory,
		request.RetryCount)

	vmetrics.GetDefaultSet().GetOrCreateCounter(metricName).Inc()
}

func RecordImagePullTime(duration time.Duration) {
	vmetrics.GetDefaultSet().GetOrCreateHistogram(metricImagePullTime).Update(duration.Seconds())
}

func RecordImageBuildSpeed(sizeInMB float64, duration time.Duration) {
	if sizeInMB > 0 {
		vmetrics.GetDefaultSet().GetOrCreateHistogram(metricImageBuildSpeed).Update(sizeInMB / duration.Seconds())
	}
}

func RecordImageUnpackSpeed(sizeInMB float64, duration time.Duration) {
	if sizeInMB > 0 {
		vmetrics.GetDefaultSet().GetOrCreateHistogram(metricImageUnpackSpeed).Update(sizeInMB / duration.Seconds())
	}
}

func RecordImageCopySpeed(sizeInMB float64, duration time.Duration) {
	if sizeInMB > 0 {
		vmetrics.GetDefaultSet().GetOrCreateHistogram(metricImageCopySpeed).Update(sizeInMB / duration.Seconds())
	}
}

func RecordImageArchiveSpeed(sizeInMB float64, duration time.Duration) {
	if sizeInMB > 0 {
		vmetrics.GetDefaultSet().GetOrCreateHistogram(metricImageArchiveSpeed).Update(sizeInMB / duration.Seconds())
	}
}

func RecordImagePushSpeed(sizeInMB float64, duration time.Duration) {
	if sizeInMB > 0 {
		vmetrics.GetDefaultSet().GetOrCreateHistogram(metricImagePushSpeed).Update(sizeInMB / duration.Seconds())
	}
}

func RecordS3PutSpeed(sizeInMB float64, duration time.Duration) {
	if sizeInMB > 0 {
		vmetrics.GetDefaultSet().GetOrCreateHistogram(metricS3PutSpeed).Update(sizeInMB / duration.Seconds())
	}
}

func RecordS3GetSpeed(sizeInMB float64, duration time.Duration) {
	if sizeInMB > 0 {
		vmetrics.GetDefaultSet().GetOrCreateHistogram(metricS3GetSpeed).Update(sizeInMB / duration.Seconds())
	}
}

func RecordDialTime(duration time.Duration, host string) {
	if dialMetricLimiter.Allow() {
		metricName := fmt.Sprintf("%s{host=\"%s\"}", metricDialTime, host)
		vmetrics.GetDefaultSet().GetOrCreateHistogram(metricName).Update(float64(duration.Milliseconds()))
	}
}

func RecordContainerStartLatency(duration time.Duration) {
	vmetrics.GetDefaultSet().GetOrCreateHistogram(metricContainerStartLatency).Update(float64(duration.Milliseconds()))
}
