package metrics

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
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
	metricRequestScheduleFailure    = "scheduler_request_schedule_failure"
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
	metricWorkerStartupLatency      = "worker_startup_latency_ms"
	metricWorkerStartupPhase        = "worker_startup_phase_duration_ms"
	metricSchedulerBacklogDepth     = "scheduler_backlog_depth"
	metricSchedulerWorkerWait       = "scheduler_worker_wait_duration_ms"
	metricWorkerQueueDepth          = "worker_queue_depth"
	metricWorkerQueueReceiveLatency = "worker_queue_receive_latency_ms"
	metricWorkerQueueEmptyPolls     = "worker_queue_empty_polls"
	metricConcurrencyLimitThrottles = "concurrency_limit_throttles"
	metricRingBufferOccupancy       = "ring_buffer_occupancy"
	metricRingBufferOverwrites      = "ring_buffer_overwrites"
	metricProxyTokenDenials         = "proxy_token_denials"
	metricProxyQueuedRequestWait    = "proxy_queued_request_wait_ms"
	metricProxyBackendDialLatency   = "proxy_backend_dial_latency_ms"
	metricSandboxConnectPhase       = "sandbox_connect_phase_duration_ms"
	metricFunctionTaskPhase         = "function_task_phase_duration_ms"
)

func escapeLabelValue(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return value
}

func metricWithLabels(metric string, labels map[string]string) string {
	if len(labels) == 0 {
		return metric
	}

	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	labelParts := make([]string, 0, len(keys))
	for _, key := range keys {
		labelParts = append(labelParts, fmt.Sprintf(`%s="%s"`, key, escapeLabelValue(labels[key])))
	}

	return fmt.Sprintf("%s{%s}", metric, strings.Join(labelParts, ","))
}

func requestMetricLabels(request *types.ContainerRequest) map[string]string {
	labels := map[string]string{
		"stub_type": "unknown",
		"gpu":       "none",
		"gpu_count": "0",
		"cpu":       "0",
		"memory":    "0",
	}

	if request == nil {
		return labels
	}

	labels["gpu_count"] = fmt.Sprintf("%d", request.GpuCount)
	labels["cpu"] = fmt.Sprintf("%d", request.Cpu)
	labels["memory"] = fmt.Sprintf("%d", request.Memory)
	if request.Gpu != "" {
		labels["gpu"] = request.Gpu
	}
	if request.Stub.Type != "" {
		labels["stub_type"] = string(request.Stub.Type.Kind())
	}
	if request.Checkpoint != nil {
		labels["checkpoint"] = "true"
	} else {
		labels["checkpoint"] = "false"
	}
	if request.DockerEnabled {
		labels["docker_enabled"] = "true"
	} else {
		labels["docker_enabled"] = "false"
	}

	return labels
}

func mergeLabels(base map[string]string, extra map[string]string) map[string]string {
	labels := make(map[string]string, len(base)+len(extra))
	for key, value := range base {
		labels[key] = value
	}
	for key, value := range extra {
		labels[key] = value
	}
	return labels
}

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
	gpu := request.Gpu
	if gpu == "" {
		gpu = "none"
	}

	gpus := strings.Join(request.GpuRequest, ":")
	metricName := fmt.Sprintf("%s{gpu=\"%s\",requested_gpus=\"%s\",gpu_count=\"%d\",cpu=\"%d\",memory=\"%d\"}",
		metricRequestSchedulingDuration,
		gpu,
		gpus,
		request.GpuCount,
		request.Cpu,
		request.Memory)

	vmetrics.GetDefaultSet().GetOrCreateHistogram(metricName).Update(float64(duration.Milliseconds()))
}

func RecordRequestRetry(request *types.ContainerRequest) {
	gpu := request.Gpu
	if gpu == "" {
		gpu = "none"
	}
	gpus := strings.Join(request.GpuRequest, ":")
	metricName := fmt.Sprintf("%s{container_id=\"%s\",gpu=\"%s\",requested_gpus=\"%s\",gpu_count=\"%d\",cpu=\"%d\",memory=\"%d\",retry_count=\"%d\"}",
		metricRequestRetries,
		request.ContainerId,
		gpu,
		gpus,
		request.GpuCount,
		request.Cpu,
		request.Memory,
		request.RetryCount)

	vmetrics.GetDefaultSet().GetOrCreateCounter(metricName).Inc()
}

func RecordRequestScheduleFailure(request *types.ContainerRequest) {
	gpu := request.Gpu
	if gpu == "" {
		gpu = "none"
	}
	gpus := strings.Join(request.GpuRequest, ":")
	metricName := fmt.Sprintf("%s{container_id=\"%s\",gpu=\"%s\",requested_gpus=\"%s\",gpu_count=\"%d\",cpu=\"%d\",memory=\"%d\",retry_count=\"%d\"}",
		metricRequestScheduleFailure,
		request.ContainerId,
		gpu,
		gpus,
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
		// Only want the machine-xxxxxxxx part of host (machine-xxxxxxxx.ttttttttt.ts.net:00000)
		hostParts := strings.Split(host, ".")
		if len(hostParts) > 0 {
			metricName := fmt.Sprintf("%s{host=\"%s\"}", metricDialTime, hostParts[0])
			vmetrics.GetDefaultSet().GetOrCreateHistogram(metricName).Update(float64(duration.Milliseconds()))
		}
	}
}

func RecordContainerStartLatency(container *types.ContainerState, duration time.Duration) {
	gpu := container.Gpu
	if gpu == "" {
		gpu = "none"
	}
	metricName := fmt.Sprintf("%s{container_id=\"%s\",gpu=\"%s\",gpu_count=\"%d\",cpu=\"%d\",memory=\"%d\"}",
		metricContainerStartLatency,
		container.ContainerId,
		gpu,
		container.GpuCount,
		container.Cpu,
		container.Memory,
	)
	vmetrics.GetDefaultSet().GetOrCreateHistogram(metricName).Update(float64(duration.Milliseconds()))
}

func RecordWorkerStartupLatency(duration time.Duration, request *types.ContainerRequest) {
	metricName := metricWithLabels(metricWorkerStartupLatency, requestMetricLabels(request))
	vmetrics.GetDefaultSet().GetOrCreateHistogram(metricName).Update(float64(duration.Milliseconds()))
}

func RecordWorkerStartupPhase(phase string, duration time.Duration, request *types.ContainerRequest, extraLabels map[string]string) {
	labels := requestMetricLabels(request)
	labels["phase"] = phase
	labels = mergeLabels(labels, extraLabels)

	metricName := metricWithLabels(metricWorkerStartupPhase, labels)
	vmetrics.GetDefaultSet().GetOrCreateHistogram(metricName).Update(float64(duration.Milliseconds()))
}

func RecordSchedulerBacklogDepth(depth int64) {
	metricName := metricWithLabels(metricSchedulerBacklogDepth, nil)
	vmetrics.GetDefaultSet().GetOrCreateGauge(metricName, nil).Set(float64(depth))
}

func RecordSchedulerWorkerWait(duration time.Duration, request *types.ContainerRequest, outcome string) {
	labels := requestMetricLabels(request)
	labels["outcome"] = outcome

	metricName := metricWithLabels(metricSchedulerWorkerWait, labels)
	vmetrics.GetDefaultSet().GetOrCreateHistogram(metricName).Update(float64(duration.Milliseconds()))
}

func RecordWorkerQueueDepth(workerId string, depth int64) {
	metricName := metricWithLabels(metricWorkerQueueDepth, map[string]string{"worker_id": workerId})
	vmetrics.GetDefaultSet().GetOrCreateGauge(metricName, nil).Set(float64(depth))
}

func RecordWorkerQueueReceiveLatency(workerId string, duration time.Duration, request *types.ContainerRequest) {
	labels := requestMetricLabels(request)
	labels["worker_id"] = workerId

	metricName := metricWithLabels(metricWorkerQueueReceiveLatency, labels)
	vmetrics.GetDefaultSet().GetOrCreateHistogram(metricName).Update(float64(duration.Milliseconds()))
}

func RecordWorkerQueueEmptyPoll(workerId string) {
	metricName := metricWithLabels(metricWorkerQueueEmptyPolls, map[string]string{"worker_id": workerId})
	vmetrics.GetDefaultSet().GetOrCreateCounter(metricName).Inc()
}

func RecordConcurrencyLimitThrottle(resource string, request *types.ContainerRequest) {
	labels := requestMetricLabels(request)
	labels["resource"] = resource

	metricName := metricWithLabels(metricConcurrencyLimitThrottles, labels)
	vmetrics.GetDefaultSet().GetOrCreateCounter(metricName).Inc()
}

func RecordRingBufferOccupancy(bufferName, workspaceName, stubId string, length, capacity int) {
	labels := map[string]string{
		"buffer":    bufferName,
		"workspace": workspaceName,
		"stub_id":   stubId,
	}
	metricName := metricWithLabels(metricRingBufferOccupancy, mergeLabels(labels, map[string]string{"stat": "length"}))
	vmetrics.GetDefaultSet().GetOrCreateGauge(metricName, nil).Set(float64(length))

	capacityMetricName := metricWithLabels(metricRingBufferOccupancy, mergeLabels(labels, map[string]string{"stat": "capacity"}))
	vmetrics.GetDefaultSet().GetOrCreateGauge(capacityMetricName, nil).Set(float64(capacity))
}

func RecordRingBufferOverwrite(bufferName, workspaceName, stubId string) {
	metricName := metricWithLabels(metricRingBufferOverwrites, map[string]string{
		"buffer":    bufferName,
		"workspace": workspaceName,
		"stub_id":   stubId,
	})
	vmetrics.GetDefaultSet().GetOrCreateCounter(metricName).Inc()
}

func RecordProxyTokenDenial(proxyName, workspaceName, stubId string) {
	metricName := metricWithLabels(metricProxyTokenDenials, map[string]string{
		"proxy":     proxyName,
		"workspace": workspaceName,
		"stub_id":   stubId,
	})
	vmetrics.GetDefaultSet().GetOrCreateCounter(metricName).Inc()
}

func RecordProxyQueuedRequestWait(proxyName, workspaceName, stubId, protocol string, duration time.Duration) {
	metricName := metricWithLabels(metricProxyQueuedRequestWait, map[string]string{
		"proxy":     proxyName,
		"workspace": workspaceName,
		"stub_id":   stubId,
		"protocol":  protocol,
	})
	vmetrics.GetDefaultSet().GetOrCreateHistogram(metricName).Update(float64(duration.Milliseconds()))
}

func RecordProxyBackendDialLatency(proxyName, workspaceName, stubId, protocol string, success bool, duration time.Duration) {
	successLabel := "false"
	if success {
		successLabel = "true"
	}

	metricName := metricWithLabels(metricProxyBackendDialLatency, map[string]string{
		"proxy":     proxyName,
		"workspace": workspaceName,
		"stub_id":   stubId,
		"protocol":  protocol,
		"success":   successLabel,
	})
	vmetrics.GetDefaultSet().GetOrCreateHistogram(metricName).Update(float64(duration.Milliseconds()))
}

func RecordSandboxConnectPhase(phase, workspaceId, stubId, containerStatus, errorCode string, success bool, duration time.Duration) {
	successLabel := "false"
	if success {
		successLabel = "true"
	}

	if stubId == "" {
		stubId = "unknown"
	}
	if containerStatus == "" {
		containerStatus = "unknown"
	}
	if errorCode == "" {
		errorCode = "none"
	}

	metricName := metricWithLabels(metricSandboxConnectPhase, map[string]string{
		"phase":            phase,
		"workspace_id":     workspaceId,
		"stub_id":          stubId,
		"container_status": containerStatus,
		"error":            errorCode,
		"success":          successLabel,
	})
	vmetrics.GetDefaultSet().GetOrCreateHistogram(metricName).Update(float64(duration.Milliseconds()))
}

func RecordFunctionTaskPhase(phase string, duration time.Duration, labels map[string]string) {
	metricLabels := map[string]string{
		"phase":        phase,
		"workspace_id": "unknown",
		"stub_id":      "unknown",
		"stub_type":    "function",
		"cpu":          "0",
		"memory":       "0",
		"gpu":          "none",
		"gpu_count":    "0",
		"status":       "unknown",
		"success":      "true",
	}

	for key, value := range labels {
		if value != "" {
			metricLabels[key] = value
		}
	}

	metricName := metricWithLabels(metricFunctionTaskPhase, metricLabels)
	vmetrics.GetDefaultSet().GetOrCreateHistogram(metricName).Update(float64(duration.Milliseconds()))
}
