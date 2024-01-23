package repository

import (
	"fmt"
	"time"

	common "github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/types"
)

type MetricsStatsd struct {
	statSender *common.StatsdSender
}

func NewMetricsStatsdRepository() *MetricsStatsd {
	return &MetricsStatsd{
		statSender: common.GetStatSender(),
	}
}

func (m *MetricsStatsd) ContainerStarted(containerId string, workerId string) {
	m.statSender.StatGaugeTags(types.ContainerLifecycleStatsKey, int(time.Now().UnixMilli()), map[string]string{
		"container_id": containerId,
		"worker_id":    workerId,
		"status":       types.ContainerStatusStarted,
	})
}

func (m *MetricsStatsd) ContainerStopped(containerId string, workerId string) {
	m.statSender.StatGaugeTags(types.ContainerLifecycleStatsKey, int(time.Now().UnixMilli()), map[string]string{
		"container_id": containerId,
		"worker_id":    workerId,
		"status":       types.ContainerStatusStopped,
	})
}

func (m *MetricsStatsd) ContainerRequested(containerId string) {
	m.statSender.StatGaugeTags(types.ContainerLifecycleStatsKey, int(time.Now().UnixMilli()), map[string]string{
		"container_id": containerId,
		"status":       types.ContainerStatusRequested,
	})
}

func (m *MetricsStatsd) ContainerScheduled(containerId string) {
	m.statSender.StatGaugeTags(types.ContainerLifecycleStatsKey, int(time.Now().UnixMilli()), map[string]string{
		"container_id": containerId,
		"status":       types.ContainerStatusScheduled,
	})
}

func (m *MetricsStatsd) ContainerDuration(containerId string, workerId string, timestampNs int64, duration time.Duration) {
	m.statSender.StatGaugeTags(types.ContainerDurationStatsKey, int(duration.Milliseconds()), map[string]string{
		"container_id": containerId,
		"worker_id":    workerId,
		"timestamp_ns": fmt.Sprintf("%d", timestampNs),
	})
}

func (m *MetricsStatsd) WorkerStarted(workerId string) {
	m.statSender.StatGaugeTags(types.WorkerLifecycleStatsKey, int(time.Now().UnixMilli()), map[string]string{
		"worker_id": workerId,
		"status":    types.WorkerLifecycleStarted,
	})
}

func (m *MetricsStatsd) WorkerStopped(workerId string) {
	m.statSender.StatGaugeTags(types.WorkerLifecycleStatsKey, int(time.Now().UnixMilli()), map[string]string{
		"worker_id": workerId,
		"status":    types.WorkerLifecycleStopped,
	})
}

func (m *MetricsStatsd) WorkerDuration(workerId string, timestampNs int64, duration time.Duration) {
	m.statSender.StatGaugeTags(types.WorkerDurationStatsKey, int(duration.Milliseconds()), map[string]string{
		"worker_id":    workerId,
		"timestamp_ns": fmt.Sprintf("%d", timestampNs),
	})
}

func (m *MetricsStatsd) Beta9DeploymentRequestDuration(bucketName string, duration time.Duration) {
	m.statSender.StatTime(fmt.Sprintf("beta9.deployment.request.%s.duration", bucketName), duration)
}

func (m *MetricsStatsd) Beta9DeploymentRequestStatus(bucketName string, status int) {
	m.statSender.StatCount(fmt.Sprintf("beta9.deployment.request.%s.http_status_%d", bucketName, status), 1)
}

func (m *MetricsStatsd) Beta9DeploymentRequestCount(bucketName string) {
	m.statSender.StatCount(fmt.Sprintf("beta9.deployment.request.%s.count", bucketName), 1)
}
