package task

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/metrics"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
)

const (
	FunctionPhaseContainerRequestReady = types.FunctionLifecycleCheckpointContainerRequestReady
	FunctionPhaseStartTask             = types.FunctionLifecycleCheckpointStartTask
	FunctionPhaseGetArgs               = types.FunctionLifecycleCheckpointGetArgs
	FunctionPhaseSetResult             = types.FunctionLifecycleCheckpointSetResult

	functionPhaseMetricTTL = 15 * time.Minute
)

type PhaseMetrics struct {
	rdb *common.RedisClient
}

func NewPhaseMetrics(rdb *common.RedisClient) *PhaseMetrics {
	return &PhaseMetrics{rdb: rdb}
}

func FunctionPhaseLabelsFromTask(task *types.TaskWithRelated) map[string]string {
	labels := map[string]string{}
	if task == nil {
		return labels
	}

	labels["workspace_id"] = task.Workspace.ExternalId
	labels["stub_id"] = task.Stub.ExternalId
	labels["stub_type"] = task.Stub.Type.Kind()
	labels["status"] = string(task.Status)

	if stubConfig, err := task.Stub.UnmarshalConfig(); err == nil && stubConfig != nil {
		addRuntimeLabels(labels, stubConfig.Runtime)
	}

	return labels
}

func FunctionPhaseLabelsFromStub(stub *types.StubWithRelated, stubConfig types.StubConfigV1, status types.TaskStatus) map[string]string {
	labels := map[string]string{
		"status": string(status),
	}
	if stub != nil {
		labels["workspace_id"] = stub.Workspace.ExternalId
		labels["stub_id"] = stub.ExternalId
		labels["stub_type"] = stub.Type.Kind()
	}

	addRuntimeLabels(labels, stubConfig.Runtime)
	return labels
}

func addRuntimeLabels(labels map[string]string, runtime types.Runtime) {
	labels["cpu"] = strconv.FormatInt(runtime.Cpu, 10)
	labels["memory"] = strconv.FormatInt(runtime.Memory, 10)
	labels["gpu_count"] = strconv.FormatUint(uint64(runtime.GpuCount), 10)

	if runtime.Gpu != "" {
		labels["gpu"] = runtime.Gpu.String()
	} else if len(runtime.Gpus) > 0 && runtime.Gpus[0] != "" {
		labels["gpu"] = runtime.Gpus[0].String()
	} else {
		labels["gpu"] = "none"
	}
}

func (m *PhaseMetrics) StoreLabels(ctx context.Context, workspaceName, taskId string, labels map[string]string) error {
	if m == nil || m.rdb == nil || len(labels) == 0 {
		return nil
	}

	key := common.RedisKeys.TaskPhaseLabels(workspaceName, taskId)
	if err := m.rdb.HSet(ctx, key, labels).Err(); err != nil {
		return fmt.Errorf("failed to store task phase labels: %w", err)
	}

	if err := m.rdb.Expire(ctx, key, functionPhaseMetricTTL).Err(); err != nil {
		return fmt.Errorf("failed to expire task phase labels: %w", err)
	}

	return nil
}

func (m *PhaseMetrics) Labels(ctx context.Context, workspaceName, taskId string, fallback map[string]string) map[string]string {
	labels := copyLabels(fallback)
	if m == nil || m.rdb == nil {
		return labels
	}

	stored, err := m.rdb.HGetAll(ctx, common.RedisKeys.TaskPhaseLabels(workspaceName, taskId)).Result()
	if err != nil || len(stored) == 0 {
		return labels
	}

	for key, value := range stored {
		if value != "" {
			labels[key] = value
		}
	}

	return labels
}

func (m *PhaseMetrics) Mark(ctx context.Context, workspaceName, taskId, phase string, at time.Time) error {
	if m == nil || m.rdb == nil {
		return nil
	}

	key := common.RedisKeys.TaskPhase(workspaceName, taskId, phase)
	if err := m.rdb.Set(ctx, key, at.UnixMilli(), functionPhaseMetricTTL).Err(); err != nil {
		return fmt.Errorf("failed to mark task phase: %w", err)
	}

	return nil
}

func (m *PhaseMetrics) Timestamp(ctx context.Context, workspaceName, taskId, phase string) (time.Time, bool, error) {
	if m == nil || m.rdb == nil {
		return time.Time{}, false, nil
	}

	value, err := m.rdb.Get(ctx, common.RedisKeys.TaskPhase(workspaceName, taskId, phase)).Int64()
	if err == redis.Nil {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("failed to get task phase: %w", err)
	}
	if value <= 0 {
		return time.Time{}, false, nil
	}

	return time.UnixMilli(value), true, nil
}

func (m *PhaseMetrics) RecordSince(ctx context.Context, workspaceName, taskId, metricPhase, sincePhase string, end time.Time, labels map[string]string) {
	start, ok, err := m.Timestamp(ctx, workspaceName, taskId, sincePhase)
	if err != nil || !ok || end.Before(start) {
		return
	}

	metrics.RecordFunctionTaskPhase(metricPhase, end.Sub(start), m.Labels(ctx, workspaceName, taskId, labels))
}

func copyLabels(labels map[string]string) map[string]string {
	copied := map[string]string{}
	for key, value := range labels {
		copied[key] = value
	}
	return copied
}
