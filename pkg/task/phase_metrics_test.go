package task

import (
	"context"
	stdjson "encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func newPhaseMetricsTestRedis(t *testing.T) (*common.RedisClient, func()) {
	t.Helper()

	s, err := miniredis.Run()
	require.NoError(t, err)

	client := redis.NewClient(&redis.Options{Addr: s.Addr()})
	return &common.RedisClient{UniversalClient: client}, func() {
		require.NoError(t, client.Close())
		s.Close()
	}
}

func TestFunctionPhaseLabelsFromTaskIncludesRuntimeAndIdentity(t *testing.T) {
	stubConfig, err := stdjson.Marshal(types.StubConfigV1{
		Runtime: types.Runtime{
			Cpu:      100,
			Memory:   128,
			Gpu:      types.GpuType("T4"),
			GpuCount: 1,
		},
	})
	require.NoError(t, err)

	task := &types.TaskWithRelated{
		Task: types.Task{
			Status: types.TaskStatusRunning,
		},
		Workspace: types.Workspace{
			ExternalId: "workspace-123",
		},
		Stub: types.Stub{
			ExternalId: "stub-123",
			Type:       types.StubType(types.StubTypeFunction),
			Config:     string(stubConfig),
		},
	}

	labels := FunctionPhaseLabelsFromTask(task)

	require.Equal(t, "workspace-123", labels["workspace_id"])
	require.Equal(t, "stub-123", labels["stub_id"])
	require.Equal(t, "function", labels["stub_type"])
	require.Equal(t, "RUNNING", labels["status"])
	require.Equal(t, "100", labels["cpu"])
	require.Equal(t, "128", labels["memory"])
	require.Equal(t, "T4", labels["gpu"])
	require.Equal(t, "1", labels["gpu_count"])
}

func TestPhaseMetricsStoresLabelsAndTimestamps(t *testing.T) {
	rdb, cleanup := newPhaseMetricsTestRedis(t)
	defer cleanup()

	ctx := context.Background()
	pm := NewPhaseMetrics(rdb)
	start := time.Now().Add(-250 * time.Millisecond)
	labels := map[string]string{
		"workspace_id": "workspace-123",
		"stub_id":      "stub-123",
		"cpu":          "100",
	}

	require.NoError(t, pm.StoreLabels(ctx, "workspace", "task-123", labels))
	require.NoError(t, pm.Mark(ctx, "workspace", "task-123", FunctionPhaseStartTask, start))

	storedLabels := pm.Labels(ctx, "workspace", "task-123", map[string]string{"success": "false"})
	require.Equal(t, "workspace-123", storedLabels["workspace_id"])
	require.Equal(t, "stub-123", storedLabels["stub_id"])
	require.Equal(t, "100", storedLabels["cpu"])
	require.Equal(t, "false", storedLabels["success"])

	storedStart, ok, err := pm.Timestamp(ctx, "workspace", "task-123", FunctionPhaseStartTask)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, start.UnixMilli(), storedStart.UnixMilli())

	pm.RecordSince(ctx, "workspace", "task-123", "start_task_to_get_args", FunctionPhaseStartTask, time.Now(), labels)
}
