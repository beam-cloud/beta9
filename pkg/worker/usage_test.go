package worker

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
)

func TestEmitContainerUsageSkipsPrivatePools(t *testing.T) {
	repo := &usageMetricsRecorder{}
	metrics := &WorkerUsageMetrics{
		workerId:    "worker-1",
		metricsRepo: repo,
		poolMode:    types.PoolModePrivate,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	metrics.EmitContainerUsage(ctx, &types.ContainerRequest{ContainerId: "container-1"})

	if repo.count != 0 {
		t.Fatalf("private pool emitted %d usage counters, want 0", repo.count)
	}
}

func TestEmitContainerUsageRecordsNonPrivatePools(t *testing.T) {
	repo := &usageMetricsRecorder{}
	metrics := &WorkerUsageMetrics{
		workerId:    "worker-1",
		metricsRepo: repo,
		poolMode:    types.PoolModeLocal,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	metrics.EmitContainerUsage(ctx, &types.ContainerRequest{ContainerId: "container-1"})

	if repo.count != 2 {
		t.Fatalf("local pool emitted %d usage counters, want duration and cost", repo.count)
	}
}

type usageMetricsRecorder struct {
	count int
}

func (r *usageMetricsRecorder) Init(string) error {
	return nil
}

func (r *usageMetricsRecorder) IncrementCounter(string, map[string]interface{}, float64) error {
	r.count++
	return nil
}

func (r *usageMetricsRecorder) SetGauge(string, map[string]interface{}, float64) error {
	return nil
}
