package scheduler

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
)

func TestPoolMetricsEventAggregatesAvailableWorkers(t *testing.T) {
	workers := []*types.Worker{
		{Id: "worker-1", MachineId: "machine-1", Status: types.WorkerStatusAvailable, TotalCpu: 2000, FreeCpu: 1000, TotalMemory: 4000, FreeMemory: 1000, TotalGpuCount: 2, FreeGpuCount: 1},
		{Id: "worker-2", MachineId: "machine-1", Status: types.WorkerStatusAvailable, TotalCpu: 1000, TotalMemory: 2000, TotalGpuCount: 1},
		{Id: "worker-3", MachineId: "machine-2", Status: types.WorkerStatusAvailable, TotalCpu: 1000, FreeCpu: 1000, TotalMemory: 2000, FreeMemory: 2000, TotalGpuCount: 1, FreeGpuCount: 1},
		{Id: "pending", Status: types.WorkerStatusPending, TotalCpu: 8000, TotalMemory: 16_000, TotalGpuCount: 8},
	}
	event := poolMetricsEvent("gpu-pool", types.WorkerPoolConfig{Mode: types.PoolModeExternal, DefaultMachineCost: 0.001}, &types.WorkerPoolState{
		Status: types.WorkerPoolStatusHealthy, RunningContainers: 5, AvailableWorkers: 3, PendingWorkers: 1,
	}, workers)

	if event.Action != types.EventComputeActionPoolHeartbeat || event.MachineCount != 2 || event.CPUCount != 4 || event.MemoryMB != 8000 || event.GPUCount != 4 {
		t.Fatalf("unexpected capacity: %+v", event)
	}
	wantAttrs := map[string]string{
		"container_count": "5", "free_gpu_count": "2", "cpu_utilization_pct": "50.00",
		"memory_used_mb": "5000", "memory_utilization_pct": "62.50", "hourly_cost_micros": "7200000",
	}
	for key, want := range wantAttrs {
		if got := event.Attrs[key]; got != want {
			t.Fatalf("attribute %q = %q, want %q", key, got, want)
		}
	}
}
