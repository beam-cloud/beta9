package scheduler

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
)

func TestPoolMetricsEventAggregatesAvailableWorkers(t *testing.T) {
	provider := types.MachineProvider("test")
	workers := []*types.Worker{
		{Id: "worker-1", MachineId: "machine-1", Status: types.WorkerStatusAvailable, TotalCpu: 2000, FreeCpu: 1000, TotalMemory: 4000, FreeMemory: 1000, TotalGpuCount: 2, FreeGpuCount: 1},
		{Id: "worker-2", MachineId: "machine-1", Status: types.WorkerStatusAvailable, TotalCpu: 1000, TotalMemory: 2000, TotalGpuCount: 1},
		{Id: "worker-3", MachineId: "machine-2", Status: types.WorkerStatusAvailable, TotalCpu: 1000, FreeCpu: 1000, TotalMemory: 2000, FreeMemory: 2000, TotalGpuCount: 1, FreeGpuCount: 1},
		{Id: "pending", MachineId: "machine-3", Status: types.WorkerStatusPending, TotalCpu: 8000, TotalMemory: 16_000, TotalGpuCount: 8},
	}
	event := poolMetricsEvent("gpu-pool", types.WorkerPoolConfig{Mode: types.PoolModeExternal, Provider: &provider, HourlyCostCents: 120}, &types.WorkerPoolState{
		Status: types.WorkerPoolStatusHealthy, RunningContainers: 5, AvailableWorkers: 3, PendingWorkers: 1, ReadyMachines: 2, PendingMachines: 1,
	}, workers)

	if event.Action != types.EventComputeActionPoolHeartbeat || event.MachineCount != 3 || event.CPUCount != 4 || event.MemoryMB != 8000 || event.GPUCount != 4 {
		t.Fatalf("unexpected capacity: %+v", event)
	}
	wantAttrs := map[string]string{
		"container_count": "5", "free_gpu_count": "2", "cpu_utilization_pct": "50.00",
		"memory_used_mb": "5000", "memory_utilization_pct": "62.50", "hourly_cost_cents": "360",
	}
	for key, want := range wantAttrs {
		if got := event.Attrs[key]; got != want {
			t.Fatalf("attribute %q = %q, want %q", key, got, want)
		}
	}
}

func TestPoolMetricsEventUsesAgentInventoryForCost(t *testing.T) {
	event := poolMetricsEvent(
		"managed-gpu-pool",
		types.WorkerPoolConfig{Mode: types.PoolModeExternal, HourlyCostCents: 60},
		&types.WorkerPoolState{RegisteredMachines: 2, ReadyMachines: 1, PendingMachines: 1},
		[]*types.Worker{{
			Id: "ready-worker", MachineId: "ready-machine", Status: types.WorkerStatusAvailable,
		}},
	)
	if event.MachineCount != 2 {
		t.Fatalf("machine count = %d, want 2 registered agent machines", event.MachineCount)
	}
	if got := event.Attrs[types.EventComputeAttrHourlyCostCents]; got != "120" {
		t.Fatalf("hourly cost = %q, want 120 cents", got)
	}
}

func TestPoolSchedulabilityEventsUsePersistedTransitionState(t *testing.T) {
	var events []types.EventComputeSchema
	monitor := &PoolHealthMonitor{
		wpc: &LocalWorkerPoolControllerForTest{name: "gpu-pool"},
		health: types.FailoverHealthConfig{
			MaxPendingWorkers: 1,
		},
		pushMetrics: func(event types.EventComputeSchema) {
			events = append(events, event)
		},
	}

	unschedulable, reasons := monitor.schedulability(&types.WorkerPoolState{PendingWorkers: 2})
	monitor.emitSchedulabilityTransition(
		&types.WorkerPoolState{Status: types.WorkerPoolStatusHealthy},
		unschedulable,
		reasons,
	)
	if len(events) != 1 || events[0].Action != types.EventComputeActionPoolUnschedulable {
		t.Fatalf("unexpected unschedulable transition events: %#v", events)
	}

	monitor.emitSchedulabilityTransition(
		&types.WorkerPoolState{Status: types.WorkerPoolStatusDegraded},
		unschedulable,
		reasons,
	)
	if len(events) != 1 {
		t.Fatalf("unchanged state emitted %d events, want 1 total", len(events))
	}

	schedulable, reasons := monitor.schedulability(&types.WorkerPoolState{})
	monitor.emitSchedulabilityTransition(
		&types.WorkerPoolState{Status: types.WorkerPoolStatusDegraded},
		schedulable,
		reasons,
	)
	if len(events) != 2 || events[1].Action != types.EventComputeActionPoolSchedulable {
		t.Fatalf("unexpected schedulable transition events: %#v", events)
	}
}
