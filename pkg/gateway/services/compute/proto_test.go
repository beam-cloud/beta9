package compute

import (
	"testing"
	"time"

	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
)

func TestAgentMachineStatusUsesWorkerState(t *testing.T) {
	now := time.Now()
	state := &model.AgentTokenState{
		Schedulable:     true,
		LastJoinAt:      now,
		LastHeartbeatAt: now,
	}

	tests := []struct {
		name   string
		worker *types.Worker
		want   types.MachineStatus
	}{
		{name: "registered without worker", want: types.MachineStatusRegistered},
		{
			name:   "pending worker",
			worker: &types.Worker{Status: types.WorkerStatusPending},
			want:   types.MachineStatusPending,
		},
		{
			name:   "available worker",
			worker: &types.Worker{Status: types.WorkerStatusAvailable},
			want:   types.MachineStatusAvailable,
		},
		{
			name:   "disabled worker",
			worker: &types.Worker{Status: types.WorkerStatusDisabled},
			want:   types.MachineStatusDisabled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := agentMachineStatus(state, tt.worker, now); got != tt.want {
				t.Fatalf("agentMachineStatus() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAgentMachineStatusPreflightFailureIsDisabled(t *testing.T) {
	now := time.Now()
	state := &model.AgentTokenState{
		Schedulable: false,
		Preflight: []model.PreflightCheckState{
			{Name: "docker", OK: false, Severity: "error"},
		},
		LastJoinAt:      now,
		LastHeartbeatAt: now,
	}

	if got := agentMachineStatus(state, nil, now); got != types.MachineStatusDisabled {
		t.Fatalf("agentMachineStatus() = %q, want %q", got, types.MachineStatusDisabled)
	}
}

func TestAgentMachineMetricsUseScheduledCapacity(t *testing.T) {
	state := &model.AgentTokenState{
		CPUMillicores: 16000,
		MemoryMB:      32768,
		GPUCount:      4,
		Metrics: model.AgentMachineMetrics{
			CPUUtilizationPct:    90,
			MemoryUtilizationPct: 80,
			MemoryUsedMB:         1000,
			DiskUsedMB:           10,
			DiskTotalMB:          100,
			DiskUsagePct:         10,
		},
	}
	worker := &types.Worker{
		TotalCpu:         16000,
		FreeCpu:          12000,
		TotalMemory:      32768,
		FreeMemory:       24576,
		TotalGpuCount:    4,
		FreeGpuCount:     3,
		ActiveContainers: []types.Container{{ContainerId: "container-one"}, {ContainerId: "container-two"}},
	}

	metrics := agentMachineMetrics(state, worker)
	if metrics.CpuUtilizationPct != 25 {
		t.Fatalf("cpu utilization = %v, want 25", metrics.CpuUtilizationPct)
	}
	if metrics.MemoryUtilizationPct != 25 {
		t.Fatalf("memory utilization = %v, want 25", metrics.MemoryUtilizationPct)
	}
	if metrics.MemoryUsedMb != 8192 {
		t.Fatalf("memory used = %d, want 8192", metrics.MemoryUsedMb)
	}
	if metrics.FreeGpuCount != 3 {
		t.Fatalf("free gpu count = %d, want 3", metrics.FreeGpuCount)
	}
	if metrics.ContainerCount != 2 {
		t.Fatalf("container count = %d, want 2", metrics.ContainerCount)
	}
}

func TestPrivatePoolProjectionUsesBillableReservationCost(t *testing.T) {
	state := &model.PoolState{
		Name: "pool-1",
		Reservations: []model.Reservation{
			{
				ID:               "reservation-1",
				Source:           model.SourceCLIReservation,
				Status:           model.ReservationActive,
				HourlyCostMicros: 1_500_000,
			},
		},
	}

	pool := (&Service{}).privatePoolStateToProto(state)
	if got, want := pool.Reservations[0].HourlyCostMicros, int64(1_650_000); got != want {
		t.Fatalf("reservation hourly cost = %d, want %d", got, want)
	}
}
