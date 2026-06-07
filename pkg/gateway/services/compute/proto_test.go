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
