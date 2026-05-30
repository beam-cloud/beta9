package repository

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/compute"
)

func TestComputeAgentStateIndexesMachinesAndWorkers(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewComputeRedisRepository(rdb)
	ctx := context.Background()
	machine := &compute.AgentTokenState{
		TokenHash:   "token-one",
		WorkspaceID: "workspace-one",
		PoolName:    "pool-one",
		MachineID:   "machine-one",
		Schedulable: true,
	}
	if err := repo.SaveAgentTokenState(ctx, machine, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := repo.SaveAgentTokenState(ctx, &compute.AgentTokenState{
		TokenHash:   "token-two",
		WorkspaceID: "workspace-one",
		PoolName:    "pool-two",
		MachineID:   "machine-two",
		Schedulable: true,
	}, time.Hour); err != nil {
		t.Fatal(err)
	}

	machines, err := repo.ListAgentTokenStates(ctx, machine.WorkspaceID, machine.PoolName)
	if err != nil {
		t.Fatal(err)
	}
	if len(machines) != 1 || machines[0].MachineID != machine.MachineID {
		t.Fatalf("machines = %#v, want only %s", machines, machine.MachineID)
	}

	slot := &compute.AgentWorkerSlotState{
		WorkerID:    "worker-one",
		WorkspaceID: machine.WorkspaceID,
		PoolName:    machine.PoolName,
		MachineID:   machine.MachineID,
	}
	if err := repo.SaveAgentWorkerSlotState(ctx, slot); err != nil {
		t.Fatal(err)
	}
	if err := repo.SaveAgentWorkerSlotState(ctx, &compute.AgentWorkerSlotState{
		WorkerID:    "worker-two",
		WorkspaceID: machine.WorkspaceID,
		PoolName:    machine.PoolName,
		MachineID:   "machine-other",
	}); err != nil {
		t.Fatal(err)
	}

	slots, err := repo.ListAgentWorkerSlotStates(ctx, slot.WorkspaceID, slot.PoolName, slot.MachineID)
	if err != nil {
		t.Fatal(err)
	}
	if len(slots) != 1 || slots[0].WorkerID != slot.WorkerID {
		t.Fatalf("slots = %#v, want only %s", slots, slot.WorkerID)
	}

	if err := repo.DeleteAgentWorkerSlotState(ctx, slot.WorkspaceID, slot.PoolName, slot.MachineID, slot.WorkerID); err != nil {
		t.Fatal(err)
	}
	slots, err = repo.ListAgentWorkerSlotStates(ctx, slot.WorkspaceID, slot.PoolName, slot.MachineID)
	if err != nil {
		t.Fatal(err)
	}
	if len(slots) != 0 {
		t.Fatalf("slots after delete = %#v, want empty", slots)
	}
}
