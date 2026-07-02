package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/compute"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

func TestAgentWorkerPoolControllerAddWorkerCreatesDesiredSlot(t *testing.T) {
	ctx := context.Background()
	redisServer, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(redisServer.Close)

	redisClient, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{redisServer.Addr()},
		Mode:  types.RedisModeSingle,
	})
	if err != nil {
		t.Fatal(err)
	}

	workspaceID := "workspace-agent-test"
	workerRepo := repo.NewWorkerRedisRepositoryForTest(redisClient)
	computeRepo := repo.NewComputeRedisRepository(redisClient)
	poolState := &compute.PoolState{
		Name:     "private-gpu",
		Selector: "private-gpu",
		Config: &pb.PoolConfig{
			Name:     "private-gpu",
			Selector: "private-gpu",
			Gpu:      []string{"A10G"},
		},
	}
	machine := &compute.AgentTokenState{
		TokenHash:                 "agent-token",
		WorkspaceID:               workspaceID,
		PoolName:                  poolState.Name,
		MachineID:                 "machine-1",
		MachineFingerprint:        "machine-1",
		Hostname:                  "agent-machine",
		OS:                        "linux",
		Arch:                      "amd64",
		CPUCount:                  8,
		CPUMillicores:             7500,
		MemoryMB:                  32768,
		GPUs:                      []string{"A10G", "A10G"},
		GPUIDs:                    []string{"0", "1"},
		GPUCount:                  2,
		NetworkSlotPoolSize:       64,
		ContainerStartConcurrency: 12,
		Executor:                  types.DefaultAgentWorkerContainerMode,
		Schedulable:               true,
		CreatedAt:                 time.Now(),
		LastJoinAt:                time.Now(),
	}
	if err := computeRepo.SaveAgentTokenState(ctx, machine, time.Hour); err != nil {
		t.Fatal(err)
	}

	opts := AgentWorkerPoolControllerOptions{
		Context:     ctx,
		Name:        poolState.Selector,
		WorkspaceID: workspaceID,
		Config: types.AppConfig{
			ClusterName: "test",
			Worker: types.WorkerConfig{
				ImageRegistry: "registry.localhost:5000",
				ImageName:     "beta9-worker",
				ImageTag:      "latest",
			},
		},
		WorkerPool: types.WorkerPoolConfig{
			GPUType:              "A10G",
			ContainerRuntime:     types.ContainerRuntimeRunc.String(),
			RequiresPoolSelector: true,
			Priority:             1000,
		},
		PoolState:   poolState,
		WorkerRepo:  workerRepo,
		ComputeRepo: computeRepo,
	}
	controller, err := NewAgentWorkerPoolController(opts)
	if err != nil {
		t.Fatal(err)
	}

	worker, err := controller.AddWorker(2000, 1024, 1)
	if err != nil {
		t.Fatal(err)
	}
	if worker == nil {
		t.Fatal("expected worker")
	}
	if worker.PoolName != poolState.Selector {
		t.Fatalf("worker pool = %q, want %q", worker.PoolName, poolState.Selector)
	}
	if worker.MachineId != machine.MachineID {
		t.Fatalf("worker machine = %q, want %q", worker.MachineId, machine.MachineID)
	}
	if len(worker.Id) != 8 {
		t.Fatalf("worker id = %q, want shared 8-character worker id shape", worker.Id)
	}
	if worker.TotalCpu != machine.CPUMillicores {
		t.Fatalf("worker cpu = %d, want %d", worker.TotalCpu, machine.CPUMillicores)
	}
	if worker.TotalMemory != int64(machine.MemoryMB) {
		t.Fatalf("worker memory = %d, want %d", worker.TotalMemory, machine.MemoryMB)
	}
	if worker.TotalGpuCount != machine.GPUCount {
		t.Fatalf("worker gpus = %d, want %d", worker.TotalGpuCount, machine.GPUCount)
	}

	secondWorker, err := controller.AddWorker(1000, 1024, 1)
	if err != nil {
		t.Fatal(err)
	}
	if secondWorker.Id != worker.Id {
		t.Fatalf("second worker = %q, want stable worker %q", secondWorker.Id, worker.Id)
	}

	if err := workerRepo.UpdateWorkerStatus(worker.Id, types.WorkerStatusDisabled); err != nil {
		t.Fatal(err)
	}
	restoredFromAddWorker, err := controller.AddWorker(1000, 1024, 1)
	if err != nil {
		t.Fatal(err)
	}
	if restoredFromAddWorker.Id != worker.Id {
		t.Fatalf("restored worker = %q, want stable worker %q", restoredFromAddWorker.Id, worker.Id)
	}
	restoredWorker, err := workerRepo.GetWorkerById(worker.Id)
	if err != nil {
		t.Fatal(err)
	}
	if restoredWorker.Status != types.WorkerStatusPending {
		t.Fatalf("restored worker status = %q, want %q", restoredWorker.Status, types.WorkerStatusPending)
	}

	if err := workerRepo.UpdateWorkerStatus(worker.Id, types.WorkerStatusDisabled); err != nil {
		t.Fatal(err)
	}
	if _, err := NewAgentWorkerPoolController(opts); err != nil {
		t.Fatal(err)
	}
	restoredWorker, err = workerRepo.GetWorkerById(worker.Id)
	if err != nil {
		t.Fatal(err)
	}
	if restoredWorker.Status != types.WorkerStatusPending {
		t.Fatalf("restored worker status = %q, want %q", restoredWorker.Status, types.WorkerStatusPending)
	}

	_, err = controller.AddWorker(999999, 1024, 1)
	if err == nil {
		t.Fatal("expected capacity error")
	}
	var capacityErr *AgentPoolCapacityError
	if !errors.As(err, &capacityErr) {
		t.Fatalf("error = %T %v, want AgentPoolCapacityError", err, err)
	}
	if capacityErr.WorkspaceID != workspaceID || capacityErr.PoolName != poolState.Name {
		t.Fatalf("capacity error scope = workspace %q pool %q", capacityErr.WorkspaceID, capacityErr.PoolName)
	}
	if capacityErr.SchedulableMachines != 1 || capacityErr.Machines != 1 {
		t.Fatalf("capacity error machine counts = %d/%d, want 1/1", capacityErr.SchedulableMachines, capacityErr.Machines)
	}
}
