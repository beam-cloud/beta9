package scheduler

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/hybrid"
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

	workspace := types.Workspace{Id: 1, ExternalId: "workspace-agent-test", Name: "agent-test"}
	backendRepo := agentPoolBackendRepoForTest{workspace: workspace}

	workerRepo := repo.NewWorkerRedisRepositoryForTest(redisClient)
	hybridRepo := repo.NewHybridRedisRepository(redisClient)
	poolState := &hybrid.PoolState{
		Name:     "private-gpu",
		Selector: "private-gpu",
		Config: &pb.HybridPoolConfig{
			Name:     "private-gpu",
			Selector: "private-gpu",
			Gpu:      []string{"A10G"},
		},
	}
	machine := &hybrid.AgentTokenState{
		TokenHash:          "agent-token",
		WorkspaceID:        workspace.ExternalId,
		PoolName:           poolState.Name,
		MachineID:          "machine-1",
		MachineFingerprint: "machine-1",
		Hostname:           "agent-machine",
		OS:                 "linux",
		Arch:               "amd64",
		CPUCount:           8,
		MemoryMB:           32768,
		GPUs:               []string{"A10G", "A10G"},
		GPUCount:           2,
		Executor:           types.DefaultAgentWorkerContainerMode,
		Schedulable:        true,
		CreatedAt:          time.Now(),
		LastJoinAt:         time.Now(),
	}
	if err := hybridRepo.SaveAgentTokenState(ctx, machine, time.Hour); err != nil {
		t.Fatal(err)
	}

	controller, err := NewAgentWorkerPoolController(AgentWorkerPoolControllerOptions{
		Context:     ctx,
		Name:        poolState.Selector,
		WorkspaceID: workspace.ExternalId,
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
		BackendRepo: backendRepo,
		WorkerRepo:  workerRepo,
		HybridRepo:  hybridRepo,
	})
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

	slots, err := hybridRepo.ListAgentWorkerSlotStates(ctx, workspace.ExternalId, poolState.Name, machine.MachineID)
	if err != nil {
		t.Fatal(err)
	}
	if len(slots) != 1 {
		t.Fatalf("slots len = %d, want 1", len(slots))
	}
	slot := slots[0]
	if slot.WorkerID != worker.Id {
		t.Fatalf("slot worker = %q, want %q", slot.WorkerID, worker.Id)
	}
	if slot.PoolName != poolState.Name {
		t.Fatalf("slot pool = %q, want %q", slot.PoolName, poolState.Name)
	}
	if slot.GPUAssignment != "0" {
		t.Fatalf("slot gpu assignment = %q, want 0", slot.GPUAssignment)
	}
}

type agentPoolBackendRepoForTest struct {
	repo.BackendRepository
	workspace types.Workspace
}

func (r agentPoolBackendRepoForTest) GetWorkspaceByExternalId(ctx context.Context, externalID string) (types.Workspace, error) {
	if externalID != r.workspace.ExternalId {
		return types.Workspace{}, sql.ErrNoRows
	}
	return r.workspace, nil
}

func (r agentPoolBackendRepoForTest) CreateToken(ctx context.Context, workspaceID uint, tokenType string, reusable bool) (types.Token, error) {
	return types.Token{
		Id:          1,
		ExternalId:  "token-agent-test",
		Key:         "worker-token-agent-test",
		Active:      true,
		Reusable:    reusable,
		WorkspaceId: &workspaceID,
		TokenType:   tokenType,
	}, nil
}
