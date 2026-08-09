package abstractions

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type durableDiskTestBackendRepo struct {
	repository.BackendRepository
	snapshot *types.DiskSnapshot
	err      error
}

func (r durableDiskTestBackendRepo) GetLatestDiskSnapshot(context.Context, uint, string) (*types.DiskSnapshot, error) {
	return r.snapshot, r.err
}

type durableDiskTestWorkerRepo struct {
	repository.WorkerRepository
	workers []*types.Worker
	err     error
}

func (r durableDiskTestWorkerRepo) GetAllWorkersInPool(string) ([]*types.Worker, error) {
	return r.workers, r.err
}

type durableDiskTestPoolRepo struct {
	repository.WorkerPoolRepository
	state *types.WorkerPoolState
	err   error
}

func (r durableDiskTestPoolRepo) GetWorkerPoolState(context.Context, string) (*types.WorkerPoolState, error) {
	return r.state, r.err
}

func TestDurableDiskPlacementKeepsNewDiskOnUnavailablePrivatePool(t *testing.T) {
	config := &types.StubConfigV1{
		Pool:  &types.PoolConfig{Name: "gpu-pool", Selector: "gpu-pool"},
		Disks: []*pb.DurableDisk{{Name: "new-disk"}},
	}
	repos := DurableDiskPlacementRepos{
		BackendRepo: durableDiskTestBackendRepo{err: &types.ErrDiskSnapshotNotFound{SnapshotId: "latest:1:new-disk"}},
		WorkerRepo:  durableDiskTestWorkerRepo{},
	}

	if err := ConfigureDurableDiskPlacement(context.Background(), repos, &types.Workspace{Id: 1}, config); err != nil {
		t.Fatalf("ConfigureDurableDiskPlacement() error = %v", err)
	}
	if got := config.PoolSelector(); got != "gpu-pool" {
		t.Fatalf("pool selector = %q, want gpu-pool", got)
	}
	if got := config.Disks[0].Driver; got != types.DurableDiskDriverSnapshot {
		t.Fatalf("disk driver = %q, want %q", got, types.DurableDiskDriverSnapshot)
	}
}

func TestPrivatePoolAvailabilityChecksLiveWorkersAfterStaleZeroState(t *testing.T) {
	repos := DurableDiskPlacementRepos{
		WorkerPoolRepo: durableDiskTestPoolRepo{state: &types.WorkerPoolState{ReadyMachines: 0}},
		WorkerRepo: durableDiskTestWorkerRepo{workers: []*types.Worker{{
			PoolName: "gpu-pool",
			Status:   types.WorkerStatusAvailable,
		}}},
	}

	if !privatePoolHasAvailableWorkers(context.Background(), repos, "gpu-pool") {
		t.Fatal("privatePoolHasAvailableWorkers() = false, want true for live available worker")
	}
}
