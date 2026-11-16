package scheduler

import (
	"errors"
	"testing"

	"github.com/beam-cloud/beta9/pkg/compute"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// headroomTestPool is the config of a pool that keeps minFreeCpu free.
func headroomTestPool(minFreeCpu string) types.WorkerPoolConfig {
	return types.WorkerPoolConfig{PoolSizing: types.WorkerPoolJobSpecPoolSizingConfig{MinFreeCPU: minFreeCpu, MinFreeMemory: "0", MinFreeGPU: "0"}}
}

// headroomTestWorker is an idle worker of cpu millicores (AddWorker stores free
// capacity from the totals).
func headroomTestWorker(id string, status types.WorkerStatus, cpu int64) *types.Worker {
	return &types.Worker{Id: id, Status: status, PoolName: "default", FreeCpu: cpu, FreeMemory: 10_000, TotalCpu: cpu, TotalMemory: 10_000}
}

// failingPoolWorkerRepo is a worker repository whose pool listing is down.
type failingPoolWorkerRepo struct{ repo.WorkerRepository }

func (failingPoolWorkerRepo) GetAllWorkersInPool(string) ([]*types.Worker, error) {
	return nil, errors.New("redis unavailable")
}

func TestWhichWorkersHoldPoolHeadroom(t *testing.T) {
	rdb, err := repo.NewRedisClientForTest()
	assert.Nil(t, err)
	workerRepo := repo.NewWorkerRedisRepositoryForTest(rdb)

	// Ids are chosen so the order workers are judged in is explicit.
	first := headroomTestWorker("a-first", types.WorkerStatusAvailable, 10_000)
	assert.Nil(t, workerRepo.AddWorker(first))

	// The only ready worker in a pool with a minimum is the headroom.
	assert.True(t, workerHoldsPoolHeadroom(workerRepo, headroomTestPool("4000m"), first))

	// No minimum configured: nothing to hold.
	assert.False(t, workerHoldsPoolHeadroom(workerRepo, headroomTestPool("0"), first))

	// A pending replacement cannot take containers yet, so the incumbent stays.
	pending := headroomTestWorker("b-pending", types.WorkerStatusPending, 10_000)
	assert.Nil(t, workerRepo.AddWorker(pending))
	assert.True(t, workerHoldsPoolHeadroom(workerRepo, headroomTestPool("4000m"), first))

	// Two idle workers that could each cover the minimum alone: exactly one
	// of them holds it, whichever order they are asked in, so they cannot
	// both conclude the other has it covered and leave together.
	second := headroomTestWorker("c-second", types.WorkerStatusAvailable, 6_000)
	assert.Nil(t, workerRepo.AddWorker(second))
	assert.True(t, workerHoldsPoolHeadroom(workerRepo, headroomTestPool("4000m"), first))
	assert.False(t, workerHoldsPoolHeadroom(workerRepo, headroomTestPool("4000m"), second))

	// When the workers ahead fall short, the later one is headroom as well.
	assert.True(t, workerHoldsPoolHeadroom(workerRepo, headroomTestPool("12000m"), first))
	assert.True(t, workerHoldsPoolHeadroom(workerRepo, headroomTestPool("12000m"), second))

	// Enough capacity ahead frees a worker even when its own share is large.
	third := headroomTestWorker("d-third", types.WorkerStatusAvailable, 20_000)
	assert.Nil(t, workerRepo.AddWorker(third))
	assert.False(t, workerHoldsPoolHeadroom(workerRepo, headroomTestPool("12000m"), third))
	assert.True(t, workerHoldsPoolHeadroom(workerRepo, headroomTestPool("12000m"), first))
	assert.True(t, workerHoldsPoolHeadroom(workerRepo, headroomTestPool("12000m"), second))

	// A cordoned worker sorted ahead of the ready ones would otherwise be
	// told it is headroom whenever they fall short, and sit idle forever.
	// The scheduler will not place on it, so it never holds headroom; nor
	// does a worker still pending.
	cordoned := headroomTestWorker("0-cordoned", types.WorkerStatusDisabled, 10_000)
	assert.Nil(t, workerRepo.AddWorker(cordoned))
	assert.Nil(t, workerRepo.SetWorkerCordon(cordoned.Id, true))
	cordoned, err = workerRepo.GetWorkerById(cordoned.Id)
	assert.Nil(t, err)
	assert.Equal(t, types.WorkerStatusDisabled, cordoned.Status)
	assert.False(t, workerHoldsPoolHeadroom(workerRepo, headroomTestPool("120000m"), cordoned))
	assert.False(t, workerHoldsPoolHeadroom(workerRepo, headroomTestPool("120000m"), pending))
	// The ready workers behind it still hold the headroom themselves.
	assert.True(t, workerHoldsPoolHeadroom(workerRepo, headroomTestPool("120000m"), first))

	// A pool without sizing, or a nil worker: never headroom.
	assert.False(t, workerHoldsPoolHeadroom(workerRepo, headroomTestPool("4000m"), nil))
	assert.False(t, workerHoldsPoolHeadroom(workerRepo, types.WorkerPoolConfig{}, first))

	// A repository outage must not read as "free to go".
	assert.True(t, workerHoldsPoolHeadroom(failingPoolWorkerRepo{workerRepo}, headroomTestPool("4000m"), second))
	assert.False(t, workerHoldsPoolHeadroom(failingPoolWorkerRepo{workerRepo}, headroomTestPool("0"), second))
}

func TestFreeCapacityAheadCoversMinimumWhenPoolDoes(t *testing.T) {
	sizing := &types.WorkerPoolSizingConfig{MinFreeCpu: 5_000}
	workers := []*types.Worker{
		headroomTestWorker("w1", types.WorkerStatusAvailable, 2_000),
		headroomTestWorker("w2", types.WorkerStatusAvailable, 2_000),
		headroomTestWorker("w3", types.WorkerStatusDisabled, 50_000),
		headroomTestWorker("w4", types.WorkerStatusAvailable, 2_000),
		headroomTestWorker("w5", types.WorkerStatusAvailable, 2_000),
	}

	// The headroom set is the id-ordered prefix up to where the minimum is
	// met; what it keeps is at least the minimum.
	var kept int64
	held := map[string]bool{}
	for _, w := range workers {
		if w.Status != types.WorkerStatusAvailable {
			continue
		}
		held[w.Id] = freeCapacityAhead(workers, w.Id).belowMinimum(sizing)
		if held[w.Id] {
			kept += w.FreeCpu
		}
	}
	assert.Equal(t, map[string]bool{"w1": true, "w2": true, "w4": true, "w5": false}, held)
	assert.GreaterOrEqual(t, kept, sizing.MinFreeCpu)
}

// The floor is the pool's own, wherever the pool came from. A pool created
// through the pools API is not in the config file and used to get no floor.
func TestWorkerHoldsHeadroomOfThePoolItBelongsTo(t *testing.T) {
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	s.config.Worker.Pools = map[string]types.WorkerPoolConfig{}
	s.workerPoolManager = NewWorkerPoolManager()

	idle := func(id, pool string) *types.Worker {
		worker := &types.Worker{Id: id, Status: types.WorkerStatusAvailable, PoolName: pool, FreeCpu: 8000, FreeMemory: 8192, TotalCpu: 8000, TotalMemory: 8192}
		require.NoError(t, s.workerRepo.AddWorker(worker))
		return worker
	}

	// A Kubernetes pool from the config file.
	s.workerPoolManager.SetPool("kubernetes", headroomTestPool("4000m"), &LocalWorkerPoolControllerForTest{name: "kubernetes"})
	require.True(t, s.WorkerHoldsPoolHeadroom(idle("k-1", "kubernetes")))

	// A managed pool created through the pools API.
	managed := headroomTestPool("4000m")
	state := &compute.PoolState{Name: "managed", Selector: "managed", ManagementSource: "managed", WorkerConfig: &managed}
	require.NoError(t, s.EnsureAgentPool("admin-workspace", state))
	worker := idle("m-1", "managed")
	worker.PoolSelector, worker.WorkspaceId = "managed", "admin-workspace"
	require.True(t, s.WorkerHoldsPoolHeadroom(worker))

	// A pool that keeps nothing free, and a pool this replica has not loaded.
	s.workerPoolManager.SetPool("no-floor", types.WorkerPoolConfig{}, &LocalWorkerPoolControllerForTest{name: "no-floor"})
	require.False(t, s.WorkerHoldsPoolHeadroom(idle("n-1", "no-floor")))
	require.False(t, s.WorkerHoldsPoolHeadroom(idle("u-1", "unknown")))
	require.False(t, s.WorkerHoldsPoolHeadroom(nil))
}

// A workspace's own pool may have the name of a shared pool. Its workers
// follow their own pool's floor, not the shared pool's.
func TestWorkerOfAPrivatePoolIgnoresASharedPoolOfTheSameName(t *testing.T) {
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	s.workerPoolManager = NewWorkerPoolManager()
	s.workerPoolManager.SetPool("gpu", headroomTestPool("4000m"), &LocalWorkerPoolControllerForTest{name: "gpu"})
	require.NoError(t, s.EnsureAgentPool("tenant", &compute.PoolState{Name: "gpu", Selector: "gpu", Mode: string(types.PoolModePrivate)}))

	shared := &types.Worker{Id: "shared-1", Status: types.WorkerStatusAvailable, PoolName: "gpu", FreeCpu: 8000, FreeMemory: 8192}
	private := &types.Worker{Id: "private-1", Status: types.WorkerStatusAvailable, PoolName: "gpu", PoolSelector: "gpu", WorkspaceId: "tenant", FreeCpu: 8000, FreeMemory: 8192}
	require.NoError(t, s.workerRepo.AddWorker(shared))

	require.True(t, s.WorkerHoldsPoolHeadroom(shared))
	require.False(t, s.WorkerHoldsPoolHeadroom(private))
}

// An opportunistic request leaves a pool's floor idle, for a pool created
// through the pools API as for one from the config file.
func TestOpportunisticRequestLeavesTheFloorOfAnAPIPool(t *testing.T) {
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	s.config.Worker.Pools = map[string]types.WorkerPoolConfig{}
	s.workerPoolManager = NewWorkerPoolManager()
	managed := headroomTestPool("6000m")
	require.NoError(t, s.EnsureAgentPool("admin-workspace", &compute.PoolState{Name: "managed", Selector: "managed", ManagementSource: "managed", WorkerConfig: &managed}))

	workers := []*types.Worker{{
		Id: "m-1", Status: types.WorkerStatusAvailable, PoolName: "managed", PoolSelector: "managed", WorkspaceId: "admin-workspace",
		FreeCpu: 8000, FreeMemory: 8192, TotalCpu: 8000, TotalMemory: 8192,
	}}
	opportunistic := func(cpu int64) *types.ContainerRequest {
		return &types.ContainerRequest{ContainerId: "c", Cpu: cpu, Memory: 1024, OpportunisticOnly: true}
	}

	require.Len(t, s.filterWorkersByPoolHeadroom(workers, opportunistic(2000)), 1, "8 vCPU free, 6 kept: 2 may be used")
	require.Empty(t, s.filterWorkersByPoolHeadroom(workers, opportunistic(3000)), "3 vCPU would cut into the floor")
	require.Len(t, s.filterWorkersByPoolHeadroom(workers, &types.ContainerRequest{ContainerId: "c", Cpu: 3000, Memory: 1024}), 1, "the floor is there for regular requests")
}

// A GPU on a worker that has no CPU or memory left cannot be used, so it is
// not part of the floor: the pool's sizer does not count it either.
func TestOpportunisticRequestLeavesAFloorOfGPUsThatCanBeUsed(t *testing.T) {
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	s.config.Worker.Pools = map[string]types.WorkerPoolConfig{}
	s.workerPoolManager = NewWorkerPoolManager()
	managed := types.WorkerPoolConfig{GPUType: "A10G", PoolSizing: types.WorkerPoolJobSpecPoolSizingConfig{MinFreeCPU: "0", MinFreeMemory: "0", MinFreeGPU: "1"}}
	require.NoError(t, s.EnsureAgentPool("admin-workspace", &compute.PoolState{Name: "managed", Selector: "managed", ManagementSource: "managed", WorkerConfig: &managed}))

	gpuWorker := func(id string, freeCPU int64) *types.Worker {
		return &types.Worker{
			Id: id, Status: types.WorkerStatusAvailable, PoolName: "managed", PoolSelector: "managed", WorkspaceId: "admin-workspace",
			Gpu: "A10G", FreeGpuCount: 1, TotalGpuCount: 2, FreeCpu: freeCPU, FreeMemory: 8192, TotalCpu: 8000, TotalMemory: 16384,
		}
	}
	request := &types.ContainerRequest{ContainerId: "c", Cpu: 1000, Memory: 1024, Gpu: "A10G", GpuCount: 1, OpportunisticOnly: true}

	usable := []*types.Worker{gpuWorker("m-1", 4000), gpuWorker("m-2", 4000)}
	require.Len(t, s.filterWorkersByPoolHeadroom(usable, request), 2, "two GPUs free, one kept: one may be used")

	oneOutOfCPU := []*types.Worker{gpuWorker("m-1", 0), gpuWorker("m-2", 4000)}
	require.Empty(t, s.filterWorkersByPoolHeadroom(oneOutOfCPU, request), "the one GPU that can be used is the floor")
}
