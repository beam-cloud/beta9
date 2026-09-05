package scheduler

import (
	"errors"
	"testing"

	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
)

func headroomTestConfig(minFreeCpu string) types.AppConfig {
	return types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
		"default": {PoolSizing: types.WorkerPoolJobSpecPoolSizingConfig{MinFreeCPU: minFreeCpu, MinFreeMemory: "0", MinFreeGPU: "0"}},
	}}}
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

func TestWorkerHoldsPoolHeadroom(t *testing.T) {
	rdb, err := repo.NewRedisClientForTest()
	assert.Nil(t, err)
	workerRepo := repo.NewWorkerRedisRepositoryForTest(rdb)

	// Ids are chosen so the order workers are judged in is explicit.
	first := headroomTestWorker("a-first", types.WorkerStatusAvailable, 10_000)
	assert.Nil(t, workerRepo.AddWorker(first))

	// The only ready worker in a pool with a minimum is the headroom.
	assert.True(t, WorkerHoldsPoolHeadroom(workerRepo, headroomTestConfig("4000m"), first))

	// No minimum configured: nothing to hold.
	assert.False(t, WorkerHoldsPoolHeadroom(workerRepo, headroomTestConfig("0"), first))

	// A pending replacement cannot take containers yet, so the incumbent stays.
	pending := headroomTestWorker("b-pending", types.WorkerStatusPending, 10_000)
	assert.Nil(t, workerRepo.AddWorker(pending))
	assert.True(t, WorkerHoldsPoolHeadroom(workerRepo, headroomTestConfig("4000m"), first))

	// Two idle workers that could each cover the minimum alone: exactly one
	// of them holds it, whichever order they are asked in, so they cannot
	// both conclude the other has it covered and leave together.
	second := headroomTestWorker("c-second", types.WorkerStatusAvailable, 6_000)
	assert.Nil(t, workerRepo.AddWorker(second))
	assert.True(t, WorkerHoldsPoolHeadroom(workerRepo, headroomTestConfig("4000m"), first))
	assert.False(t, WorkerHoldsPoolHeadroom(workerRepo, headroomTestConfig("4000m"), second))

	// When the workers ahead fall short, the later one is headroom as well.
	assert.True(t, WorkerHoldsPoolHeadroom(workerRepo, headroomTestConfig("12000m"), first))
	assert.True(t, WorkerHoldsPoolHeadroom(workerRepo, headroomTestConfig("12000m"), second))

	// Enough capacity ahead frees a worker even when its own share is large.
	third := headroomTestWorker("d-third", types.WorkerStatusAvailable, 20_000)
	assert.Nil(t, workerRepo.AddWorker(third))
	assert.False(t, WorkerHoldsPoolHeadroom(workerRepo, headroomTestConfig("12000m"), third))
	assert.True(t, WorkerHoldsPoolHeadroom(workerRepo, headroomTestConfig("12000m"), first))
	assert.True(t, WorkerHoldsPoolHeadroom(workerRepo, headroomTestConfig("12000m"), second))

	// Unknown pool or nil worker: never headroom.
	assert.False(t, WorkerHoldsPoolHeadroom(workerRepo, headroomTestConfig("4000m"), nil))
	assert.False(t, WorkerHoldsPoolHeadroom(workerRepo, types.AppConfig{}, first))

	// A repository outage must not read as "free to go".
	assert.True(t, WorkerHoldsPoolHeadroom(failingPoolWorkerRepo{workerRepo}, headroomTestConfig("4000m"), second))
	assert.False(t, WorkerHoldsPoolHeadroom(failingPoolWorkerRepo{workerRepo}, headroomTestConfig("0"), second))
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
