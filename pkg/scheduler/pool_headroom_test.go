package scheduler

import (
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

func TestWorkerHoldsPoolHeadroom(t *testing.T) {
	rdb, err := repo.NewRedisClientForTest()
	assert.Nil(t, err)
	workerRepo := repo.NewWorkerRedisRepositoryForTest(rdb)

	only := headroomTestWorker("only", types.WorkerStatusAvailable, 10_000)
	assert.Nil(t, workerRepo.AddWorker(only))

	// The only ready worker in a pool with a minimum is the headroom.
	assert.True(t, WorkerHoldsPoolHeadroom(workerRepo, headroomTestConfig("4000m"), only))

	// No minimum configured: nothing to hold.
	assert.False(t, WorkerHoldsPoolHeadroom(workerRepo, headroomTestConfig("0"), only))

	// A pending replacement cannot take containers yet, so the incumbent stays.
	pending := headroomTestWorker("pending", types.WorkerStatusPending, 10_000)
	assert.Nil(t, workerRepo.AddWorker(pending))
	assert.True(t, WorkerHoldsPoolHeadroom(workerRepo, headroomTestConfig("4000m"), only))

	// Once another ready worker covers the minimum on its own, it is free to go.
	other := headroomTestWorker("other", types.WorkerStatusAvailable, 6_000)
	assert.Nil(t, workerRepo.AddWorker(other))
	assert.False(t, WorkerHoldsPoolHeadroom(workerRepo, headroomTestConfig("4000m"), only))

	// ...but not when the others together still fall short of it.
	assert.True(t, WorkerHoldsPoolHeadroom(workerRepo, headroomTestConfig("8000m"), only))

	// Unknown pool or nil worker: never headroom.
	assert.False(t, WorkerHoldsPoolHeadroom(workerRepo, headroomTestConfig("4000m"), nil))
	assert.False(t, WorkerHoldsPoolHeadroom(workerRepo, types.AppConfig{}, only))
}
