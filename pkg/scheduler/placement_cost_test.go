package scheduler

import (
	"context"
	"fmt"
	"testing"

	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

const placementCostPool = "sandboxes"

// placementCostScheduler is a scheduler on a Redis that counts its commands,
// with idle workers in the pool the test's sandboxes select. The workspace's
// concurrency limit is cached, as it is for any workspace that ran lately.
func placementCostScheduler(t *testing.T, workers int) (*Scheduler, *repo.RedisCommandCounts) {
	t.Helper()
	previous := log.Logger
	log.Logger = zerolog.Nop()
	t.Cleanup(func() { log.Logger = previous })

	rdb, counts, err := repo.NewRedisClientForTestWithCounts()
	require.NoError(t, err)
	t.Cleanup(func() { rdb.Close() })

	config := types.AppConfig{}
	config.Worker.Pools = map[string]types.WorkerPoolConfig{placementCostPool: {}}
	backendRepo, _ := repo.NewBackendPostgresRepositoryForTest()
	s := &Scheduler{
		ctx:                       context.Background(),
		config:                    config,
		backendRepo:               backendRepo,
		workerRepo:                repo.NewWorkerRedisRepositoryForTest(rdb),
		containerRepo:             repo.NewContainerRedisRepositoryForTest(rdb),
		workspaceRepo:             repo.NewWorkspaceRedisRepositoryForTest(rdb),
		computeRepo:               repo.NewComputeRedisRepository(rdb),
		workerPoolManager:         NewWorkerPoolManager(),
		requestBacklog:            NewRequestBacklog(rdb),
		credentials:               newSchedulerCredentialCache(),
		provisioning:              newProvisioningTracker(),
		workerProvisioningBackoff: newWorkerProvisioningBackoff(),
	}
	s.workerPoolManager.SetPool(placementCostPool, config.Worker.Pools[placementCostPool], nil)
	require.NoError(t, s.workspaceRepo.SetConcurrencyLimitByWorkspaceId("workspace", &types.ConcurrencyLimit{GPULimit: 100, CPUMillicoreLimit: 1 << 30}))
	for i := 0; i < workers; i++ {
		require.NoError(t, s.workerRepo.AddWorker(&types.Worker{
			Id: fmt.Sprintf("worker-%d", i), PoolName: placementCostPool, Status: types.WorkerStatusAvailable,
			TotalCpu: 1 << 30, FreeCpu: 1 << 30, TotalMemory: 1 << 30, FreeMemory: 1 << 30,
		}))
	}
	return s, counts
}

// placementCostSandbox is the request a sandbox create makes of the scheduler.
func placementCostSandbox(id string) *types.ContainerRequest {
	return &types.ContainerRequest{
		ContainerId: id, StubId: "stub", WorkspaceId: "workspace", Workspace: testWorkspaceWithStorage(),
		Stub:         types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeSandbox)}},
		Cpu:          1000,
		Memory:       1024,
		PoolSelector: placementCostPool,
		Ports:        []uint32{8001},
	}
}

// placeBacklog runs one turn of the scheduler's loop.
func placeBacklog(t *testing.T, s *Scheduler) int {
	t.Helper()
	requests, err := s.requestBacklog.PopN(requestProcessingBatchSize)
	require.NoError(t, err)
	workers, err := s.workerRepo.GetAllWorkers()
	require.NoError(t, err)
	s.processRequestBatch(requests, workers)
	return len(requests)
}

// Admitting a sandbox is Scheduler.Run: the pool lookup, the concurrency
// limit, the container's state and its reservation, and the backlog. It was
// 12 round trips and 3 scripts while the state was written under the
// container's lock.
func TestRedisCommandsToAdmitASandbox(t *testing.T) {
	s, counts := placementCostScheduler(t, 1)
	// The first sandbox loads the scripts and sets up the workspace's usage.
	require.NoError(t, s.Run(placementCostSandbox("sandbox-warm")))

	counts.Start()
	require.NoError(t, s.Run(placementCostSandbox("sandbox-1")))
	commands, roundTrips := counts.Stop()
	t.Logf("admit: %d round trips, %v", roundTrips, commands)
	// The pool the sandbox selects is looked up twice (GET, SMEMBERS each
	// time), the workspace's limit read (HGETALL), the container admitted
	// (one script) and its request queued (ZADD and two ZCARDs together).
	require.Equal(t, 7, roundTrips)
	require.Equal(t, map[string]int{
		"GET": 2, "SMEMBERS": 2, "HGETALL": 1, "EVALSHA": 1, "ZCARD": 2,
		"HGET": 5, "EXISTS": 2, "HINCRBY": 2, "HSET": 3, "SADD": 3, "EXPIRE": 1, "ZADD": 2,
	}, commands)

	state, err := s.containerRepo.GetContainerState("sandbox-1")
	require.NoError(t, err)
	require.Equal(t, types.ContainerStatusPending, state.Status)
	require.Equal(t, int64(2), s.requestBacklog.Len())
}

// Placing is one turn of the scheduler's loop. A turn's fixed cost is shared
// by the sandboxes it places, and so is each worker's.
func TestRedisCommandsToPlaceSandboxes(t *testing.T) {
	const workers, sandboxes = 4, 100
	s, counts := placementCostScheduler(t, workers)
	require.NoError(t, s.Run(placementCostSandbox("sandbox-warm")))
	require.Equal(t, 1, placeBacklog(t, s))

	for i := 0; i < sandboxes; i++ {
		require.NoError(t, s.Run(placementCostSandbox(fmt.Sprintf("sandbox-%d", i))))
	}
	counts.Start()
	require.Equal(t, sandboxes, placeBacklog(t, s))
	commands, roundTrips := counts.Stop()
	t.Logf("place %d sandboxes on %d workers: %d round trips, %v", sandboxes, workers, roundTrips, commands)
	// Pop the backlog (a script), read the workers (two round trips) and the
	// sandboxes' statuses (one), then for the worker that takes them: its
	// lock, its state, the script that queues them, the lock's release, the
	// wake-up and the queue's length.
	require.Equal(t, 10, roundTrips)
	require.Equal(t, 4, commands["EVALSHA"])
	require.Equal(t, sandboxes, commands["RPUSH"])

	placed := 0
	for i := 0; i < workers; i++ {
		queued, err := s.workerRepo.GetNextContainerRequests(fmt.Sprintf("worker-%d", i), 1000)
		require.NoError(t, err)
		placed += len(queued)
	}
	require.Equal(t, sandboxes+1, placed, "the sandboxes and the one that loaded the scripts")
}
