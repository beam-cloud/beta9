package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// A workspace names its own pools, and may pick the name of a shared pool.
// The two are different pools: what is asked about the shared one must not
// be answered from the workspace's.

// ownPool registers a pool of machines that belong to one workspace.
func ownPool(manager *WorkerPoolManager, workspaceID, name string) {
	key := agentPoolControllerKey(workspaceID, &compute.PoolState{Selector: name})
	manager.SetPoolAt(key, name, types.WorkerPoolConfig{Mode: types.PoolModePrivate}, nil)
}

func TestPoolConfigsLeavesOutTheWorkspacesOwnPools(t *testing.T) {
	manager := NewWorkerPoolManager()
	manager.SetPool("cpu", types.WorkerPoolConfig{Mode: types.PoolModeExternal}, nil)
	ownPool(manager, "workspace-1", "cpu")
	ownPool(manager, "workspace-1", "rack")
	s := &Scheduler{workerPoolManager: manager}

	// The pools come out in a different order each time.
	for range 100 {
		modes := map[string]types.PoolMode{}
		for name, config := range s.PoolConfigs() {
			modes[name] = config.Mode
		}
		require.Equal(t, map[string]types.PoolMode{"cpu": types.PoolModeExternal}, modes)
	}
}

func TestEnforcementStopsWorkOnASharedPoolThatHasTheNameOfSomeonesOwnPool(t *testing.T) {
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	ctx := context.Background()
	rdb := s.requestBacklog.rdb
	s.backendRepo = &enforcementStubRepo{calls: map[string]int{}}
	s.creditGate = newCreditGate(types.CreditGateConfig{}, &fakeCreditBackend{decision: creditDecision{OK: false}}, rdb)

	s.workerPoolManager.SetPool("gpu", types.WorkerPoolConfig{Mode: types.PoolModeExternal, GPUType: "A10G"}, nil)
	ownPool(s.workerPoolManager, "owner", "gpu")
	for _, worker := range []*types.Worker{
		{Id: "shared", PoolName: "gpu", PoolSelector: "gpu", WorkspaceId: "admin", ControlPlaneManaged: true, Status: types.WorkerStatusAvailable},
		{Id: "own", PoolName: "gpu", PoolSelector: "gpu", WorkspaceId: "owner", Status: types.WorkerStatusAvailable},
	} {
		require.NoError(t, s.workerRepo.AddWorker(worker))
	}
	for _, container := range []types.ContainerState{
		{ContainerId: "on-shared", WorkspaceId: "customer", WorkerId: "shared"},
		{ContainerId: "on-own", WorkspaceId: "owner", WorkerId: "own"},
	} {
		container.StubId, container.Status, container.ScheduledAt = "customer", types.ContainerStatusRunning, time.Now().Unix()
		require.NoError(t, s.containerRepo.SetContainerState(container.ContainerId, &container))
		require.NoError(t, rdb.SAdd(ctx, common.RedisKeys.SchedulerContainerWorkerIndex(container.WorkerId), common.RedisKeys.SchedulerContainerState(container.ContainerId)).Err())
	}

	require.NoError(t, s.enforceCredits(ctx))

	billed, err := s.containerRepo.GetContainerState("on-shared")
	require.NoError(t, err)
	require.Equal(t, types.ContainerStatusStopping, billed.Status, "the shared pool is paid for with credit")
	exempt, err := s.containerRepo.GetContainerState("on-own")
	require.NoError(t, err)
	require.Equal(t, types.ContainerStatusRunning, exempt.Status, "a workspace's own machines are not")
}
