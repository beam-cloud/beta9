package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/compute"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// A machine that drops out of a workspace's own pool leaves its worker on
// record for a while. A request for the pool is placed only on a worker
// whose machine is connected, or it would wait there for nothing.

// unlistableMachines is a compute repository whose machines cannot be listed
// while err is set.
type unlistableMachines struct {
	repo.ComputeRepository
	err error
}

func (r *unlistableMachines) ListAgentTokenStates(ctx context.Context, workspaceID, poolName string) ([]*compute.AgentTokenState, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.ComputeRepository.ListAgentTokenStates(ctx, workspaceID, poolName)
}

// ownPoolFixture is a workspace's own pool with two machines, each with a
// worker that is up and has room. One machine is connected; the other has
// not been heard from for too long.
type ownPoolFixture struct {
	*Scheduler
	machines   *unlistableMachines
	controller *AgentWorkerPoolController
	request    *types.ContainerRequest
	connected  string
	silent     string
}

func newOwnPoolFixture(t *testing.T) *ownPoolFixture {
	t.Helper()
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	s.workerPoolManager = NewWorkerPoolManager()
	f := &ownPoolFixture{
		Scheduler: s,
		machines:  &unlistableMachines{ComputeRepository: s.computeRepo},
		connected: compute.AgentMachineWorkerID("connected"),
		silent:    compute.AgentMachineWorkerID("silent"),
	}
	s.computeRepo = f.machines

	const workspace = "workspace"
	// The pool's name is not what requests select it by.
	pool := &compute.PoolState{Name: "rack-1", Selector: "own-cpu", Mode: string(types.PoolModePrivate)}
	ctx := context.Background()
	require.NoError(t, s.computeRepo.SavePoolState(ctx, workspace, pool))
	now := time.Now()
	for machine, lastHeard := range map[string]time.Time{"connected": now, "silent": now.Add(-2 * compute.AgentHeartbeatTimeout)} {
		require.NoError(t, s.computeRepo.SaveAgentTokenState(ctx, &compute.AgentTokenState{
			TokenHash: "token-of-" + machine, WorkspaceID: workspace, PoolName: pool.Name, MachineID: machine,
			CPUMillicores: 4000, MemoryMB: 8192, Executor: types.DefaultAgentWorkerContainerMode,
			Schedulable: true, LastHeartbeatAt: lastHeard,
		}, time.Hour))
		require.NoError(t, s.workerRepo.AddWorker(&types.Worker{
			Id: compute.AgentMachineWorkerID(machine), MachineId: machine, Status: types.WorkerStatusAvailable,
			PoolName: pool.Name, PoolSelector: pool.Selector, WorkspaceId: workspace, RequiresPoolSelector: true,
			TotalCpu: 4000, FreeCpu: 4000, TotalMemory: 8192, FreeMemory: 8192,
		}))
	}
	require.NoError(t, s.EnsureAgentPool(workspace, pool))
	loaded, ok := s.privateAgentPool(workspace, pool.Selector)
	require.True(t, ok)
	f.controller = loaded.Controller.(*AgentWorkerPoolController)

	f.request = &types.ContainerRequest{
		ContainerId: "container", WorkspaceId: workspace, PoolSelector: pool.Selector,
		Cpu: 1000, Memory: 1000, Workspace: testWorkspaceWithStorage(),
	}
	return f
}

// candidates returns the workers the request may be placed on.
func (f *ownPoolFixture) candidates(t *testing.T) []string {
	t.Helper()
	workers, err := f.workerRepo.GetAllWorkers()
	require.NoError(t, err)
	var ids []string
	for _, worker := range f.filterLivePrivateAgentWorkers(workers, f.request) {
		ids = append(ids, worker.Id)
	}
	return ids
}

func TestAnOwnPoolsLiveWorkersAreThoseOfItsConnectedMachines(t *testing.T) {
	f := newOwnPoolFixture(t)

	live, err := f.controller.liveWorkerIDs()

	require.NoError(t, err)
	require.Equal(t, map[string]struct{}{f.connected: {}}, live)
}

func TestARequestForAnOwnPoolIsPlacedOnlyOnAConnectedMachine(t *testing.T) {
	f := newOwnPoolFixture(t)
	require.Equal(t, []string{f.connected}, f.candidates(t))

	// The silent machine has the more room, and still is not chosen.
	silent, err := f.workerRepo.GetWorkerById(f.silent)
	require.NoError(t, err)
	silent.TotalCpu, silent.FreeCpu = 64000, 64000
	require.NoError(t, f.workerRepo.AddWorker(silent))
	selected, err := f.selectWorker(f.request)
	require.NoError(t, err)
	require.Equal(t, f.connected, selected.Id)

	// A request of another workspace, or for another pool, is not this
	// pool's to filter.
	workers, err := f.workerRepo.GetAllWorkers()
	require.NoError(t, err)
	for _, other := range []*types.ContainerRequest{
		{WorkspaceId: "another-workspace", PoolSelector: f.request.PoolSelector},
		{WorkspaceId: f.request.WorkspaceId, PoolSelector: "another-pool"},
		{WorkspaceId: f.request.WorkspaceId},
	} {
		require.Len(t, f.filterLivePrivateAgentWorkers(workers, other), 2)
	}
}

// When the pool's machines cannot be listed, none of its workers can be
// ruled out: the request is placed as if all were connected, and the failure
// is reported.
func TestAnOwnPoolsWorkersAreAllKeptWhenItsMachinesCannotBeListed(t *testing.T) {
	f := newOwnPoolFixture(t)
	f.machines.err = errors.New("redis is down")
	logged := captureLog(t)

	require.ElementsMatch(t, []string{f.connected, f.silent}, f.candidates(t))

	require.Contains(t, logged.String(), "redis is down")
	require.Contains(t, logged.String(), `"pool_name":"own-cpu"`)
	require.Contains(t, logged.String(), `"container_id":"container"`)
}
