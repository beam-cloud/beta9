package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/compute"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// An external machine's record in Redis has no TTL. Its worker's record has
// one, so under memory pressure Redis evicts the worker record while the
// machine and the worker keep running. These tests follow the machine through
// that, with the pool controller doing what the gateway does on each agent
// heartbeat and the worker sending its keepalives.

type externalMachine struct {
	ctx        context.Context
	rdb        *common.RedisClient
	workers    repo.WorkerRepository
	machines   repo.ComputeRepository
	controller *AgentWorkerPoolController
	state      *compute.AgentTokenState
	workerID   string
}

// keepAlive is the keepalive of the machine's started worker: the gateway that
// receives it holds the worker's container request stream.
func (m *externalMachine) keepAlive() error {
	return m.workers.SetWorkerKeepAlive(m.workerID, types.WorkerKeepAlive{MachineId: m.state.MachineID, RequestStreamOpen: true})
}

func (m *externalMachine) requirePoolState(t *testing.T, ready, pending int64) {
	t.Helper()
	state, err := m.controller.State()
	require.NoError(t, err)
	require.Equal(t, []int64{ready, pending}, []int64{state.ReadyMachines, state.PendingMachines}, "ready and pending machines")
}

// newServingExternalMachine joins one machine to an external pool and starts
// its worker: the controller creates the worker record, the worker sends its
// first keepalive, and its request stream opens and marks it available.
func newServingExternalMachine(t *testing.T, cordoned bool) *externalMachine {
	t.Helper()
	ctx := context.Background()
	rdb, err := repo.NewRedisClientForTest()
	require.NoError(t, err)
	m := &externalMachine{
		ctx: ctx, rdb: rdb,
		workers:  repo.NewWorkerRedisRepositoryForTest(rdb),
		machines: repo.NewComputeRedisRepository(rdb),
		state: &compute.AgentTokenState{
			TokenHash: "agent-token", WorkspaceID: "workspace", PoolName: "microvm-cpu", MachineID: "machine-1",
			CPUMillicores: 8000, MemoryMB: 16384, Executor: types.DefaultAgentWorkerContainerMode,
			Schedulable: true, Cordoned: cordoned, LastHeartbeatAt: time.Now(),
		},
	}
	m.workerID = compute.AgentMachineWorkerID(m.state.MachineID)
	require.NoError(t, m.machines.SaveAgentTokenState(ctx, m.state, time.Hour))

	poolConfig := types.WorkerPoolConfig{Mode: types.PoolModeExternal, ContainerRuntime: types.ContainerRuntimeMicroVM.String()}
	m.controller, err = NewAgentWorkerPoolController(AgentWorkerPoolControllerOptions{
		Context: ctx, Name: m.state.PoolName, WorkspaceID: m.state.WorkspaceID, WorkerPool: poolConfig,
		PoolState:  &compute.PoolState{Name: m.state.PoolName, Selector: m.state.PoolName, Mode: string(types.PoolModeExternal)},
		WorkerRepo: m.workers, ComputeRepo: m.machines,
	})
	require.NoError(t, err)

	require.NoError(t, m.controller.ensureMachine(m.state.MachineID))
	require.NoError(t, m.workers.SetWorkerKeepAlive(m.workerID, types.WorkerKeepAlive{MachineId: m.state.MachineID}))
	require.NoError(t, m.workers.ToggleWorkerAvailable(m.workerID, ""))
	return m
}

func TestExternalMachineIsReadyAgainAfterItsWorkerRecordIsEvicted(t *testing.T) {
	m := newServingExternalMachine(t, false)
	m.requirePoolState(t, 1, 0)

	// Redis evicts the worker record. The machine record stays.
	require.NoError(t, m.rdb.Del(m.ctx, common.RedisKeys.SchedulerWorkerState(m.workerID)).Err())
	var notFound *types.ErrWorkerNotFound
	require.ErrorAs(t, m.keepAlive(), &notFound, "the worker's keepalive does not recreate its record")

	// The gateway's next agent heartbeat recreates the record, as pending.
	require.NoError(t, m.controller.ensureMachine(m.state.MachineID))
	m.requirePoolState(t, 0, 1)
	capacity, err := m.controller.FreeCapacity()
	require.NoError(t, err)
	require.Zero(t, capacity.FreeCpu, "nothing can be placed on a pending worker")

	// The worker's next keepalive makes the machine usable again.
	require.NoError(t, m.keepAlive())
	m.requirePoolState(t, 1, 0)
	capacity, err = m.controller.FreeCapacity()
	require.NoError(t, err)
	require.EqualValues(t, 8000, capacity.FreeCpu)

	// Later heartbeats and keepalives keep it that way, with one worker.
	require.NoError(t, m.controller.ensureMachine(m.state.MachineID))
	require.NoError(t, m.keepAlive())
	m.requirePoolState(t, 1, 0)
	workers, err := m.workers.GetAllWorkersInPool(m.state.PoolName)
	require.NoError(t, err)
	require.Len(t, workers, 1)
}

func TestCordonedExternalMachineStaysDisabledAfterItsWorkerRecordIsEvicted(t *testing.T) {
	m := newServingExternalMachine(t, true)
	worker, err := m.workers.GetWorkerById(m.workerID)
	require.NoError(t, err)
	require.Equal(t, types.WorkerStatusDisabled, worker.Status)

	require.NoError(t, m.rdb.Del(m.ctx, common.RedisKeys.SchedulerWorkerState(m.workerID)).Err())
	require.NoError(t, m.controller.ensureMachine(m.state.MachineID))
	require.NoError(t, m.keepAlive())

	worker, err = m.workers.GetWorkerById(m.workerID)
	require.NoError(t, err)
	require.Equal(t, types.WorkerStatusDisabled, worker.Status)
	require.True(t, worker.CordonRequested)
	m.requirePoolState(t, 0, 1)
}

func TestRemovedExternalMachineStaysRemovedWhileItsWorkerKeepsSendingKeepAlives(t *testing.T) {
	m := newServingExternalMachine(t, false)

	// An operator deletes the machine: its worker and its own records go.
	require.NoError(t, m.workers.RemoveWorker(m.workerID))
	require.NoError(t, m.machines.DeleteAgentMachineState(m.ctx, m.state.WorkspaceID, m.state.PoolName, m.state.MachineID))

	var notFound *types.ErrWorkerNotFound
	for range 2 {
		require.ErrorAs(t, m.keepAlive(), &notFound)
		require.NoError(t, m.controller.ensureMachine(m.state.MachineID))
		require.NoError(t, m.controller.reconcileMachines())
	}
	_, err := m.workers.GetWorkerById(m.workerID)
	require.ErrorAs(t, err, &notFound)
	m.requirePoolState(t, 0, 0)
}
