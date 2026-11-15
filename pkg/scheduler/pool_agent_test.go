package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/compute"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// An agent pool is the machines that joined it. Work can be placed on a
// machine that is connected and runs workers in containers; the pool counts
// every machine, and looks after the worker of each.

// agentPoolFixture is a pool of four machines:
//
//	ready      connected, its worker is up
//	starting   connected, its worker has not started yet
//	silent     not heard from for too long, with the most room of all
//	no-workers connected, but it does not run workers in containers
type agentPoolFixture struct {
	ctx        context.Context
	workers    *failingWorkerRepo
	machines   repo.ComputeRepository
	controller *AgentWorkerPoolController
}

const (
	agentPoolWorkspace = "workspace"
	agentPoolName      = "rack"
)

func newAgentPoolFixture(t *testing.T) *agentPoolFixture {
	t.Helper()
	ctx := context.Background()
	rdb, err := repo.NewRedisClientForTest()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	f := &agentPoolFixture{
		ctx:      ctx,
		workers:  &failingWorkerRepo{WorkerRepository: repo.NewWorkerRedisRepositoryForTest(rdb)},
		machines: repo.NewComputeRedisRepository(rdb),
	}

	now := time.Now()
	for _, machine := range []*compute.AgentTokenState{
		{MachineID: "ready", CPUMillicores: 8000, MemoryMB: 16000, LastHeartbeatAt: now, Executor: types.DefaultAgentWorkerContainerMode},
		{MachineID: "starting", CPUMillicores: 4000, MemoryMB: 8000, LastHeartbeatAt: now, Executor: types.DefaultAgentWorkerContainerMode},
		{MachineID: "silent", CPUMillicores: 64000, MemoryMB: 128000, LastHeartbeatAt: now.Add(-2 * compute.AgentHeartbeatTimeout), Executor: types.DefaultAgentWorkerContainerMode},
		{MachineID: "no-workers", CPUMillicores: 32000, MemoryMB: 64000, LastHeartbeatAt: now, Executor: "process"},
	} {
		machine.TokenHash, machine.WorkspaceID, machine.PoolName, machine.Schedulable = "token-of-"+machine.MachineID, agentPoolWorkspace, agentPoolName, true
		require.NoError(t, f.machines.SaveAgentTokenState(ctx, machine, time.Hour))
	}

	f.controller, err = NewAgentWorkerPoolController(AgentWorkerPoolControllerOptions{
		Context: ctx, Name: agentPoolName, WorkspaceID: agentPoolWorkspace,
		WorkerPool: types.WorkerPoolConfig{Mode: types.PoolModeExternal},
		PoolState:  &compute.PoolState{Name: agentPoolName, Selector: agentPoolName, Mode: string(types.PoolModeExternal)},
		WorkerRepo: f.workers, ComputeRepo: f.machines,
	})
	require.NoError(t, err)

	// The connected machines get their workers, and the ready one's starts.
	require.NoError(t, f.controller.reconcileMachines())
	require.NoError(t, f.workers.UpdateWorkerStatus(compute.AgentMachineWorkerID("ready"), types.WorkerStatusAvailable))
	// The silent machine's worker is on record from before it went quiet.
	require.NoError(t, f.workers.AddWorker(&types.Worker{
		Id: compute.AgentMachineWorkerID("silent"), MachineId: "silent", PoolName: agentPoolName, Status: types.WorkerStatusAvailable,
		TotalCpu: 64000, FreeCpu: 64000, TotalMemory: 128000, FreeMemory: 128000,
	}))
	return f
}

func (f *agentPoolFixture) worker(t *testing.T, machineID string) *types.Worker {
	t.Helper()
	worker, err := f.workers.GetWorkerById(compute.AgentMachineWorkerID(machineID))
	require.NoError(t, err)
	return worker
}

func TestAnAgentPoolCountsEveryMachineAndTellsThoseWorkCanBePlacedOn(t *testing.T) {
	f := newAgentPoolFixture(t)

	state, err := f.controller.State()
	require.NoError(t, err)
	require.Equal(t, &types.WorkerPoolState{
		Status:             types.WorkerPoolStatusHealthy,
		RegisteredMachines: 4,
		ReadyMachines:      1,
		PendingMachines:    1,
	}, state)

	// With no machine whose worker is up, the pool is degraded.
	require.NoError(t, f.workers.UpdateWorkerStatus(compute.AgentMachineWorkerID("ready"), types.WorkerStatusPending))
	state, err = f.controller.State()
	require.NoError(t, err)
	require.Equal(t, &types.WorkerPoolState{
		Status:             types.WorkerPoolStatusDegraded,
		RegisteredMachines: 4,
		PendingMachines:    2,
	}, state)

	// A worker that cannot be read is an error, not a machine to count.
	f.workers.getWorker = errors.New("redis is down")
	_, err = f.controller.State()
	require.ErrorIs(t, err, f.workers.getWorker)
}

func TestAnAgentPoolAddsAWorkerOnlyOnAMachineWorkCanBePlacedOn(t *testing.T) {
	f := newAgentPoolFixture(t)

	// Both connected machines have room for a small worker, and only the
	// ready one for a worker of its own size.
	can, err := f.controller.CanAddWorker(1000, 1000, 0)
	require.NoError(t, err)
	require.True(t, can)
	worker, err := f.controller.AddWorker(8000, 1000, 0)
	require.NoError(t, err)
	require.Equal(t, "ready", worker.MachineId)

	// Only the silent machine and the one that runs no workers have room for
	// more than that, and neither can take work.
	can, err = f.controller.CanAddWorker(16000, 1000, 0)
	require.NoError(t, err)
	require.False(t, can)
}

// When no machine has room, the pool says what it had. The scheduler logs
// it as it is for whoever has to find out why a request for a workspace's
// own machines is waiting.
func TestAnAgentPoolWithNoRoomSaysWhatItsMachinesHad(t *testing.T) {
	f := newAgentPoolFixture(t)

	_, err := f.controller.AddWorker(16000, 1000, 0)

	var noRoom *AgentPoolCapacityError
	require.ErrorAs(t, err, &noRoom)
	require.Equal(t, &AgentPoolCapacityError{
		WorkspaceID: agentPoolWorkspace, PoolName: agentPoolName,
		CPU: 16000, Memory: 1000,
		Machines: 4, SchedulableMachines: 2,
		MaxAvailableCPU: 8000, MaxAvailableMemory: 16000,
	}, noRoom)
	require.EqualError(t, err, `no joined agent machine in workspace "workspace" pool "rack" has enough capacity (requested cpu=16000 memory=1000 gpu="" gpu_count=0, schedulable_machines=2/4, max_available_cpu=8000 max_available_memory=16000 max_available_gpu=0)`)

	logged := captureLog(t)
	logAddWorkerFailure(&types.ContainerRequest{ContainerId: "container"}, err)
	var line map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(logged.String())), &line))
	for field, want := range map[string]any{
		"level": "debug", "message": "agent pool capacity unavailable", "container_id": "container",
		"pool_name": "rack", "machines": 4.0, "schedulable_machines": 2.0,
		"max_available_cpu": 8000.0, "max_available_memory": 16000.0, "max_available_gpu": 0.0,
		"error": err.Error(),
	} {
		require.Equal(t, want, line[field], field)
	}

	// A worker that cannot be read does not count towards what was there.
	f.workers.getWorker = errors.New("redis is down")
	require.ErrorAs(t, f.controller.capacityError(16000, 1000, "", 0), &noRoom)
	require.Equal(t, []int{4, 2}, []int{noRoom.Machines, noRoom.SchedulableMachines})
	require.Zero(t, noRoom.MaxAvailableCPU)
}

// The gateway looks at a pool's machines all together when it reconciles
// the pool, and at one alone when that machine's agent sends a heartbeat.
// Either way a machine is treated the same.
func TestAMachineIsLookedAfterTheSameAloneAsWithItsPool(t *testing.T) {
	alone := func(f *agentPoolFixture, machineID string) error { return f.controller.ensureMachine(machineID) }
	withItsPool := func(f *agentPoolFixture, _ string) error { return f.controller.reconcileMachines() }

	for name, reconcile := range map[string]func(*agentPoolFixture, string) error{"alone": alone, "with its pool": withItsPool} {
		t.Run(name, func(t *testing.T) {
			t.Run("a machine that has gone quiet has its worker disabled", func(t *testing.T) {
				f := newAgentPoolFixture(t)
				require.NoError(t, reconcile(f, "silent"))
				require.Equal(t, types.WorkerStatusDisabled, f.worker(t, "silent").Status)
			})

			t.Run("and that is an error when its worker cannot be read", func(t *testing.T) {
				f := newAgentPoolFixture(t)
				f.workers.getWorker = errors.New("redis is down")
				require.ErrorIs(t, reconcile(f, "silent"), f.workers.getWorker)
			})

			t.Run("a machine that runs no workers gets none", func(t *testing.T) {
				f := newAgentPoolFixture(t)
				require.NoError(t, reconcile(f, "no-workers"))
				_, err := f.workers.GetWorkerById(compute.AgentMachineWorkerID("no-workers"))
				var notFound *types.ErrWorkerNotFound
				require.ErrorAs(t, err, &notFound)
			})

			t.Run("a connected machine whose worker is gone gets one that is starting", func(t *testing.T) {
				f := newAgentPoolFixture(t)
				require.NoError(t, f.workers.RemoveWorker(compute.AgentMachineWorkerID("starting")))
				require.NoError(t, reconcile(f, "starting"))
				require.Equal(t, types.WorkerStatusPending, f.worker(t, "starting").Status)
			})
		})
	}

	t.Run("a machine that is not the pool's is not an error", func(t *testing.T) {
		f := newAgentPoolFixture(t)
		require.NoError(t, f.controller.ensureMachine("nobody"))
	})
}

// A machine that dies stops heartbeating and its worker stops sending
// keepalives, while the gateway goes on reconciling the machine's pool. The
// worker's record has to expire all the same: only then do the requests
// queued on it go back to the backlog.
func TestReconcilingAPoolLetsTheWorkerRecordOfADeadMachineExpire(t *testing.T) {
	m := newServingExternalMachine(t, false)
	key := common.RedisKeys.SchedulerWorkerState(m.workerID)
	// Most of the record's lifetime passes with no keepalive.
	const left = 5 * time.Second
	timePasses := func() { require.NoError(t, m.rdb.Expire(m.ctx, key, left).Err()) }
	lifetime := func() time.Duration { return m.rdb.TTL(m.ctx, key).Val() }

	// The machine dies with a request queued on its worker.
	request := &types.ContainerRequest{ContainerId: "queued", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100}
	require.NoError(t, repo.NewContainerRedisRepositoryForTest(m.rdb).SetContainerState(request.ContainerId, &types.ContainerState{
		ContainerId: request.ContainerId, Status: types.ContainerStatusPending, WorkspaceId: request.WorkspaceId,
	}))
	worker, err := m.workers.GetWorkerById(m.workerID)
	require.NoError(t, err)
	require.NoError(t, m.workers.ScheduleContainerRequests(worker, []*types.ContainerRequest{request}))

	// Its last heartbeat still counts for a while, and a reconcile in that
	// time saves its worker. That gives the record no more time.
	timePasses()
	require.NoError(t, m.controller.reconcileMachines())
	require.Equal(t, left, lifetime())

	// Once the heartbeat is too old the worker is disabled.
	m.state.LastHeartbeatAt = time.Now().Add(-2 * compute.AgentHeartbeatTimeout)
	require.NoError(t, m.machines.SaveAgentTokenState(m.ctx, m.state, time.Hour))
	require.NoError(t, m.controller.reconcileMachines())
	worker, err = m.workers.GetWorkerById(m.workerID)
	require.NoError(t, err)
	require.Equal(t, types.WorkerStatusDisabled, worker.Status)

	// A worker that is disabled already is left as it is, whether the pool
	// is reconciled or the gateway looks at the machine alone.
	timePasses()
	for range 3 {
		require.NoError(t, m.controller.reconcileMachines())
		require.NoError(t, m.controller.ensureMachine(m.state.MachineID))
		require.Equal(t, left, lifetime())
	}

	// The record expires. Nothing makes it again, and the next look at the
	// workers returns the request to the backlog.
	require.NoError(t, m.rdb.Del(m.ctx, key).Err())
	require.NoError(t, m.controller.reconcileMachines())
	workers, err := m.workers.GetAllWorkers()
	require.NoError(t, err)
	require.Empty(t, workers)
	require.Zero(t, m.rdb.LLen(m.ctx, common.RedisKeys.SchedulerWorkerRequests(m.workerID)).Val())
	require.EqualValues(t, 1, m.rdb.ZCard(m.ctx, common.RedisKeys.SchedulerContainerRequests()).Val())
}
