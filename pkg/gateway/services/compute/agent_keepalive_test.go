package compute

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// Of an external machine's records, two have a TTL and can be evicted by Redis
// under memory pressure: the machine's agent token and its worker's record.
// The machine record has none. The machine and its worker keep running and
// keep sending their heartbeats and keepalives.
func TestMachineIsAvailableAgainAfterRedisEvictsItsRecords(t *testing.T) {
	ctx := context.Background()
	rdb, err := repository.NewRedisClientForTest()
	require.NoError(t, err)
	workerRepo := repository.NewWorkerRedisRepositoryForTest(rdb)
	computeRepo := repository.NewComputeRedisRepository(rdb)
	service := &Service{
		workerRepo: workerRepo, computeRepo: computeRepo,
		scheduler: scheduler.NewSchedulerForCapacityChecks(workerRepo, computeRepo, scheduler.NewWorkerPoolManager()),
	}
	pool := &model.PoolState{Name: "microvm-cpu", Selector: "microvm-cpu", Mode: string(types.PoolModeExternal)}
	require.NoError(t, service.savePrivatePoolState(ctx, "workspace", pool))
	machine := &model.AgentTokenState{
		TokenHash: hashComputeToken("agent-token"), WorkspaceID: "workspace", PoolName: pool.Name, MachineID: "machine-1",
		Mode: string(types.PoolModeExternal), Executor: types.DefaultAgentWorkerContainerMode,
		CPUMillicores: 8000, MemoryMB: 16384, Schedulable: true, LastHeartbeatAt: time.Now(),
	}
	require.NoError(t, service.saveComputeAgentTokenState(ctx, machine))
	workerID := model.AgentMachineWorkerID(machine.MachineID)

	// agentHeartbeat is what StreamAgent does on each heartbeat tick.
	agentHeartbeat := func() {
		t.Helper()
		current, err := service.touchAgentHeartbeat(ctx, machine)
		require.NoError(t, err)
		machine = current
		if worker := service.agentMachineStatusWorker(machine); worker == nil || worker.Status == types.WorkerStatusDisabled {
			require.NoError(t, service.ensureAgentMachine(ctx, machine, nil))
		}
	}
	// workerKeepAlive is the keepalive of the machine's started worker: the
	// gateway that receives it holds the worker's container request stream.
	workerKeepAlive := func() error {
		return workerRepo.SetWorkerKeepAlive(workerID, types.WorkerKeepAlive{MachineId: machine.MachineID, RequestStreamOpen: true})
	}
	status := func() types.MachineStatus { return types.MachineStatus(service.agentMachineToProto(machine).Status) }

	// The machine joins, its worker starts and opens its request stream.
	agentHeartbeat()
	require.Equal(t, types.MachineStatusPending, status())
	require.NoError(t, workerRepo.ToggleWorkerAvailable(workerID, ""))
	require.Equal(t, types.MachineStatusAvailable, status())

	// Redis evicts both records.
	require.NoError(t, rdb.Del(ctx, common.RedisKeys.SchedulerWorkerState(workerID), common.RedisKeys.ComputeAgentToken(machine.TokenHash)).Err())
	require.Equal(t, types.MachineStatusRegistered, status())
	_, problem := service.requireAgentState(ctx, "agent-token")
	require.NotEmpty(t, problem)
	require.Error(t, workerKeepAlive())

	// The agent's next heartbeat restores its token and recreates the worker
	// record, as pending.
	agentHeartbeat()
	_, problem = service.requireAgentState(ctx, "agent-token")
	require.Empty(t, problem)
	require.Equal(t, types.MachineStatusPending, status())

	// The worker's next keepalive makes the machine available again.
	require.NoError(t, workerKeepAlive())
	require.Equal(t, types.MachineStatusAvailable, status())
}
