package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	vmetrics "github.com/VictoriaMetrics/metrics"
	"github.com/beam-cloud/beta9/pkg/clock"
	"github.com/beam-cloud/beta9/pkg/common"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestPoolMetricsEventAggregatesAvailableWorkers(t *testing.T) {
	workers := []*types.Worker{
		{Id: "worker-1", MachineId: "machine-1", Status: types.WorkerStatusAvailable, TotalCpu: 2000, FreeCpu: 1000, TotalMemory: 4000, FreeMemory: 1000, TotalGpuCount: 2, FreeGpuCount: 1},
		{Id: "worker-2", MachineId: "machine-1", Status: types.WorkerStatusAvailable, TotalCpu: 1000, TotalMemory: 2000, TotalGpuCount: 1},
		{Id: "worker-3", MachineId: "machine-2", Status: types.WorkerStatusAvailable, TotalCpu: 1000, FreeCpu: 1000, TotalMemory: 2000, FreeMemory: 2000, TotalGpuCount: 1, FreeGpuCount: 1},
		{Id: "pending", MachineId: "machine-3", Status: types.WorkerStatusPending, TotalCpu: 8000, TotalMemory: 16_000, TotalGpuCount: 8},
	}
	event := poolMetricsEvent("gpu-pool", types.WorkerPoolConfig{Mode: types.PoolModeExternal, HourlyCostCents: 120}, &types.WorkerPoolState{
		Status: types.WorkerPoolStatusHealthy, RunningContainers: 5, AvailableWorkers: 3, PendingWorkers: 1, ReadyMachines: 2, PendingMachines: 1,
	}, workers)

	if event.Action != types.EventComputeActionPoolHeartbeat || event.MachineCount != 3 || event.CPUCount != 4 || event.MemoryMB != 8000 || event.GPUCount != 4 {
		t.Fatalf("unexpected capacity: %+v", event)
	}
	wantAttrs := map[string]string{
		"container_count": "5", "free_gpu_count": "2", "cpu_utilization_pct": "50.00",
		"memory_used_mb": "5000", "memory_utilization_pct": "62.50", "hourly_cost_cents": "360",
	}
	for key, want := range wantAttrs {
		if got := event.Attrs[key]; got != want {
			t.Fatalf("attribute %q = %q, want %q", key, got, want)
		}
	}
}

func TestPoolMetricsEventUsesAgentInventoryForCost(t *testing.T) {
	event := poolMetricsEvent(
		"managed-gpu-pool",
		types.WorkerPoolConfig{Mode: types.PoolModeExternal, HourlyCostCents: 60},
		&types.WorkerPoolState{RegisteredMachines: 2, ReadyMachines: 1, PendingMachines: 1},
		[]*types.Worker{{
			Id: "ready-worker", MachineId: "ready-machine", Status: types.WorkerStatusAvailable,
		}},
	)
	if event.MachineCount != 2 {
		t.Fatalf("machine count = %d, want 2 registered agent machines", event.MachineCount)
	}
	if got := event.Attrs[types.EventComputeAttrHourlyCostCents]; got != "120" {
		t.Fatalf("hourly cost = %q, want 120 cents", got)
	}
}

// Every poolHealthCheckInterval a pool's monitor measures the pool, saves
// what it found for the gateway's other parts to read, and sends a heartbeat
// to the compute event stream.

// measuredPool is the test controller with the free capacity of its workers,
// as the real controllers report it, and the machines an agent pool counts.
type measuredPool struct {
	*LocalWorkerPoolControllerForTest
	machines    *types.WorkerPoolState
	machinesErr error
	capacityErr error
}

func (p *measuredPool) FreeCapacity() (*WorkerPoolCapacity, error) {
	if p.capacityErr != nil {
		return nil, p.capacityErr
	}
	return freePoolCapacity(p.workerRepo, p.name)
}

func (p *measuredPool) State() (*types.WorkerPoolState, error) {
	return p.machines, p.machinesErr
}

// poolHealthFixture is a GPU pool of two workers, and its monitor. One
// worker is up and has a container that runs, one that is starting and one
// that is stopping; the other worker is still starting.
type poolHealthFixture struct {
	rdb        *common.RedisClient
	monitor    *PoolHealthMonitor
	pool       *measuredPool
	workers    *failingWorkerRepo
	heartbeats []types.EventComputeSchema
	containers []string
}

const poolHealthPool = "gpu"

// newPoolHealthFixture is the fixture for a Kubernetes pool.
func newPoolHealthFixture(t *testing.T) *poolHealthFixture {
	return newPoolHealthFixtureFor(t, types.WorkerPoolConfig{Mode: types.PoolModeLocal, GPUType: "A10G", HourlyCostCents: 120})
}

func newPoolHealthFixtureFor(t *testing.T, config types.WorkerPoolConfig) *poolHealthFixture {
	t.Helper()
	rdb, err := repo.NewRedisClientForTest()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	workers := &failingWorkerRepo{WorkerRepository: repo.NewWorkerRedisRepositoryForTest(rdb)}
	containers := repo.NewContainerRedisRepositoryForTest(rdb)
	now := time.Unix(1_800_000_000, 0)
	f := &poolHealthFixture{rdb: rdb, workers: workers}

	for _, worker := range []*types.Worker{
		{Id: "up", MachineId: "node-1", Status: types.WorkerStatusAvailable, PoolName: poolHealthPool, Gpu: "A10G",
			TotalCpu: 8000, FreeCpu: 8000, TotalMemory: 16000, FreeMemory: 16000, TotalGpuCount: 4, FreeGpuCount: 4},
		{Id: "starting", Status: types.WorkerStatusPending, PoolName: poolHealthPool, Gpu: "A10G",
			TotalCpu: 8000, FreeCpu: 8000, TotalMemory: 16000, FreeMemory: 16000, TotalGpuCount: 4, FreeGpuCount: 4},
	} {
		require.NoError(t, workers.AddWorker(worker))
	}
	for _, container := range []types.ContainerState{
		// It took four seconds to start.
		{ContainerId: "container-running", Status: types.ContainerStatusRunning, ScheduledAt: now.Unix() - 60, StartedAt: now.Unix() - 56},
		// It has been starting for two.
		{ContainerId: "container-starting", Status: types.ContainerStatusPending, ScheduledAt: now.Unix() - 2},
		// It took six to start, and holds what it has until it is gone.
		{ContainerId: "container-stopping", Status: types.ContainerStatusStopping, ScheduledAt: now.Unix() - 90, StartedAt: now.Unix() - 84},
	} {
		container.WorkerId, container.StubId, container.WorkspaceId = "up", "stub", "workspace"
		container.Cpu, container.Memory, container.Gpu, container.GpuCount = 1000, 2000, "A10G", 1
		require.NoError(t, containers.SetContainerState(container.ContainerId, &container))
		// The worker records the start and lists the container as its own.
		state := common.RedisKeys.SchedulerContainerState(container.ContainerId)
		require.NoError(t, rdb.HSet(context.Background(), state, "started_at", container.StartedAt).Err())
		require.NoError(t, rdb.SAdd(context.Background(), common.RedisKeys.SchedulerContainerWorkerIndex(container.WorkerId), state).Err())
		f.containers = append(f.containers, container.ContainerId)
	}
	// What the containers hold comes off the worker's free capacity.
	up, err := workers.GetWorkerById("up")
	require.NoError(t, err)
	require.NoError(t, workers.AddWorker(up))

	f.pool = &measuredPool{LocalWorkerPoolControllerForTest: &LocalWorkerPoolControllerForTest{
		ctx: context.Background(), name: poolHealthPool, workerRepo: workers,
	}}
	f.monitor = NewPoolHealthMonitor(PoolHealthMonitorOptions{
		Controller:       f.pool,
		WorkerPoolConfig: config,
		WorkerRepo:       workers,
		WorkerPoolRepo:   repo.NewWorkerPoolRedisRepositoryForTest(rdb),
		ContainerRepo:    containers,
		PushMetrics:      func(event types.EventComputeSchema) { f.heartbeats = append(f.heartbeats, event) },
		Clock:            clock.NewFake(now),
	})
	return f
}

// The monitor once kept a histogram for each container it saw, named for the
// container, and added to it on every tick: the gateway held one for every
// container there had ever been.
func TestThePoolHealthTickKeepsNoMetricPerContainer(t *testing.T) {
	f := newPoolHealthFixture(t)

	f.monitor.tick()
	f.monitor.tick()

	require.Len(t, f.heartbeats, 2, "the pool was measured")
	for _, name := range vmetrics.GetDefaultSet().ListMetricNames() {
		for _, container := range f.containers {
			require.NotContains(t, name, container)
		}
	}
}

// published returns the pool's state as it is stored for the rest of the
// gateway to read.
func (f *poolHealthFixture) published(t *testing.T) map[string]string {
	t.Helper()
	state, err := f.rdb.HGetAll(context.Background(), common.RedisKeys.WorkerPoolState(poolHealthPool)).Result()
	require.NoError(t, err)
	return state
}

// What the monitor stores and sends is read by other parts of the gateway
// and by the dashboard, of this release and the one before it: every field,
// under its name, with these values.
func TestThePoolHealthTickStoresThePoolsStateAndSendsItsHeartbeat(t *testing.T) {
	f := newPoolHealthFixture(t)

	f.monitor.tick()

	require.Equal(t, map[string]string{
		"status":            "HEALTHY",
		"available_workers": "1", "pending_workers": "1",
		"running_containers": "1", "pending_containers": "1",
		// Two containers took four and six seconds to start, and the third
		// has been starting for two.
		"scheduling_latency": "4000",
		// What the worker that is up has left. The one that is starting has
		// nothing to place work on yet.
		"free_cpu": "5000", "free_memory": "8500", "free_gpu": "1",
		"registered_machines": "0", "pending_machines": "0", "ready_machines": "0",
	}, f.published(t))
	require.Equal(t, []types.EventComputeSchema{{
		PoolName:     poolHealthPool,
		Action:       types.EventComputeActionPoolHeartbeat,
		Status:       "HEALTHY",
		CPUCount:     8,
		MemoryMB:     16000,
		GPUCount:     4,
		MachineCount: 1,
		Attrs: map[string]string{
			"container_count": "1", "pending_container_count": "1",
			"worker_count": "2", "available_worker_count": "1", "pending_worker_count": "1",
			"free_gpu_count": "1", "cpu_utilization_pct": "37.50",
			"memory_used_mb": "7500", "memory_utilization_pct": "46.88",
			"hourly_cost_cents": "120", "scheduling_latency_ms": "4000", "pool_mode": "local",
		},
	}}, f.heartbeats)
}

// A pool of machines that join through an agent also reports how many
// machines it has. Its controller counts them.
func TestThePoolHealthTickReportsTheMachinesOfAnAgentPool(t *testing.T) {
	f := newPoolHealthFixtureFor(t, types.WorkerPoolConfig{Mode: types.PoolModeExternal, GPUType: "A10G", HourlyCostCents: 120})
	// The controller calls a pool with no machine ready degraded. What is
	// stored and sent is that a pool which can be measured is healthy.
	f.pool.machines = &types.WorkerPoolState{Status: types.WorkerPoolStatusDegraded, RegisteredMachines: 3, PendingMachines: 2, ReadyMachines: 1}

	f.monitor.tick()

	published := f.published(t)
	require.Equal(t, "HEALTHY", published["status"])
	require.Equal(t, []string{"3", "2", "1"}, []string{published["registered_machines"], published["pending_machines"], published["ready_machines"]})
	require.Len(t, f.heartbeats, 1)
	heartbeat := f.heartbeats[0]
	require.Equal(t, "HEALTHY", heartbeat.Status)
	require.EqualValues(t, 3, heartbeat.MachineCount, "every machine that has joined is paid for")
	require.Equal(t, "360", heartbeat.Attrs[types.EventComputeAttrHourlyCostCents])
	require.Equal(t, "external", heartbeat.Attrs[types.EventComputeAttrPoolMode])
}

// A pool that cannot be measured keeps the state it had, and sends no
// heartbeat.
func TestThePoolHealthTickPublishesNothingWhenThePoolCannotBeMeasured(t *testing.T) {
	down := errors.New("redis is down")
	for name, breakIt := range map[string]func(*poolHealthFixture){
		"its workers cannot be listed":   func(f *poolHealthFixture) { f.workers.listPool = down },
		"its machines cannot be counted": func(f *poolHealthFixture) { f.pool.machinesErr = down },
		"its free capacity is unknown":   func(f *poolHealthFixture) { f.pool.capacityErr = down },
	} {
		t.Run(name, func(t *testing.T) {
			f := newPoolHealthFixtureFor(t, types.WorkerPoolConfig{Mode: types.PoolModeExternal})
			f.pool.machines = &types.WorkerPoolState{}
			f.monitor.tick()
			before := f.published(t)
			require.NotEmpty(t, before)
			f.heartbeats = nil
			breakIt(f)
			logged := captureLog(t)

			f.monitor.tick()

			require.Equal(t, before, f.published(t))
			require.Empty(t, f.heartbeats)
			require.Contains(t, logged.String(), "redis is down")
		})
	}
}

// Every scheduler replica runs the health tick for a pool. One reports the
// pool's state at a time: a replica that finds another at it leaves the pass
// to that one, which is no failure.

func TestThePoolHealthTickLeavesThePassToTheReplicaThatIsAtIt(t *testing.T) {
	f := newPoolHealthFixture(t)
	other := repo.NewWorkerPoolRedisRepositoryForTest(f.rdb)
	logged := captureLog(t)

	require.NoError(t, other.WithWorkerPoolStateLock(poolHealthPool, f.monitor.tick))
	require.Empty(t, f.heartbeats)
	require.Empty(t, logged.String())

	f.monitor.tick()
	require.Len(t, f.heartbeats, 1)
}

func TestThePoolHealthTickReportsALockItCouldNotAskFor(t *testing.T) {
	f := newPoolHealthFixture(t)
	// The lock's key holds something that is not a lock.
	require.NoError(t, f.rdb.HSet(context.Background(), common.RedisKeys.WorkerPoolStateLock(poolHealthPool), "not", "a lock").Err())
	logged := captureLog(t)

	f.monitor.tick()

	require.Empty(t, f.heartbeats)
	require.Contains(t, logged.String(), `"level":"error"`)
	require.Contains(t, logged.String(), `"pool_name":"gpu"`)
}
