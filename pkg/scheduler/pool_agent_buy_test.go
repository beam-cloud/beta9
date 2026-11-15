package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/compute"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// fakeProvisioner sells machines of one size, or refuses to.
type fakeProvisioner struct {
	inStock bool
	err     error
	// sells is the size of every machine it sells.
	sells ProvisionedMachine
	// asked is what it was asked to hold, by CanProvision and Provision.
	asked []MachineNeed
	// sold are the machines it sold. They are booting until removed.
	sold []ProvisionedMachine
}

func (p *fakeProvisioner) CanProvision(ctx context.Context, pool string, need MachineNeed) bool {
	p.asked = append(p.asked, need)
	return p.inStock
}

func (p *fakeProvisioner) Provision(ctx context.Context, pool string, need MachineNeed) (*ProvisionedMachine, error) {
	p.asked = append(p.asked, need)
	if p.err != nil {
		return nil, p.err
	}
	machine := p.sells
	machine.MachineID = "machine-" + string(rune('a'+len(p.sold)))
	p.sold = append(p.sold, machine)
	return &machine, nil
}

func (p *fakeProvisioner) Booting(ctx context.Context, pool string) ([]ProvisionedMachine, error) {
	return p.sold, p.err
}

// buyingPool is the controller of a pool with a provisioning block, on its
// own Redis.
type buyingPool struct {
	t          *testing.T
	ctx        context.Context
	redis      *miniredis.Miniredis
	workers    repo.WorkerRepository
	machines   repo.ComputeRepository
	controller *AgentWorkerPoolController
}

const buyingWorkspace = "admin-workspace"

func newBuyingPool(t *testing.T, provisioner Provisioner, change ...func(*types.WorkerPoolConfig)) *buyingPool {
	t.Helper()
	server := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)

	config := types.WorkerPoolConfig{
		GPUType:          "L40S",
		ContainerRuntime: types.ContainerRuntimeRunc.String(),
		Priority:         5,
		Provisioning:     types.WorkerPoolProvisioningConfig{Provider: "verda", Spot: true, MaxNodes: 4},
	}
	for _, apply := range change {
		apply(&config)
	}
	config.ApplyProvisioning()

	p := &buyingPool{
		t: t, ctx: context.Background(), redis: server,
		workers:  repo.NewWorkerRedisRepositoryForTest(rdb),
		machines: repo.NewComputeRedisRepository(rdb),
	}
	p.controller, err = NewAgentWorkerPoolController(AgentWorkerPoolControllerOptions{
		Context: p.ctx, Name: "spot-l40s", WorkspaceID: buyingWorkspace, WorkerPool: config,
		Config:      types.AppConfig{Worker: types.WorkerConfig{ImageTag: "v1"}},
		PoolState:   &compute.PoolState{Name: "spot-l40s", Selector: "spot-l40s", Mode: string(types.PoolModeExternal)},
		WorkerRepo:  p.workers,
		ComputeRepo: p.machines,
		Provisioner: provisioner,
	})
	require.NoError(t, err)
	return p
}

func (p *buyingPool) poolWorkers() []*types.Worker {
	p.t.Helper()
	workers, err := p.workers.GetAllWorkersInPool("spot-l40s")
	require.NoError(p.t, err)
	return workers
}

// join is the machine's agent joining the pool and reporting what it has.
func (p *buyingPool) join(machineID string, cpuMillicores int64, memoryMB uint64) {
	p.t.Helper()
	require.NoError(p.t, p.machines.SaveAgentTokenState(p.ctx, &compute.AgentTokenState{
		TokenHash: "agent-token-" + machineID, WorkspaceID: buyingWorkspace, PoolName: "spot-l40s", MachineID: machineID,
		CPUMillicores: cpuMillicores, MemoryMB: memoryMB, GPUs: []string{"L40S"}, GPUCount: 1,
		Executor: types.DefaultAgentWorkerContainerMode, Schedulable: true, LastHeartbeatAt: time.Now(),
		Mode: string(types.PoolModeExternal), ManagedPoolInstanceID: "instance-1",
	}, time.Hour))
	require.NoError(p.t, p.controller.ensureMachine(machineID))
}

var oneL40S = ProvisionedMachine{CPU: 20_000, Memory: 54_000, GPUCount: 1}

func TestAPoolThatBuysAddsAWorkerByBuyingAMachine(t *testing.T) {
	provisioner := &fakeProvisioner{inStock: true, sells: oneL40S}
	p := newBuyingPool(t, provisioner)
	require.True(t, p.controller.IsPreemptable(), "spot machines can be taken back")

	can, err := p.controller.CanAddWorker(2000, 8192, 1)
	require.NoError(t, err)
	require.True(t, can)

	worker, err := p.controller.AddWorker(2000, 8192, 1)
	require.NoError(t, err)
	require.Equal(t, []MachineNeed{{CPU: 2000, Memory: 8192, GPUCount: 1}, {CPU: 2000, Memory: 8192, GPUCount: 1}}, provisioner.asked)

	// The worker stands for the whole machine, under the ID the machine's
	// own worker will have, and is not usable yet.
	require.Equal(t, compute.AgentMachineWorkerID("machine-a"), worker.Id)
	require.Equal(t, "machine-a", worker.MachineId)
	require.Equal(t, types.WorkerStatusPending, worker.Status)
	require.Equal(t, "spot-l40s", worker.PoolName)
	require.Equal(t, "L40S", worker.Gpu)
	require.EqualValues(t, 20_000, worker.TotalCpu)
	require.EqualValues(t, 54_000, worker.TotalMemory)
	require.EqualValues(t, 1, worker.TotalGpuCount)
	require.EqualValues(t, 5, worker.Priority)
	require.True(t, worker.Preemptable)
	require.True(t, worker.ControlPlaneManaged)
	require.Equal(t, buyingWorkspace, worker.WorkspaceId)

	stored := p.poolWorkers()
	require.Len(t, stored, 1)
	require.Equal(t, worker.Id, stored[0].Id)

	// Invariant 3: a booting machine counts as capacity, so the sizer and
	// waiting requests do not buy another.
	capacity, err := p.controller.FreeCapacity()
	require.NoError(t, err)
	require.EqualValues(t, 1, capacity.PendingGpu)
	require.Zero(t, capacity.FreeGpu)
}

func TestAPoolThatBuysAsksForPhysicalCores(t *testing.T) {
	provisioner := &fakeProvisioner{inStock: true, sells: oneL40S}
	p := newBuyingPool(t, provisioner, func(pool *types.WorkerPoolConfig) { pool.CPUShareMultiplier = 2 })

	// Four schedulable cores take two physical ones, and a machine with
	// twenty physical cores offers forty.
	worker, err := p.controller.AddWorker(4000, 8192, 1)
	require.NoError(t, err)
	require.Equal(t, []MachineNeed{{CPU: 2000, Memory: 8192, GPUCount: 1}}, provisioner.asked)
	require.EqualValues(t, 40_000, worker.TotalCpu)
}

func TestAPoolThatBuysCannotGrowWhenNothingCanBeBought(t *testing.T) {
	refused := errors.New("verda has no L40S")
	provisioner := &fakeProvisioner{inStock: false, err: refused}
	p := newBuyingPool(t, provisioner)

	can, err := p.controller.CanAddWorker(2000, 8192, 1)
	require.NoError(t, err)
	require.False(t, can)

	_, err = p.controller.AddWorker(2000, 8192, 1)
	require.ErrorIs(t, err, refused)
	require.Empty(t, p.poolWorkers(), "nothing was bought, so nothing stands for it")
}

func TestAPoolThatBuysGrowsNoOtherWay(t *testing.T) {
	provisioner := &fakeProvisioner{inStock: false, sells: oneL40S}
	p := newBuyingPool(t, provisioner)

	// A machine that has joined has its worker already. There is nothing
	// else to bind, so its free room is no way to add a worker.
	p.join("machine-by-hand", 20_000, 58_000)
	require.Len(t, p.poolWorkers(), 1)

	can, err := p.controller.CanAddWorker(2000, 8192, 1)
	require.NoError(t, err)
	require.False(t, can)
}

func TestAPoolThatBuysOnAGatewayWithNothingToBuyWith(t *testing.T) {
	p := newBuyingPool(t, nil)

	can, err := p.controller.CanAddWorker(2000, 8192, 1)
	require.NoError(t, err)
	require.False(t, can)
	_, err = p.controller.AddWorker(2000, 8192, 1)
	require.ErrorContains(t, err, `pool "spot-l40s" buys its machines, but this gateway has nothing to buy them with`)
	require.NoError(t, p.controller.reconcileMachines())
}

func TestTheWorkerOfABoughtMachineCarriesOverWhenTheMachineJoins(t *testing.T) {
	provisioner := &fakeProvisioner{inStock: true, sells: oneL40S}
	p := newBuyingPool(t, provisioner)
	bought, err := p.controller.AddWorker(2000, 8192, 1)
	require.NoError(t, err)

	// The machine reports a little more memory than was counted on.
	p.join("machine-a", 20_000, 58_500)

	workers := p.poolWorkers()
	require.Len(t, workers, 1, "the machine has one worker, not two")
	require.Equal(t, bought.Id, workers[0].Id)
	require.Equal(t, types.WorkerStatusPending, workers[0].Status)
	require.EqualValues(t, 58_500, workers[0].TotalMemory)
	require.True(t, workers[0].Preemptable)
	require.True(t, workers[0].ControlPlaneManaged)

	// Its worker starts and takes requests.
	require.NoError(t, p.workers.SetWorkerKeepAlive(bought.Id, types.WorkerKeepAlive{MachineId: "machine-a", RequestStreamOpen: true}))
	workers = p.poolWorkers()
	require.Len(t, workers, 1)
	require.Equal(t, types.WorkerStatusAvailable, workers[0].Status)
}

func TestBootingMachinesKeepTheirPendingWorker(t *testing.T) {
	provisioner := &fakeProvisioner{inStock: true, sells: oneL40S}
	p := newBuyingPool(t, provisioner)
	worker, err := p.controller.AddWorker(2000, 8192, 1)
	require.NoError(t, err)

	// A Pending worker expires after ten minutes. A machine can take longer
	// than that to boot; the pool's reconcile, every thirty seconds, keeps
	// its worker for as long as the machine is booting.
	for range 50 {
		p.redis.FastForward(30 * time.Second)
		require.NoError(t, p.controller.reconcileMachines())
	}
	require.Len(t, p.poolWorkers(), 1)

	// Redis loses the worker: it is made again, since the machine is still
	// coming.
	require.NoError(t, p.workers.RemoveWorker(worker.Id))
	require.Empty(t, p.poolWorkers())
	require.NoError(t, p.controller.reconcileMachines())
	workers := p.poolWorkers()
	require.Len(t, workers, 1)
	require.Equal(t, worker.Id, workers[0].Id)
	require.Equal(t, types.WorkerStatusPending, workers[0].Status)
	require.EqualValues(t, 54_000, workers[0].TotalMemory)

	// The machine is given up: its worker is no longer kept.
	provisioner.sold = nil
	for range 21 {
		p.redis.FastForward(30 * time.Second)
		require.NoError(t, p.controller.reconcileMachines())
	}
	require.Empty(t, p.poolWorkers())
}

func TestKeepingBootingWorkersLeavesJoinedMachinesToTheirAgent(t *testing.T) {
	provisioner := &fakeProvisioner{inStock: true, sells: oneL40S}
	p := newBuyingPool(t, provisioner)
	worker, err := p.controller.AddWorker(2000, 8192, 1)
	require.NoError(t, err)

	// The machine has joined and its worker is up, but the provisioner has
	// not seen that yet and still lists it as booting.
	p.join("machine-a", 20_000, 58_500)
	require.NoError(t, p.workers.SetWorkerKeepAlive(worker.Id, types.WorkerKeepAlive{MachineId: "machine-a", RequestStreamOpen: true}))
	require.NoError(t, p.controller.reconcileMachines())

	workers := p.poolWorkers()
	require.Len(t, workers, 1)
	require.Equal(t, types.WorkerStatusAvailable, workers[0].Status, "the worker is not set back to pending")
	require.EqualValues(t, 58_500, workers[0].TotalMemory, "nor to the size that was counted on")
}

func TestKeepBootingWorkersReportsAProvisionerItCannotAsk(t *testing.T) {
	provisioner := &fakeProvisioner{inStock: true, sells: oneL40S}
	p := newBuyingPool(t, provisioner)
	_, err := p.controller.AddWorker(2000, 8192, 1)
	require.NoError(t, err)

	provisioner.err = errors.New("redis is down")
	require.ErrorContains(t, p.controller.reconcileMachines(), "redis is down")
	require.Len(t, p.poolWorkers(), 1)
}

// A request asked for a small worker and the pool bought a machine. What is
// left of the machine is there for other requests to wait on.
func TestTakeCapacityOf(t *testing.T) {
	// The worker the request was going to get, with the request on it.
	asked := func() *types.Worker {
		return &types.Worker{
			TotalCpu: 2000, FreeCpu: 0, TotalMemory: 8192, FreeMemory: 0, TotalGpuCount: 1, FreeGpuCount: 0,
		}
	}

	tests := []struct {
		name        string
		placeholder *types.Worker
		added       *types.Worker
		want        types.Worker
	}{
		{
			name:        "the worker that was asked for",
			placeholder: asked(),
			added:       &types.Worker{TotalCpu: 2000, FreeCpu: 2000, TotalMemory: 8192, FreeMemory: 8192, TotalGpuCount: 1, FreeGpuCount: 1},
			want:        types.Worker{TotalCpu: 2000, TotalMemory: 8192, TotalGpuCount: 1},
		},
		{
			name:        "a machine with more of everything",
			placeholder: asked(),
			added:       &types.Worker{Gpu: "L40S", TotalCpu: 40_000, FreeCpu: 40_000, TotalMemory: 108_000, FreeMemory: 108_000, TotalGpuCount: 2, FreeGpuCount: 2},
			want:        types.Worker{Gpu: "L40S", TotalCpu: 40_000, FreeCpu: 38_000, TotalMemory: 108_000, FreeMemory: 99_808, TotalGpuCount: 2, FreeGpuCount: 1},
		},
		{
			name:        "a worker that came back smaller than asked for",
			placeholder: asked(),
			added:       &types.Worker{TotalCpu: 1000, FreeCpu: 1000, TotalMemory: 4096, FreeMemory: 4096},
			want:        types.Worker{TotalCpu: 1000, TotalMemory: 4096},
		},
		{
			name:        "the placeholder's GPU type stays when the worker names none",
			placeholder: &types.Worker{Gpu: "L40S", TotalCpu: 2000, TotalGpuCount: 1},
			added:       &types.Worker{TotalCpu: 20_000, FreeCpu: 20_000, TotalGpuCount: 1, FreeGpuCount: 1},
			want:        types.Worker{Gpu: "L40S", TotalCpu: 20_000, FreeCpu: 18_000, TotalGpuCount: 1},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			takeCapacityOf(test.placeholder, test.added)
			require.Equal(t, test.want, *test.placeholder)
		})
	}
}

func TestHoldsHeadroom(t *testing.T) {
	floor := func(gpus string) types.WorkerPoolConfig {
		return types.WorkerPoolConfig{GPUType: "L40S", PoolSizing: types.WorkerPoolJobSpecPoolSizingConfig{MinFreeGPU: gpus}}
	}
	ready := func(id string, freeGPUs uint32) *types.Worker {
		return &types.Worker{
			Id: id, Status: types.WorkerStatusAvailable, Gpu: "L40S",
			TotalCpu: 20_000, FreeCpu: 20_000, TotalMemory: 54_000, FreeMemory: 54_000,
			TotalGpuCount: 1, FreeGpuCount: freeGPUs,
		}
	}
	pending := ready("a-pending", 1)
	pending.Status = types.WorkerStatusPending
	workers := []*types.Worker{pending, ready("b", 1), ready("c", 0), ready("d", 1), ready("e", 1)}

	tests := []struct {
		name   string
		pool   types.WorkerPoolConfig
		worker *types.Worker
		want   bool
	}{
		{"the first ready worker holds a floor of one", floor("1"), workers[1], true},
		{"the next free one does not", floor("1"), workers[3], false},
		{"a busy worker ahead holds nothing, so the next one does", floor("2"), workers[3], true},
		{"the floor is met before the last", floor("2"), workers[4], false},
		{"a pending worker holds nothing", floor("1"), pending, false},
		{"no floor, nothing to hold", floor(""), workers[1], false},
		{"no worker", floor("1"), nil, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, HoldsHeadroom(test.pool, workers, test.worker))
		})
	}
}

// failingAdds is a worker repository that fails to write the next workers
// it is given.
type failingAdds struct {
	repo.WorkerRepository
	failures int
}

func (r *failingAdds) AddWorker(worker *types.Worker) error {
	if r.failures > 0 {
		r.failures--
		return errors.New("redis is down")
	}
	return r.WorkerRepository.AddWorker(worker)
}

// A machine that was bought is paid for, whatever becomes of the worker that
// stands for it. Reporting it as a worker that could not be added would have
// the scheduler ask the pool again, and the pool buy a second machine.
func TestAMachineWhoseWorkerCouldNotBeWrittenIsNotReportedAsAFailure(t *testing.T) {
	provisioner := &fakeProvisioner{inStock: true, sells: oneL40S}
	p := newBuyingPool(t, provisioner)
	p.controller.workerRepo = &failingAdds{WorkerRepository: p.workers, failures: 1}

	worker, err := p.controller.AddWorker(2000, 8192, 1)
	require.NoError(t, err)
	require.Equal(t, compute.AgentMachineWorkerID("machine-a"), worker.Id)
	require.Equal(t, types.WorkerStatusPending, worker.Status)
	require.Len(t, provisioner.sold, 1)
	require.Empty(t, p.poolWorkers(), "the worker was not written")

	// The machine is still booting, so the pool's reconcile makes its
	// worker again.
	require.NoError(t, p.controller.reconcileMachines())
	workers := p.poolWorkers()
	require.Len(t, workers, 1)
	require.Equal(t, worker.Id, workers[0].Id)
	require.Equal(t, types.WorkerStatusPending, workers[0].Status)
	require.Len(t, provisioner.sold, 1, "nothing more was bought")
}
