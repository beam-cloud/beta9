package scheduler

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
	"github.com/tj/assert"
)

// routeFleet is one of each kind of pool the routing rule tells apart.
var routeFleet = map[string]types.WorkerPoolConfig{
	"cpu":        {},
	"cpu-first":  {Priority: 10},
	"cpu-shared": {Priority: 20, CPUShareMultiplier: 2.5},
	"cpu-named":  {RequiresPoolSelector: true},
	"build":      {RequiresPoolSelector: true},
	"gvisor":     {ContainerRuntime: types.ContainerRuntimeGvisor.String()},
	"microvm":    {ContainerRuntime: types.ContainerRuntimeMicroVM.String()},

	"a10g":         {GPUType: "A10G", Priority: 10},
	"a10g-reserve": {GPUType: "A10G", Priority: 5},
	"a10g-agent":   {GPUType: "A10G", Priority: 1, Mode: types.PoolModeExternal},
	"a10g-spot":    {GPUType: "A10G", Priority: 20, Preemptable: true},
	"a10g-partner": {GPUType: "A10G", Priority: 30, Mode: types.PoolModeProvider, RequiresPoolSelector: true},
	"t4":           {GPUType: "T4"},
	"rtx4090":      {GPUType: "RTX4090", Priority: 5, AlsoServes: []string{"A10G"}},
	"l40s":         {GPUType: "L40S", Priority: 3, RequiresPoolSelector: true, AlsoServes: []string{"A10G", "T4"}},
	"h100-spot":    {GPUType: "H100", Priority: 50, Preemptable: true, AlsoServes: []string{"a10g"}},
	"microvm-4090": {GPUType: "RTX4090", ContainerRuntime: types.ContainerRuntimeMicroVM.String()},
}

const routeFleetBuildPool = "build"

func routeTestPool(name string, config types.WorkerPoolConfig) *WorkerPool {
	return &WorkerPool{Name: name, Config: config, Controller: &LocalWorkerPoolControllerForTest{name: name}}
}

func routeTestPools(configs map[string]types.WorkerPoolConfig) []*WorkerPool {
	pools := make([]*WorkerPool, 0, len(configs))
	for name, config := range configs {
		pools = append(pools, routeTestPool(name, config))
	}
	return pools
}

// describeRoute lists a route's pools in order. A pool that only also serves
// the request is followed by its rank.
func describeRoute(r route) []string {
	names := []string{}
	for _, pool := range r.pools {
		name := pool.Name
		if pool.rank > 0 {
			name = fmt.Sprintf("%s (%d)", name, pool.rank)
		}
		names = append(names, name)
	}
	return names
}

// controllersFor returns the controllers of the pools a request may grow, in
// route order.
func controllersFor(s *Scheduler, request *types.ContainerRequest) ([]WorkerPoolController, error) {
	r, err := s.growthRouteFor(request)
	if err != nil {
		return nil, err
	}
	controllers := make([]WorkerPoolController, 0, len(r.pools))
	for _, pool := range r.pools {
		controllers = append(controllers, pool.Controller)
	}
	return controllers, nil
}

// poolsAccepting returns the names of the pools that accept the request, in
// route order. The pools are the test controllers' own, each with the given
// config.
func poolsAccepting(request *types.ContainerRequest, configs map[string]types.WorkerPoolConfig) []string {
	return describeRoute(newRoute(request, routeTestPools(configs), ""))
}

func TestRoute(t *testing.T) {
	gpu := func(gpus ...string) *types.ContainerRequest {
		return &types.ContainerRequest{GpuRequest: gpus, GpuCount: 1}
	}
	withStorage := func(request *types.ContainerRequest) *types.ContainerRequest {
		request.Workspace = testWorkspaceWithStorage()
		return request
	}
	preemptable := func(request *types.ContainerRequest) *types.ContainerRequest {
		request.Preemptable = true
		return request
	}
	named := func(pool string, request *types.ContainerRequest) *types.ContainerRequest {
		request.PoolSelector = pool
		return request
	}
	stub := func(stubType string, request *types.ContainerRequest) *types.ContainerRequest {
		request.Stub.Type = types.StubType(stubType)
		return request
	}
	tier := func(tier string) *types.ContainerRequest {
		request := &types.ContainerRequest{}
		request.Stub.Config = `{"cpu_tier":"` + tier + `"}`
		return request
	}
	dockerfile := "FROM python:3.12"

	tests := []struct {
		name    string
		request *types.ContainerRequest
		want    []string
	}{
		// CPU work runs on CPU pools that need no naming, by priority then
		// name. Work that does not share CPU overflows onto Shared pools
		// last, whatever their priority.
		{"cpu work", &types.ContainerRequest{}, []string{"cpu-first", "cpu", "gvisor", "cpu-shared"}},
		{"performance cpu work", tier("performance"), []string{"cpu-first", "cpu", "gvisor", "cpu-shared"}},
		{"shared cpu work", tier("shared"), []string{"cpu-shared"}},
		{"spot cpu work borrows from every hosted cpu pool", tier("spot"), []string{"cpu-shared", "cpu-first", "cpu", "gvisor"}},

		// A named pool is the whole route.
		{"named pool", named("cpu-named", &types.ContainerRequest{}), []string{"cpu-named"}},
		{"named pool that does not exist", named("retired", &types.ContainerRequest{}), []string{}},
		{"named gpu pool", named("l40s", gpu("L40S")), []string{"l40s"}},
		{"naming a pool does not make it accept the request", named("a10g-spot", gpu("A10G")), []string{}},

		// A GPU request runs on the pools of its type, then on the pools
		// that also serve it.
		{"gpu", gpu("A10G"), []string{"a10g", "a10g-reserve", "rtx4090 (1)", "l40s (2)"}},
		{"gpu with a fallback behind a selector", gpu("T4"), []string{"t4", "l40s (1)"}},
		{"alsoServes goes one way", gpu("RTX4090"), []string{"rtx4090"}},
		{"gpu whose only pool must be named", gpu("L40S"), []string{}},
		{"gpu no pool has or serves", gpu("B200"), []string{}},
		{"several gpu types use the fallbacks of each", gpu("H100", "T4", "A10G"),
			[]string{"a10g", "a10g-reserve", "t4", "rtx4090 (1)", "l40s (2)"}},
		{"gpu preference weighs against priority", gpu("RTX4090", "A10G"), []string{"a10g", "rtx4090", "a10g-reserve", "l40s (1)"}},

		// What the request brings decides which of those pools it may use.
		{"agent pools need workspace storage", withStorage(gpu("A10G")),
			[]string{"a10g", "a10g-reserve", "a10g-agent", "rtx4090 (1)", "l40s (2)"}},
		{"preemptable pools take work that accepts interruption", preemptable(gpu("A10G")),
			[]string{"a10g-spot", "a10g", "a10g-reserve", "h100-spot (1)", "rtx4090 (2)", "l40s (3)"}},
		{"only preemptable pool of its type", gpu("H100"), []string{}},
		{"partner machines run managed endpoints only", named("a10g-partner", withStorage(gpu("A10G"))), []string{}},
		{"managed endpoint on partner machines",
			stub(types.StubTypeManagedEndpointDeployment, named("a10g-partner", withStorage(gpu("A10G")))), []string{"a10g-partner"}},

		// The build pool is for builds.
		{"build pool refuses other work", named("build", &types.ContainerRequest{}), []string{}},
		{"build pool takes builds",
			named("build", &types.ContainerRequest{BuildOptions: types.BuildOptions{Dockerfile: &dockerfile}}), []string{"build"}},

		// Runtimes.
		{"docker needs a sandboxed runtime", &types.ContainerRequest{DockerEnabled: true}, []string{"gvisor"}},
		{"vm sandbox runs only on microvm",
			stub(types.StubTypeSandbox, &types.ContainerRequest{UseVM: true}), []string{"microvm"}},
		{"gpu vm sandbox", func() *types.ContainerRequest {
			request := stub(types.StubTypeSandbox, gpu("RTX4090"))
			request.UseVM = true
			return request
		}(), []string{"microvm-4090"}},
		{"vm without use_vm runs nowhere", stub(types.StubTypeVM, &types.ContainerRequest{}), []string{}},
		{"checkpoint restores on the runtime it was taken on", &types.ContainerRequest{Checkpoint: &types.Checkpoint{
			CheckpointId: "c1", Status: string(types.CheckpointStatusAvailable), Runtime: types.ContainerRuntimeGvisor.String(),
		}}, []string{"gvisor"}},
	}

	pools := routeTestPools(routeFleet)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidates := pools
			if test.request.PoolSelector != "" {
				// routeFor hands a request that names a pool only that pool.
				candidates = slices.DeleteFunc(slices.Clone(pools), func(pool *WorkerPool) bool {
					return pool.Name != test.request.PoolSelector
				})
			}
			got := newRoute(test.request, candidates, routeFleetBuildPool)
			require.Equal(t, test.want, describeRoute(got))

			for _, pool := range candidates {
				_, accepted := poolAccepts(pool, newRouteQuery(test.request, routeFleetBuildPool))
				require.Equal(t, accepted, got.considered[pool.Name] != notInRoute,
					"pool %s: the route and poolAccepts disagree", pool.Name)
			}
		})
	}
}

func TestRouteOrderDoesNotDependOnTheOrderPoolsAreListed(t *testing.T) {
	request := &types.ContainerRequest{Gpu: "A10G", GpuCount: 1}
	pools := routeTestPools(map[string]types.WorkerPoolConfig{
		"primary-b": {GPUType: "A10G"},
		"primary-a": {GPUType: "A10G"},
		"spare-c":   {GPUType: "L40S", AlsoServes: []string{"A10G"}},
		"spare-b":   {GPUType: "L40S", AlsoServes: []string{"A10G"}},
		"spare-a":   {GPUType: "H100", AlsoServes: []string{"A10G"}},
	})
	want := []string{"primary-a", "primary-b", "spare-a (1)", "spare-b (2)", "spare-c (3)"}

	for range 20 {
		// routeTestPools ranges over a map, so each call lists the pools in
		// a different order.
		require.Equal(t, want, describeRoute(newRoute(request, slices.Clone(pools), "")))
		pools = routeTestPools(map[string]types.WorkerPoolConfig{
			"primary-b": {GPUType: "A10G"},
			"primary-a": {GPUType: "A10G"},
			"spare-c":   {GPUType: "L40S", AlsoServes: []string{"A10G"}},
			"spare-b":   {GPUType: "L40S", AlsoServes: []string{"A10G"}},
			"spare-a":   {GPUType: "H100", AlsoServes: []string{"A10G"}},
		})
	}
}

func TestAnyGPURunsOnEveryGPUPoolThatNeedsNoNaming(t *testing.T) {
	request := &types.ContainerRequest{Gpu: string(types.GPU_ANY), GpuCount: 1}
	got := newRoute(request, routeTestPools(routeFleet), routeFleetBuildPool)

	names := describeRoute(got)
	slices.Sort(names)
	// No CPU pool, no pool behind a selector, nothing preemptable, and no
	// fallback rank: "any" asks for every type directly.
	require.Equal(t, []string{"a10g", "a10g-reserve", "rtx4090", "t4"}, names)
}

func TestCheckpointRestoresOnlyOnItsAccelerator(t *testing.T) {
	request := &types.ContainerRequest{
		GpuRequest: []string{"A10G", "RTX4090"},
		GpuCount:   1,
		Workspace:  testWorkspaceWithStorage(),
		Checkpoint: &types.Checkpoint{
			CheckpointId: "c1", Status: string(types.CheckpointStatusAvailable), Accelerator: "RTX4090",
		},
	}
	pools := routeTestPools(map[string]types.WorkerPoolConfig{
		"a10g":    {GPUType: "A10G"},
		"rtx4090": {GPUType: "RTX4090"},
		// An agent pool with no gpuType holds whatever its machines have,
		// so its workers are checked one by one.
		"mixed-agent": {Mode: types.PoolModePrivate},
	})

	require.Equal(t, []string{"rtx4090"}, describeRoute(newRoute(request, pools, "")))

	request.PoolSelector = "mixed-agent"
	require.Equal(t, []string{"mixed-agent"}, describeRoute(newRoute(request, pools, "")))
}

func TestRouteRankOfWorker(t *testing.T) {
	pools := routeTestPools(map[string]types.WorkerPoolConfig{
		"a10g":      {GPUType: "A10G"},
		"a10g-spot": {GPUType: "A10G", Preemptable: true},
		"rtx4090":   {GPUType: "RTX4090", AlsoServes: []string{"A10G"}},
		"l40s":      {GPUType: "L40S", RequiresPoolSelector: true, AlsoServes: []string{"A10G"}},
	})
	worker := func(pool string, requiresSelector bool) *types.Worker {
		return &types.Worker{PoolName: pool, RequiresPoolSelector: requiresSelector}
	}

	tests := []struct {
		name     string
		selector string
		worker   *types.Worker
		wantRank int
		wantOK   bool
	}{
		{name: "pool the request asked for", worker: worker("a10g", false), wantOK: true},
		{name: "pool that also serves it", worker: worker("rtx4090", false), wantRank: 2, wantOK: true},
		{name: "pool behind a selector that also serves it", worker: worker("l40s", true), wantRank: 1, wantOK: true},
		{name: "pool that does not accept it", worker: worker("a10g-spot", false)},
		// A pool this replica has not loaded is judged by its worker.
		{name: "unloaded pool", worker: worker("a10g-new", false), wantOK: true},
		{name: "unloaded pool behind a selector", worker: worker("someones-private-pool", true)},
		// A workspace's private pool can share a shared pool's name.
		{name: "private pool named like a shared pool", worker: worker("a10g", true)},
		{name: "private pool named like a pool that also serves it", worker: &types.Worker{PoolName: "rtx4090", RequiresPoolSelector: true, WorkspaceId: "workspace"}},
		{name: "managed machine of a pool that also serves it", worker: &types.Worker{PoolName: "rtx4090", WorkspaceId: "workspace", ControlPlaneManaged: true}, wantRank: 2, wantOK: true},
		{name: "named pool", selector: "a10g", worker: worker("a10g", false), wantOK: true},
		{name: "another pool than the one named", selector: "a10g", worker: worker("rtx4090", false)},
		{name: "named pool that is not loaded", selector: "private", worker: worker("private", true), wantOK: true},
		{name: "another pool than the unloaded one named", selector: "private", worker: worker("a10g", false)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := &types.ContainerRequest{Gpu: "A10G", GpuCount: 1, PoolSelector: test.selector}
			candidates := pools
			if test.selector != "" {
				candidates = slices.DeleteFunc(slices.Clone(pools), func(pool *WorkerPool) bool {
					return pool.Name != test.selector
				})
			}

			rank, ok := newRoute(request, candidates, "").rankOf(test.worker)
			require.Equal(t, test.wantOK, ok)
			require.Equal(t, test.wantRank, rank)
		})
	}
}

func TestRouteEligibleWorkers(t *testing.T) {
	pools := routeTestPools(map[string]types.WorkerPoolConfig{
		"a10g":    {GPUType: "A10G"},
		"agent":   {Mode: types.PoolModeExternal},
		"rtx4090": {GPUType: "RTX4090", AlsoServes: []string{"A10G"}},
		"t4":      {GPUType: "T4"},
		"custom":  {GPUType: "FPGA9000", RequiresPoolSelector: true},
	})
	worker := func(id, pool, gpu string) *types.Worker {
		return &types.Worker{Id: id, PoolName: pool, Gpu: gpu, RequiresPoolSelector: pools[slices.IndexFunc(pools, func(p *WorkerPool) bool {
			return p.Name == pool
		})].Config.RequiresPoolSelector}
	}
	workers := []*types.Worker{
		worker("a10g", "a10g", "A10G"),
		worker("agent-a10g", "agent", "A10G"),
		worker("agent-h100", "agent", "H100"),
		worker("rtx4090", "rtx4090", "RTX4090"),
		worker("t4", "t4", "T4"),
		worker("custom", "custom", "FPGA9000"),
	}
	ids := func(workers []*types.Worker) []string {
		ids := []string{}
		for _, worker := range workers {
			ids = append(ids, worker.Id)
		}
		return ids
	}
	filter := func(request *types.ContainerRequest) []string {
		candidates := pools
		if request.PoolSelector != "" {
			candidates = slices.DeleteFunc(slices.Clone(pools), func(pool *WorkerPool) bool {
				return pool.Name != request.PoolSelector
			})
		}
		request.Workspace = testWorkspaceWithStorage()
		return ids(newRoute(request, candidates, "").eligible(workers))
	}

	// A pool that also serves the request takes it on whatever GPU it has.
	// The agent pool has no gpuType, so it is in no GPU route.
	require.Equal(t, []string{"a10g", "rtx4090"}, filter(&types.ContainerRequest{Gpu: "A10G", GpuCount: 1}))
	require.Equal(t, []string{"rtx4090"}, filter(&types.ContainerRequest{Gpu: "RTX4090", GpuCount: 1}))

	// Inside a named pool, the worker still needs a GPU the request can use.
	require.Equal(t, []string{"agent-a10g"}, filter(&types.ContainerRequest{Gpu: "A10G", GpuCount: 1, PoolSelector: "agent"}))

	// "any" means any GPU in the catalog, or whatever a named pool has.
	require.Equal(t, []string{"a10g", "rtx4090", "t4"}, filter(&types.ContainerRequest{Gpu: string(types.GPU_ANY), GpuCount: 1}))
	require.Equal(t, []string{"custom"}, filter(&types.ContainerRequest{Gpu: string(types.GPU_ANY), GpuCount: 1, PoolSelector: "custom"}))

	// A machine pin has already chosen the worker; its GPU still has to fit.
	require.Equal(t, []string{"a10g", "agent-a10g", "rtx4090"},
		filter(&types.ContainerRequest{Gpu: "A10G", GpuCount: 1, MachineId: "machine-1"}))
}

func TestRequestNamingAPoolGetsItsWorkspacesOwnPoolFirst(t *testing.T) {
	manager := NewWorkerPoolManager()
	manager.SetPool("gpu", types.WorkerPoolConfig{GPUType: "A10G", Priority: 1}, &LocalWorkerPoolControllerForTest{name: "gpu"})
	manager.SetPoolAt(
		agentPoolControllerKey("workspace-1", &compute.PoolState{Selector: "gpu"}), "gpu",
		types.WorkerPoolConfig{Mode: types.PoolModePrivate, RequiresPoolSelector: true, Priority: 1000},
		&LocalWorkerPoolControllerForTest{name: "gpu"},
	)
	scheduler := &Scheduler{workerPoolManager: manager}
	request := func(workspace, selector string) *types.ContainerRequest {
		return &types.ContainerRequest{
			Gpu: "A10G", GpuCount: 1, WorkspaceId: workspace, PoolSelector: selector, Workspace: testWorkspaceWithStorage(),
		}
	}

	own := scheduler.routeFor(request("workspace-1", "gpu"))
	require.Len(t, own.pools, 1)
	require.Equal(t, types.PoolModePrivate, own.pools[0].Config.Mode)

	shared := scheduler.routeFor(request("workspace-2", "gpu"))
	require.Len(t, shared.pools, 1)
	require.Equal(t, int32(1), shared.pools[0].Config.Priority)

	// A request that names no pool is never routed to a private pool.
	unnamed := scheduler.routeFor(request("workspace-1", ""))
	require.Len(t, unnamed.pools, 1)
	require.Equal(t, int32(1), unnamed.pools[0].Config.Priority)
}

func TestHasManagedPoolForGPU(t *testing.T) {
	manager := NewWorkerPoolManager()
	for name, config := range map[string]types.WorkerPoolConfig{
		"beta9-t4":     {GPUType: "T4"},
		"private-h100": {GPUType: "H100", Mode: types.PoolModePrivate, RequiresPoolSelector: true},
		// Provider pools only host managed endpoint replicas, even when a
		// misconfigured one does not require a selector.
		"provider-a6000": {GPUType: "A6000", Mode: types.PoolModeProvider},
		"provider-spare": {GPUType: "L4", Mode: types.PoolModeProvider, AlsoServes: []string{"A16"}},
		"spare-l40s":     {GPUType: "L40S", RequiresPoolSelector: true, AlsoServes: []string{"A10G"}},
	} {
		manager.SetPool(name, config, &LocalWorkerPoolControllerForTest{name: name})
	}
	scheduler := &Scheduler{workerPoolManager: manager}

	// A pool with zero live workers still counts.
	require.True(t, scheduler.HasManagedPoolForGPU("T4", false))
	// A GPU type that no pool has, but one also serves, has somewhere to run.
	require.True(t, scheduler.HasManagedPoolForGPU("A10G", false))
	// Pools requiring a pool selector can't serve workloads that name none.
	require.False(t, scheduler.HasManagedPoolForGPU("H100", false))
	require.False(t, scheduler.HasManagedPoolForGPU("L40S", false))
	// Provider pools never count for ordinary workloads.
	require.False(t, scheduler.HasManagedPoolForGPU("A6000", false))
	require.False(t, scheduler.HasManagedPoolForGPU("A16", false))
	// GPU_ANY matches any GPU pool usable without a selector.
	require.True(t, scheduler.HasManagedPoolForGPU(string(types.GPU_ANY), false))
	// A GPU type nothing has or serves would never be placed.
	require.False(t, scheduler.HasManagedPoolForGPU("B200", false))
}

// The fail-fast check and scheduling must agree about every GPU type.
func TestHasManagedPoolForGPUAgreesWithTheRoute(t *testing.T) {
	manager := NewWorkerPoolManager()
	for name, config := range routeFleet {
		manager.SetPool(name, config, &LocalWorkerPoolControllerForTest{name: name})
	}
	scheduler := &Scheduler{workerPoolManager: manager}

	for _, gpu := range types.AllGPUTypes() {
		for _, preemptable := range []bool{true, false} {
			request := &types.ContainerRequest{
				Gpu: gpu.String(), GpuCount: 1, Preemptable: preemptable,
				// A request that brings everything else a pool can ask for.
				Workspace: testWorkspaceWithStorage(),
			}
			routed := len(scheduler.routeFor(request).pools) > 0
			require.Equal(t, routed, scheduler.HasManagedPoolForGPU(gpu.String(), preemptable), "%s, preemptable: %v", gpu, preemptable)
		}
	}
}

// overflowSchedulerForTest returns a scheduler whose A10G requests may
// overflow into the 4090 pool first and the L40S pool second.
func overflowSchedulerForTest(t *testing.T) *Scheduler {
	t.Helper()

	scheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)

	for name, config := range map[string]types.WorkerPoolConfig{
		"beta9-4090": {GPUType: "RTX4090", Priority: 2, AlsoServes: []string{"A10G"}},
		"beta9-l40s": {GPUType: "L40S", Priority: 1, AlsoServes: []string{"A10G"}},
		"beta9-h100": {GPUType: "H100", Priority: 9},
	} {
		scheduler.workerPoolManager.SetPool(name, config, &LocalWorkerPoolControllerForTest{
			ctx:        scheduler.ctx,
			name:       name,
			config:     scheduler.config,
			workerRepo: scheduler.workerRepo,
		})
	}
	return scheduler
}

func gpuWorkerForTest(id, poolName, gpu string, freeGpuCount uint32) *types.Worker {
	return &types.Worker{
		Id:           id,
		PoolName:     poolName,
		Gpu:          gpu,
		Status:       types.WorkerStatusAvailable,
		FreeCpu:      4000,
		FreeMemory:   8000,
		FreeGpuCount: freeGpuCount,
		Priority:     10,
	}
}

// TestSelectionFollowsTheRoute covers the placement rule: free capacity
// anywhere in the route schedules immediately, the pools of the requested GPU
// type win while they have room, and the pools that also serve it are taken in
// route order. Available overflow capacity beating pending capacity of the
// requested type falls out of the attempt order in reserve.go, which only
// looks at pending workers after no available worker fits.
func TestSelectionFollowsTheRoute(t *testing.T) {
	tests := []struct {
		name       string
		workers    []*types.Worker
		request    *types.ContainerRequest
		wantWorker string
	}{
		{
			name: "pool of the requested type wins while it has capacity",
			workers: []*types.Worker{
				gpuWorkerForTest("spare-4090", "beta9-4090", "RTX4090", 1),
				gpuWorkerForTest("a10g", "beta9-a10g", "A10G", 1),
			},
			request:    &types.ContainerRequest{Gpu: "A10G", GpuCount: 1, Cpu: 1000, Memory: 1000},
			wantWorker: "a10g",
		},
		{
			name: "pool that also serves the type takes the request when its own pool is full",
			workers: []*types.Worker{
				gpuWorkerForTest("a10g", "beta9-a10g", "A10G", 0),
				gpuWorkerForTest("spare-4090", "beta9-4090", "RTX4090", 1),
			},
			request:    &types.ContainerRequest{Gpu: "A10G", GpuCount: 1, Cpu: 1000, Memory: 1000},
			wantWorker: "spare-4090",
		},
		{
			name: "route order decides between pools that also serve the type",
			workers: []*types.Worker{
				gpuWorkerForTest("spare-l40s", "beta9-l40s", "L40S", 1),
				gpuWorkerForTest("spare-4090", "beta9-4090", "RTX4090", 1),
			},
			request:    &types.ContainerRequest{Gpu: "A10G", GpuCount: 1, Cpu: 1000, Memory: 1000},
			wantWorker: "spare-4090",
		},
		{
			name: "later pool takes the request when the earlier one is full",
			workers: []*types.Worker{
				gpuWorkerForTest("spare-4090", "beta9-4090", "RTX4090", 0),
				gpuWorkerForTest("spare-l40s", "beta9-l40s", "L40S", 1),
			},
			request:    &types.ContainerRequest{Gpu: "A10G", GpuCount: 1, Cpu: 1000, Memory: 1000},
			wantWorker: "spare-l40s",
		},
		{
			name: "gpu spelling in alsoServes is case-insensitive",
			workers: []*types.Worker{
				gpuWorkerForTest("spare-4090", "beta9-4090", "RTX4090", 1),
			},
			request:    &types.ContainerRequest{GpuRequest: []string{"A10G"}, GpuCount: 1, Cpu: 1000, Memory: 1000},
			wantWorker: "spare-4090",
		},
		{
			name: "capacity in a pool that does not serve the type is invisible",
			workers: []*types.Worker{
				gpuWorkerForTest("h100", "beta9-h100", "H100", 1),
			},
			request: &types.ContainerRequest{Gpu: "A10G", GpuCount: 1, Cpu: 1000, Memory: 1000},
		},
		{
			name: "alsoServes goes one way",
			workers: []*types.Worker{
				gpuWorkerForTest("a10g", "beta9-a10g", "A10G", 1),
			},
			request: &types.ContainerRequest{Gpu: "RTX4090", GpuCount: 1, Cpu: 1000, Memory: 1000},
		},
		{
			name: "any gpu takes no overflow rank",
			workers: []*types.Worker{
				gpuWorkerForTest("spare-4090", "beta9-4090", "RTX4090", 1),
				gpuWorkerForTest("h100", "beta9-h100", "H100", 1),
			},
			request: &types.ContainerRequest{Gpu: string(types.GPU_ANY), GpuCount: 1, Cpu: 1000, Memory: 1000},
			// Both pools are asked for directly, so the ordinary GPU
			// preference decides.
			wantWorker: "h100",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scheduler := overflowSchedulerForTest(t)

			worker, err := scheduler.selectWorkerFromWorkers(test.workers, test.request)
			if test.wantWorker == "" {
				assert.Error(t, err)
				return
			}
			assert.Nil(t, err)
			assert.Equal(t, test.wantWorker, worker.Id)
		})
	}
}

func TestReadyOverflowCapacityPrecedesPendingOrProvisionableCapacityOfTheRequestedType(t *testing.T) {
	scheduler := overflowSchedulerForTest(t)
	a10gController := &LocalWorkerPoolControllerForTest{
		ctx:        scheduler.ctx,
		name:       "beta9-a10g",
		config:     scheduler.config,
		workerRepo: scheduler.workerRepo,
	}
	scheduler.workerPoolManager.SetPool(
		"beta9-a10g",
		types.WorkerPoolConfig{GPUType: "A10G"},
		a10gController,
	)

	pendingA10G := gpuWorkerForTest("pending-a10g", "beta9-a10g", "A10G", 1)
	pendingA10G.Status = types.WorkerStatusPending
	ready4090 := gpuWorkerForTest("ready-4090", "beta9-4090", "RTX4090", 1)
	assert.NoError(t, scheduler.workerRepo.AddWorker(pendingA10G))
	assert.NoError(t, scheduler.workerRepo.AddWorker(ready4090))

	request := &types.ContainerRequest{
		Gpu:       "A10G",
		GpuCount:  1,
		Cpu:       1000,
		Memory:    1000,
		Timestamp: time.Now(),
	}
	setPendingSchedulerRequests(t, scheduler, request)
	scheduler.processRequestBatch(
		[]*types.ContainerRequest{request},
		[]*types.Worker{pendingA10G, ready4090},
	)

	queued, err := nextContainerRequest(scheduler.workerRepo, ready4090.Id)
	assert.NoError(t, err)
	assert.NotNil(t, queued)
	assert.Equal(t, request.ContainerId, queued.ContainerId)
	assert.Equal(t, 0, a10gController.AddWorkerCallCount())
}

func TestRouteRankOverridesWorkerPriority(t *testing.T) {
	scheduler := overflowSchedulerForTest(t)
	a10g := gpuWorkerForTest("a10g", "beta9-a10g", "A10G", 1)
	a10g.Priority = -100
	first := gpuWorkerForTest("spare-4090", "beta9-4090", "RTX4090", 1)
	first.Priority = -50
	second := gpuWorkerForTest("spare-l40s", "beta9-l40s", "L40S", 1)
	second.Priority = 100
	request := &types.ContainerRequest{Gpu: "A10G", GpuCount: 1, Cpu: 1000, Memory: 1000}

	worker, err := scheduler.selectWorkerFromWorkers([]*types.Worker{second, first, a10g}, request)
	assert.Nil(t, err)
	assert.Equal(t, a10g.Id, worker.Id)

	a10g.FreeGpuCount = 0
	worker, err = scheduler.selectWorkerFromWorkers([]*types.Worker{second, first, a10g}, request)
	assert.Nil(t, err)
	assert.Equal(t, first.Id, worker.Id)
}

// A pool that hosts one of the request's GPU types is asked for directly,
// even though it also serves another of them as overflow.
func TestPoolOfARequestedGPUTypeIsNotTreatedAsOverflow(t *testing.T) {
	scheduler := overflowSchedulerForTest(t)
	configured := gpuWorkerForTest("configured-4090", "beta9-4090", "RTX4090", 1)
	configured.Priority = 2
	unloaded := gpuWorkerForTest("unloaded-4090", "pool-this-replica-has-not-loaded", "RTX4090", 1)
	unloaded.Priority = -1

	request := &types.ContainerRequest{
		GpuRequest: []string{"RTX4090", "A10G"},
		GpuCount:   1,
		Cpu:        1000,
		Memory:     1000,
	}
	worker, err := scheduler.selectWorkerFromWorkers([]*types.Worker{unloaded, configured}, request)
	assert.Nil(t, err)
	assert.Equal(t, configured.Id, worker.Id)
}

func TestStorageBackedRequestPrefersManagedAgentCapacity(t *testing.T) {
	scheduler := overflowSchedulerForTest(t)
	agent := gpuWorkerForTest("managed-agent", "agent-4090", "RTX4090", 1)
	agent.ControlPlaneManaged = true
	agent.PoolSelector = "agent-4090"
	agent.Priority = 2
	legacy := gpuWorkerForTest("legacy-4090", "legacy-4090", "RTX4090", 1)
	legacy.Priority = 3

	request := &types.ContainerRequest{
		GpuRequest: []string{"RTX4090", "A10G"},
		GpuCount:   1,
		Cpu:        1000,
		Memory:     1000,
		Workspace:  testWorkspaceWithStorage(),
	}
	worker, err := scheduler.selectWorkerFromWorkers([]*types.Worker{legacy, agent}, request)
	assert.Nil(t, err)
	assert.Equal(t, agent.Id, worker.Id)

	request.Workspace = types.Workspace{}
	worker, err = scheduler.selectWorkerFromWorkers([]*types.Worker{legacy, agent}, request)
	assert.Nil(t, err)
	assert.Equal(t, legacy.Id, worker.Id)
}
