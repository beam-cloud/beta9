package scheduler

import (
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/tj/assert"
)

// failoverSchedulerForTest returns a scheduler whose A10G requests may fail
// over to the 4090 pool first and the L40S pool second.
func failoverSchedulerForTest(t *testing.T, onDemand *types.FailoverOnDemandStep) *Scheduler {
	t.Helper()

	scheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)

	for name, gpu := range map[string]string{"beta9-4090": "RTX4090", "beta9-l40s": "L40S"} {
		scheduler.workerPoolManager.SetPool(name, types.WorkerPoolConfig{GPUType: gpu}, &LocalWorkerPoolControllerForTest{
			ctx:        scheduler.ctx,
			name:       name,
			config:     scheduler.config,
			workerRepo: scheduler.workerRepo,
		})
	}

	scheduler.config.Scheduling.Failover = types.FailoverConfig{
		Enabled: true,
		Chains: map[string]types.FailoverChain{
			"A10G": {Pools: []string{"beta9-4090", "beta9-l40s"}, OnDemand: onDemand},
		},
	}
	return scheduler
}

func failoverWorker(id, poolName, gpu string, freeGpuCount uint32) *types.Worker {
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

// TestFailoverChainBinding covers which requests may fail over at all. Binding
// is deliberately narrow: widening the pool set for a request that has its own
// fallback, or that already sees every pool, would only add ambiguity.
func TestFailoverChainBinding(t *testing.T) {
	tests := []struct {
		name      string
		disabled  bool
		chains    map[string]types.FailoverChain
		request   *types.ContainerRequest
		wantPools []string
	}{
		{
			name:    "gpu request binds its chain",
			request: &types.ContainerRequest{Gpu: "A10G", GpuCount: 1},
			// Configured but non-existent pools are skipped rather than
			// failing the request.
			wantPools: []string{"beta9-4090", "beta9-l40s"},
		},
		{
			name:      "gpu spelling is case-insensitive",
			request:   &types.ContainerRequest{Gpu: "a10g", GpuCount: 1},
			wantPools: []string{"beta9-4090", "beta9-l40s"},
		},
		{
			name:    "first requested gpu with a chain wins",
			request: &types.ContainerRequest{GpuRequest: []string{"H100", "A10G"}, GpuCount: 1},
			// H100 has no chain, so binding continues to the next preference.
			wantPools: []string{"beta9-4090", "beta9-l40s"},
		},
		{
			name:     "disabled failover never binds",
			disabled: true,
			request:  &types.ContainerRequest{Gpu: "A10G", GpuCount: 1},
		},
		{
			name:    "unchained gpu never binds",
			request: &types.ContainerRequest{Gpu: "T4", GpuCount: 1},
		},
		{
			name:    "cpu request never binds",
			request: &types.ContainerRequest{Cpu: 1000, Memory: 1000},
		},
		{
			name:    "any-gpu request never binds",
			request: &types.ContainerRequest{Gpu: string(types.GPU_ANY), GpuCount: 1},
		},
		{
			name:    "selector-bound request never binds",
			request: &types.ContainerRequest{Gpu: "A10G", GpuCount: 1, PoolSelector: "private-pool"},
		},
		{
			name:    "chain of unknown pools without on-demand never binds",
			chains:  map[string]types.FailoverChain{"A10G": {Pools: []string{"retired-pool"}}},
			request: &types.ContainerRequest{Gpu: "A10G", GpuCount: 1},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scheduler := failoverSchedulerForTest(t, nil)
			if test.disabled {
				scheduler.config.Scheduling.Failover.Enabled = false
			}
			if test.chains != nil {
				scheduler.config.Scheduling.Failover.Chains = test.chains
			}

			chain := scheduler.failoverChainFor(test.request)
			if test.wantPools == nil {
				assert.Nil(t, chain)
				return
			}
			assert.NotNil(t, chain)
			assert.Equal(t, test.wantPools, chain.pools)
		})
	}
}

// TestFailoverSelectionPreference covers the placement rule: free capacity
// anywhere in the primary pools plus the chain schedules immediately, primary
// pools win while they have room, and the chain is walked in order. Available
// failover capacity beating pending primary capacity falls out of the attempt
// order in reserve.go, which only looks at pending workers after no available
// worker fits.
func TestFailoverSelectionPreference(t *testing.T) {
	tests := []struct {
		name       string
		disabled   bool
		workers    []*types.Worker
		request    *types.ContainerRequest
		wantWorker string
	}{
		{
			name: "primary pool wins while it has capacity",
			workers: []*types.Worker{
				failoverWorker("failover-4090", "beta9-4090", "RTX4090", 1),
				failoverWorker("primary-a10g", "beta9-a10g", "A10G", 1),
			},
			request:    &types.ContainerRequest{Gpu: "A10G", GpuCount: 1, Cpu: 1000, Memory: 1000},
			wantWorker: "primary-a10g",
		},
		{
			name: "failover pool takes the request when the primary pool is full",
			workers: []*types.Worker{
				failoverWorker("primary-a10g", "beta9-a10g", "A10G", 0),
				failoverWorker("failover-4090", "beta9-4090", "RTX4090", 1),
			},
			request:    &types.ContainerRequest{Gpu: "A10G", GpuCount: 1, Cpu: 1000, Memory: 1000},
			wantWorker: "failover-4090",
		},
		{
			name: "chain order decides between failover pools",
			workers: []*types.Worker{
				failoverWorker("failover-l40s", "beta9-l40s", "L40S", 1),
				failoverWorker("failover-4090", "beta9-4090", "RTX4090", 1),
			},
			request:    &types.ContainerRequest{Gpu: "A10G", GpuCount: 1, Cpu: 1000, Memory: 1000},
			wantWorker: "failover-4090",
		},
		{
			name: "later chain pool takes the request when the earlier one is full",
			workers: []*types.Worker{
				failoverWorker("failover-4090", "beta9-4090", "RTX4090", 0),
				failoverWorker("failover-l40s", "beta9-l40s", "L40S", 1),
			},
			request:    &types.ContainerRequest{Gpu: "A10G", GpuCount: 1, Cpu: 1000, Memory: 1000},
			wantWorker: "failover-l40s",
		},
		{
			name:     "failover capacity is invisible without a chain",
			disabled: true,
			workers: []*types.Worker{
				failoverWorker("failover-4090", "beta9-4090", "RTX4090", 1),
			},
			request: &types.ContainerRequest{Gpu: "A10G", GpuCount: 1, Cpu: 1000, Memory: 1000},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scheduler := failoverSchedulerForTest(t, nil)
			if test.disabled {
				scheduler.config.Scheduling.Failover.Enabled = false
			}

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

// TestFailoverDemandRecordedOnEstateExhaustion covers the only trigger for
// on-demand hardware: every pool the request could provision into, primary and
// failover alike, has recently refused for capacity.
func TestFailoverDemandRecordedOnEstateExhaustion(t *testing.T) {
	tests := []struct {
		name        string
		onDemand    *types.FailoverOnDemandStep
		exhausted   bool
		wantRecords int
	}{
		{
			name:        "exhausted estate records demand",
			onDemand:    &types.FailoverOnDemandStep{MaxNodes: 2},
			exhausted:   true,
			wantRecords: 1,
		},
		{
			name:      "chain without an on-demand step records nothing",
			exhausted: true,
		},
		{
			name:     "a pool that can still provision records nothing",
			onDemand: &types.FailoverOnDemandStep{MaxNodes: 2},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scheduler := failoverSchedulerForTest(t, test.onDemand)
			request := &types.ContainerRequest{
				ContainerId: "container-1",
				Gpu:         "A10G",
				GpuCount:    1,
				Cpu:         1000,
				Memory:      1000,
				Timestamp:   time.Now(),
			}
			setPendingSchedulerRequests(t, scheduler, request)

			controllers, err := scheduler.getControllers(request)
			assert.Nil(t, err)
			assert.Contains(t, controllerNames(controllers), "beta9-4090")

			if test.exhausted {
				for _, controller := range controllers {
					scheduler.recordWorkerProvisioningFailure(controller, &AgentPoolCapacityError{PoolName: controller.Name()})
				}
			}

			newSchedulingAttempt(scheduler, request, nil).provisionWorker()

			records, err := scheduler.computeRepo.ListFailoverDemand(scheduler.ctx)
			assert.Nil(t, err)
			assert.Len(t, records, test.wantRecords)
			if test.wantRecords > 0 {
				assert.Equal(t, "A10G", records[0].GPU)
				assert.Contains(t, records[0].Pools, "beta9-4090")
			}
		})
	}
}
