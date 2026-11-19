package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddWorkerIfNeeded(t *testing.T) {
	s, err := miniredis.Run()
	assert.NotNil(t, s)
	assert.Nil(t, err)
	t.Cleanup(s.Close)

	redisClient, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{s.Addr()}, Mode: types.RedisModeSingle})
	assert.NotNil(t, redisClient)
	assert.Nil(t, err)

	workerRepo := repo.NewWorkerRedisRepositoryForTest(redisClient)
	workerPoolRepo := repo.NewWorkerPoolRedisRepositoryForTest(redisClient)
	config := &types.WorkerPoolSizingConfig{
		MinFreeCpu:           10000,
		MinFreeMemory:        5000,
		MinFreeGpu:           0,
		DefaultWorkerCpu:     2000,
		DefaultWorkerMemory:  500,
		DefaultWorkerGpuType: "",
	}

	configWithGpu := &types.WorkerPoolSizingConfig{
		MinFreeCpu:           10000,
		MinFreeMemory:        5000,
		MinFreeGpu:           2,
		DefaultWorkerCpu:     2000,
		DefaultWorkerMemory:  500,
		DefaultWorkerGpuType: "A10G",
	}

	tests := []struct {
		name            string
		config          *types.WorkerPoolSizingConfig
		freeCapacity    *WorkerPoolCapacity
		shouldAddWorker bool
	}{
		{
			name:   "should not add worker when capacity is sufficient",
			config: config,
			freeCapacity: &WorkerPoolCapacity{
				FreeCpu:    10000,
				FreeMemory: 5000,
				FreeGpu:    0,
			},
			shouldAddWorker: false,
		},
		{
			name:   "should not add worker when pending capacity is sufficient",
			config: config,
			freeCapacity: &WorkerPoolCapacity{
				PendingCpu:    10000,
				PendingMemory: 5000,
				PendingGpu:    0,
			},
			shouldAddWorker: false,
		},
		{
			name:   "should add worker when capacity is not sufficient",
			config: config,
			freeCapacity: &WorkerPoolCapacity{
				FreeCpu:    5000,
				FreeMemory: 5000,
				FreeGpu:    0,
			},
			shouldAddWorker: true,
		},
		{
			name:   "should add worker (w/ GPU) when free CPU is not sufficient",
			config: configWithGpu,
			freeCapacity: &WorkerPoolCapacity{
				FreeCpu:    5000,
				FreeMemory: 1000,
				FreeGpu:    1,
			},
			shouldAddWorker: true,
		},
		{
			name:   "should add worker (w/ GPU) when free + pending CPU is not sufficient",
			config: configWithGpu,
			freeCapacity: &WorkerPoolCapacity{
				PendingCpu:    2500,
				PendingMemory: 500,
				PendingGpu:    1,
				FreeCpu:       2500,
				FreeMemory:    500,
				FreeGpu:       1,
			},
			shouldAddWorker: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller := &LocalWorkerPoolControllerForTest{
				name:       "TestPool",
				workerRepo: workerRepo,
			}
			sizer := &WorkerPoolSizer{
				controller:     controller,
				sizing:         tt.config,
				workerPoolRepo: workerPoolRepo,
			}

			newWorker, err := sizer.addWorkerIfNeeded(tt.freeCapacity)
			assert.NoError(t, err)

			if tt.shouldAddWorker {
				assert.NotNil(t, newWorker, "New worker should be added")
			} else {
				assert.Nil(t, newWorker, "New worker should not be added")
			}
		})
	}
}

func TestReadPoolSizing(t *testing.T) {
	defaults := func(change func(*types.WorkerPoolSizingConfig)) *types.WorkerPoolSizingConfig {
		sizing := &types.WorkerPoolSizingConfig{DefaultWorkerCpu: 1000, DefaultWorkerMemory: 1024}
		if change != nil {
			change(sizing)
		}
		return sizing
	}

	for _, test := range []struct {
		name    string
		pool    types.WorkerPoolConfig
		want    *types.WorkerPoolSizingConfig
		ignored []string
	}{
		{name: "nothing written", want: defaults(nil)},
		{
			name: "a floor",
			pool: types.WorkerPoolConfig{PoolSizing: types.WorkerPoolJobSpecPoolSizingConfig{
				MinFreeCPU: "132000m", MinFreeMemory: "100Gi", MinFreeGPU: "0",
			}},
			want: defaults(func(s *types.WorkerPoolSizingConfig) { s.MinFreeCpu, s.MinFreeMemory = 132000, 102400 }),
		},
		{
			name: "a worker size",
			pool: types.WorkerPoolConfig{PoolSizing: types.WorkerPoolJobSpecPoolSizingConfig{
				DefaultWorkerCPU: "8000m", DefaultWorkerMemory: " 32Gi ", DefaultWorkerGpuCount: "2",
			}},
			want: defaults(func(s *types.WorkerPoolSizingConfig) {
				s.DefaultWorkerCpu, s.DefaultWorkerMemory, s.DefaultWorkerGpuCount = 8000, 32768, 2
			}),
		},
		{
			name: "what cannot be read takes its default and is named",
			pool: types.WorkerPoolConfig{GPUType: "A10G", PoolSizing: types.WorkerPoolJobSpecPoolSizingConfig{
				MinFreeCPU: "two", MinFreeMemory: "4GB", MinFreeGPU: "-1",
				DefaultWorkerCPU: "2 cores", DefaultWorkerMemory: "lots", DefaultWorkerGpuType: "3060", DefaultWorkerGpuCount: "one",
			}},
			want: defaults(nil),
			ignored: []string{
				`minFreeCpu "two": write it like 2000m`,
				`minFreeMemory "4GB": write it like 4Gi`,
				`minFreeGpu "-1": write it like 1`,
				`defaultWorkerCpu "2 cores": write it like 2000m`,
				`defaultWorkerMemory "lots": write it like 4Gi`,
				`defaultWorkerGpuType "3060": write it like A10G`,
				`defaultWorkerGpuCount "one": write it like 1`,
			},
		},
		{
			name: "a worker of a GPU type has one GPU",
			pool: types.WorkerPoolConfig{GPUType: "T4", PoolSizing: types.WorkerPoolJobSpecPoolSizingConfig{DefaultWorkerGpuType: "T4"}},
			want: defaults(func(s *types.WorkerPoolSizingConfig) { s.DefaultWorkerGpuType, s.DefaultWorkerGpuCount = "T4", 1 }),
		},
		{
			name: "or as many as were written",
			pool: types.WorkerPoolConfig{GPUType: "A100-40", PoolSizing: types.WorkerPoolJobSpecPoolSizingConfig{DefaultWorkerGpuType: "A100-40", DefaultWorkerGpuCount: "4"}},
			want: defaults(func(s *types.WorkerPoolSizingConfig) { s.DefaultWorkerGpuType, s.DefaultWorkerGpuCount = "A100-40", 4 }),
		},
		{
			name: "a GPU pool with no worker GPU type adds workers without one",
			pool: types.WorkerPoolConfig{GPUType: "A10G", PoolSizing: types.WorkerPoolJobSpecPoolSizingConfig{MinFreeCPU: "4000m"}},
			want: defaults(func(s *types.WorkerPoolSizingConfig) { s.MinFreeCpu = 4000 }),
		},
		{
			// With none, the workers added would never meet the floor.
			name: "unless it keeps GPUs free",
			pool: types.WorkerPoolConfig{GPUType: "A10G", PoolSizing: types.WorkerPoolJobSpecPoolSizingConfig{MinFreeGPU: "2"}},
			want: defaults(func(s *types.WorkerPoolSizingConfig) { s.MinFreeGpu, s.DefaultWorkerGpuCount = 2, 1 }),
		},
		{
			name: "a Kubernetes pool with no gpuType has no GPUs to keep free",
			pool: types.WorkerPoolConfig{Mode: types.PoolModeLocal, PoolSizing: types.WorkerPoolJobSpecPoolSizingConfig{MinFreeCPU: "2000m", MinFreeGPU: "1"}},
			want: defaults(func(s *types.WorkerPoolSizingConfig) { s.MinFreeCpu = 2000 }),
			ignored: []string{
				`minFreeGpu "1": the pool has no gpuType, so its workers have no GPU`,
			},
		},
		{
			name: "nor has a pool that buys machines without GPUs",
			pool: types.WorkerPoolConfig{
				Provisioning: types.WorkerPoolProvisioningConfig{Providers: []string{"hetzner"}, MaxNodes: 2},
				PoolSizing:   types.WorkerPoolJobSpecPoolSizingConfig{MinFreeGPU: "1"},
			},
			want:    defaults(nil),
			ignored: []string{`minFreeGpu "1": the pool has no gpuType, so its workers have no GPU`},
		},
		{
			// Its machines join with whatever GPUs they have.
			name: "a pool of joined machines with no gpuType may",
			pool: types.WorkerPoolConfig{Mode: types.PoolModeExternal, PoolSizing: types.WorkerPoolJobSpecPoolSizingConfig{MinFreeGPU: "1"}},
			want: defaults(func(s *types.WorkerPoolSizingConfig) { s.MinFreeGpu, s.DefaultWorkerGpuCount = 1, 1 }),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			sizing, ignored := readPoolSizing(test.pool)
			assert.Equal(t, test.want, sizing)
			assert.Equal(t, test.ignored, ignored)
		})
	}
}

func TestApplyBuildPoolSizingMinimums(t *testing.T) {
	config := types.AppConfig{}
	config.ImageService.BuildContainerPoolSelector = "build"
	config.ImageService.BuildContainerCpu = 14000
	config.ImageService.BuildContainerMemory = 54000

	sizing := &types.WorkerPoolSizingConfig{
		MinFreeCpu:          14000,
		MinFreeMemory:       55296,
		DefaultWorkerCpu:    1000,
		DefaultWorkerMemory: 55296,
	}

	applyBuildPoolSizingMinimums("build", config, sizing)

	assert.Equal(t, int64(14000), sizing.DefaultWorkerCpu)
	assert.Equal(t, int64(67500), sizing.DefaultWorkerMemory)
	assert.Equal(t, int64(14000), sizing.MinFreeCpu)
	assert.Equal(t, int64(55296), sizing.MinFreeMemory)
}

func TestApplyBuildPoolSizingMinimumsOnlyAppliesToBuildPool(t *testing.T) {
	config := types.AppConfig{}
	config.ImageService.BuildContainerPoolSelector = "build"
	config.ImageService.BuildContainerCpu = 14000
	config.ImageService.BuildContainerMemory = 54000

	sizing := &types.WorkerPoolSizingConfig{
		DefaultWorkerCpu:    1000,
		DefaultWorkerMemory: 1024,
	}

	applyBuildPoolSizingMinimums("default", config, sizing)

	assert.Equal(t, int64(1000), sizing.DefaultWorkerCpu)
	assert.Equal(t, int64(1024), sizing.DefaultWorkerMemory)
}

// The sizer asks before it adds. A pool that cannot add a worker (out of
// stock, at its limit, stalled) is left as it is, and that is not an error:
// the next tick asks again.
func TestSizerDoesNotAskAPoolThatCannotAddAWorker(t *testing.T) {
	underFloor := &WorkerPoolCapacity{}
	sizing := &types.WorkerPoolSizingConfig{MinFreeCpu: 4000, DefaultWorkerCpu: 2000, DefaultWorkerMemory: 1024}
	sizerFor := func(controller *LocalWorkerPoolControllerForTest) *WorkerPoolSizer {
		s, err := NewSchedulerForTest()
		require.NoError(t, err)
		controller.name, controller.workerRepo = "pool", s.workerRepo
		return &WorkerPoolSizer{controller: controller, sizing: sizing, workerPoolRepo: s.workerPoolRepo}
	}

	t.Run("a pool that can is asked", func(t *testing.T) {
		controller := &LocalWorkerPoolControllerForTest{}
		worker, err := sizerFor(controller).addWorkerIfNeeded(underFloor)
		require.NoError(t, err)
		require.NotNil(t, worker)
		require.Equal(t, 1, controller.AddWorkerCallCount())
	})

	t.Run("a pool that cannot is not", func(t *testing.T) {
		controller := &LocalWorkerPoolControllerForTest{cannotAddWorker: true}
		worker, err := sizerFor(controller).addWorkerIfNeeded(underFloor)
		require.NoError(t, err)
		require.Nil(t, worker)
		require.Zero(t, controller.AddWorkerCallCount())
	})

	t.Run("a pool that cannot be asked is not, and the error is reported", func(t *testing.T) {
		controller := &LocalWorkerPoolControllerForTest{canAddWorkerErr: errors.New("provider is down")}
		worker, err := sizerFor(controller).addWorkerIfNeeded(underFloor)
		require.ErrorContains(t, err, "provider is down")
		require.Nil(t, worker)
		require.Zero(t, controller.AddWorkerCallCount())
	})
}

// sizerLocks counts how often a pool's sizer asks for its lock.
type sizerLocks struct {
	repo.WorkerPoolRepository
	asked int
}

func (l *sizerLocks) WithWorkerPoolSizerLock(name string, do func()) error {
	l.asked++
	return l.WorkerPoolRepository.WithWorkerPoolSizerLock(name, do)
}

// A pool with no floor has nothing to keep free, and is not watched: it asks
// for no lock and reads no workers, however long it lives.
func TestOnlyAPoolWithAFloorIsSized(t *testing.T) {
	for _, test := range []struct {
		name    string
		sizing  types.WorkerPoolJobSpecPoolSizingConfig
		watched bool
	}{
		{"no floor", types.WorkerPoolJobSpecPoolSizingConfig{DefaultWorkerCPU: "2000m", DefaultWorkerMemory: "4Gi"}, false},
		{"a floor that cannot be read", types.WorkerPoolJobSpecPoolSizingConfig{MinFreeCPU: "two"}, false},
		{"a CPU floor", types.WorkerPoolJobSpecPoolSizingConfig{MinFreeCPU: "2000m"}, true},
		{"a memory floor", types.WorkerPoolJobSpecPoolSizingConfig{MinFreeMemory: "4Gi"}, true},
		{"a GPU floor", types.WorkerPoolJobSpecPoolSizingConfig{MinFreeGPU: "1"}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, err := NewSchedulerForTest()
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			controller := &LocalWorkerPoolControllerForTest{ctx: ctx, name: "pool", workerRepo: s.workerRepo}
			locks := &sizerLocks{WorkerPoolRepository: s.workerPoolRepo}
			fake := common.NewFakeClock(time.Now())

			MonitorPoolSize(controller, types.AppConfig{}, &types.WorkerPoolConfig{GPUType: "T4", PoolSizing: test.sizing}, locks, fake)
			fake.Advance(3 * poolMonitoringInterval)

			require.Equal(t, test.watched, locks.asked > 0, "asked for the sizer's lock %d times", locks.asked)
			require.Equal(t, test.watched, controller.AddWorkerCallCount() > 0, "added %d workers", controller.AddWorkerCallCount())
		})
	}
}
