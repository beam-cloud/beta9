package scheduler

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestAddWorkerIfNeeded(t *testing.T) {
	s, err := miniredis.Run()
	assert.NotNil(t, s)
	assert.Nil(t, err)

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
			name:   "should add worker when CPU is not sufficient GPU",
			config: configWithGpu,
			freeCapacity: &WorkerPoolCapacity{
				FreeCpu:    5000,
				FreeMemory: 1000,
				FreeGpu:    1,
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
				controller:             controller,
				workerPoolSizingConfig: tt.config,
				workerPoolRepo:         workerPoolRepo,
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

func TestParsePoolSizingConfig(t *testing.T) {
	tests := []struct {
		name             string
		sizingConfigHave *types.WorkerPoolJobSpecPoolSizingConfig
		sizingConfigWant *types.WorkerPoolSizingConfig
	}{
		{
			name:             "should set defaults when no values provided",
			sizingConfigHave: &types.WorkerPoolJobSpecPoolSizingConfig{},
			sizingConfigWant: &types.WorkerPoolSizingConfig{
				MinFreeCpu:           0,
				MinFreeMemory:        0,
				MinFreeGpu:           0,
				DefaultWorkerCpu:     1000,
				DefaultWorkerMemory:  1024,
				DefaultWorkerGpuType: "",
			},
		},
		{
			name: "should ignore bad values and use default values",
			sizingConfigHave: &types.WorkerPoolJobSpecPoolSizingConfig{
				MinFreeCPU:           "bad value",
				MinFreeMemory:        "bad value",
				MinFreeGPU:           "bad value",
				DefaultWorkerCPU:     "bad value",
				DefaultWorkerMemory:  "bad value",
				DefaultWorkerGpuType: "bad value",
			},
			sizingConfigWant: &types.WorkerPoolSizingConfig{
				MinFreeCpu:           0,
				MinFreeMemory:        0,
				MinFreeGpu:           0,
				DefaultWorkerCpu:     1000,
				DefaultWorkerMemory:  1024,
				DefaultWorkerGpuType: "",
			},
		},
		{
			name: "should parse minFreeCpu minFreeMemory and minFreeGpu",
			sizingConfigHave: &types.WorkerPoolJobSpecPoolSizingConfig{
				MinFreeCPU:    "132000m",
				MinFreeMemory: "100Gi",
				MinFreeGPU:    "0",
			},
			sizingConfigWant: &types.WorkerPoolSizingConfig{
				MinFreeCpu:          132000,
				MinFreeMemory:       102400,
				MinFreeGpu:          0,
				DefaultWorkerCpu:    1000,
				DefaultWorkerMemory: 1024,
			},
		},
		{
			name:             "should parse defaultWorkerGpuType as T4",
			sizingConfigHave: &types.WorkerPoolJobSpecPoolSizingConfig{DefaultWorkerGpuType: "T4"},
			sizingConfigWant: &types.WorkerPoolSizingConfig{
				DefaultWorkerCpu:      1000,
				DefaultWorkerMemory:   1024,
				DefaultWorkerGpuType:  "T4",
				DefaultWorkerGpuCount: 1,
			},
		},
		{
			name:             "should parse defaultWorkerGpuType as A100-40",
			sizingConfigHave: &types.WorkerPoolJobSpecPoolSizingConfig{DefaultWorkerGpuType: "A100-40"},
			sizingConfigWant: &types.WorkerPoolSizingConfig{
				DefaultWorkerCpu:      1000,
				DefaultWorkerMemory:   1024,
				DefaultWorkerGpuType:  "A100-40",
				DefaultWorkerGpuCount: 1,
			},
		},
		{
			name:             "should return empty DefaultWorkerGpuType when using 3060 (str) as defaultWorkerGpuType",
			sizingConfigHave: &types.WorkerPoolJobSpecPoolSizingConfig{DefaultWorkerGpuType: "3060"},
			sizingConfigWant: &types.WorkerPoolSizingConfig{
				DefaultWorkerCpu:     1000,
				DefaultWorkerMemory:  1024,
				DefaultWorkerGpuType: "",
			},
		},
		{
			name:             "should set empty DefaultWorkerGpuType when using 4090 (int) as defaultWorkerGpuType",
			sizingConfigHave: &types.WorkerPoolJobSpecPoolSizingConfig{DefaultWorkerGpuType: "4090"},
			sizingConfigWant: &types.WorkerPoolSizingConfig{
				DefaultWorkerCpu:     1000,
				DefaultWorkerMemory:  1024,
				DefaultWorkerGpuType: "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := parsePoolSizingConfig(*tt.sizingConfigHave)
			assert.NoError(t, err)

			assert.Equal(t, tt.sizingConfigWant, config)
		})
	}
}
