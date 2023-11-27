package scheduler

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beam/internal/common"
	repo "github.com/beam-cloud/beam/internal/repository"
	"github.com/beam-cloud/beam/internal/types"
	"github.com/stretchr/testify/assert"
)

func TestAddWorkerIfNeeded(t *testing.T) {
	s, err := miniredis.Run()
	assert.NotNil(t, s)
	assert.Nil(t, err)

	redisClient, err := common.NewRedisClient(common.WithAddress(s.Addr()))
	assert.NotNil(t, redisClient)
	assert.Nil(t, err)

	workerRepo := repo.NewWorkerRedisRepositoryForTest(redisClient)

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
			controller, _ := NewWorkerPoolControllerForTest("TestPool", &WorkerPoolControllerConfigForTest{
				namespace:        "namespace",
				workerPoolConfig: &WorkerPoolConfig{},
				workerRepo:       workerRepo,
			})
			sizer := &WorkerPoolSizer{
				controller: controller,
				config:     tt.config,
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
		name                 string
		sizingConfigJSON     string
		sizingConfigExpected *types.WorkerPoolSizingConfig
	}{
		{
			name:             "should set defaults when no values provided",
			sizingConfigJSON: "{}",
			sizingConfigExpected: &types.WorkerPoolSizingConfig{
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
			sizingConfigJSON: `
				{
					"minFreeCpu": "bad value",
					"minFreeMemory": "bad value",
					"minFreeGpu": "bad value",
					"defaultWorkerCpu": -10,
					"defaultWorkerMemory": -10,
					"defaultWorkerGpuType": "bad value"
				}
			`,
			sizingConfigExpected: &types.WorkerPoolSizingConfig{
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
			sizingConfigJSON: `
				{
					"minFreeCpu": "132000m",
					"minFreeMemory": "100Gi",
					"minFreeGpu": 0
				}
			`,
			sizingConfigExpected: &types.WorkerPoolSizingConfig{
				MinFreeCpu:          132000,
				MinFreeMemory:       102400,
				MinFreeGpu:          0,
				DefaultWorkerCpu:    1000,
				DefaultWorkerMemory: 1024,
			},
		},
		{
			name:             "should parse defaultWorkerGpuType as T4",
			sizingConfigJSON: `{"defaultWorkerGpuType": "T4"}`,
			sizingConfigExpected: &types.WorkerPoolSizingConfig{
				DefaultWorkerCpu:     1000,
				DefaultWorkerMemory:  1024,
				DefaultWorkerGpuType: "T4",
			},
		},
		{
			name:             "should parse defaultWorkerGpuType as A100-40",
			sizingConfigJSON: `{"defaultWorkerGpuType": "A100-40"}`,
			sizingConfigExpected: &types.WorkerPoolSizingConfig{
				DefaultWorkerCpu:     1000,
				DefaultWorkerMemory:  1024,
				DefaultWorkerGpuType: "A100-40",
			},
		},
		{
			name:             "should return empty DefaultWorkerGpuType when using 3060 (str) as defaultWorkerGpuType",
			sizingConfigJSON: `{"defaultWorkerGpuType": "3060"}`,
			sizingConfigExpected: &types.WorkerPoolSizingConfig{
				DefaultWorkerCpu:     1000,
				DefaultWorkerMemory:  1024,
				DefaultWorkerGpuType: "",
			},
		},
		{
			name:             "should set empty DefaultWorkerGpuType when using 4090 (int) as defaultWorkerGpuType",
			sizingConfigJSON: `{"defaultWorkerGpuType": 4090}`,
			sizingConfigExpected: &types.WorkerPoolSizingConfig{
				DefaultWorkerCpu:     1000,
				DefaultWorkerMemory:  1024,
				DefaultWorkerGpuType: "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := []byte(tt.sizingConfigJSON)
			config, err := ParsePoolSizingConfig(raw)
			assert.NoError(t, err)

			assert.Equal(t, tt.sizingConfigExpected, config)
		})
	}
}
