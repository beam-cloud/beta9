package scheduler

import (
	"slices"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestGetPoolsByGPU(t *testing.T) {
	m := NewWorkerPoolManager()

	// Test that the pools are sorted by priority
	pools := []*WorkerPool{
		{
			Name: "aws-t4",
			Config: types.WorkerPoolConfig{
				GPUType:  "T4",
				Priority: -1,
			},
			Controller: &LocalWorkerPoolControllerForTest{},
		},
		{
			Name: "gcp-t4",
			Config: types.WorkerPoolConfig{
				GPUType:  "T4",
				Priority: 0,
			},
			Controller: &LocalWorkerPoolControllerForTest{},
		},
	}

	for _, pool := range pools {
		m.SetPool(pool.Name, pool.Config, pool.Controller)
	}

	t4s := m.GetPoolsByGPU("T4")
	assert.Equal(t, 2, len(t4s))
	assert.Equal(t, "gcp-t4", t4s[0].Name) // highest priority
	assert.Equal(t, "aws-t4", t4s[1].Name)

	// When a new pool is added, it will still be sorted by priority
	wp := &WorkerPool{
		Name: "xyz-t4",
		Config: types.WorkerPoolConfig{
			GPUType:  "T4",
			Priority: 10,
		},
	}
	m.SetPool(wp.Name, wp.Config, wp.Controller)

	t4s = m.GetPoolsByGPU("T4")
	assert.Equal(t, 3, len(t4s))
	assert.Equal(t, "xyz-t4", t4s[0].Name) // highest priority
	assert.Equal(t, "gcp-t4", t4s[1].Name)
	assert.Equal(t, "aws-t4", t4s[2].Name)
}

func TestWorkerPoolManager_GetPoolByFilters(t *testing.T) {
	manager := NewWorkerPoolManager()

	controller := &LocalWorkerPoolControllerForTest{
		preemptable: true,
	}

	// Set up test pools with different configurations
	testPools := []struct {
		name       string
		config     types.WorkerPoolConfig
		controller WorkerPoolController
	}{
		{
			name: "pool1",
			config: types.WorkerPoolConfig{
				GPUType:  "",
				Priority: 100,
			},
			controller: controller,
		},
		{
			name: "pool2",
			config: types.WorkerPoolConfig{
				GPUType:  "",
				Priority: 200,
			},
			controller: controller,
		},
		{
			name: "pool3",
			config: types.WorkerPoolConfig{
				GPUType:  "A100-40",
				Priority: 150,
			},
			controller: controller,
		},
	}

	// Add pools to manager
	for _, pool := range testPools {
		manager.SetPool(pool.name, pool.config, pool.controller)
	}

	// Test cases
	tests := []struct {
		name    string
		filters poolFilters
		want    []string // Expected pool names in order
		wantLen int
	}{
		{
			name: "filter by GPU type only",
			filters: poolFilters{
				GPUType: "A100-40",
			},
			want:    []string{"pool3"},
			wantLen: 1,
		},
		{
			name: "filter by all CPU pools",
			filters: poolFilters{
				GPUType: "",
			},
			want:    []string{"pool2", "pool1"},
			wantLen: 2,
		},
		{
			name: "filter by GPU type",
			filters: poolFilters{
				GPUType: "A100-40",
			},
			want:    []string{"pool3"},
			wantLen: 1,
		},
		{
			name: "no matching GPU type",
			filters: poolFilters{
				GPUType: "P100",
			},
			want:    []string{},
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := manager.GetPoolByFilters(tt.filters)

			if len(got) != tt.wantLen {
				t.Errorf("GetPoolByFilters() returned %d pools, want %d", len(got), tt.wantLen)
			}

			// Check if pools are returned in correct order
			gotNames := make([]string, len(got))
			for i, pool := range got {
				gotNames[i] = pool.Name
			}

			if !slices.Equal(gotNames, tt.want) {
				t.Errorf("GetPoolByFilters() returned pools %v, want %v", gotNames, tt.want)
			}
		})
	}
}
