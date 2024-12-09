package scheduler

import (
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
