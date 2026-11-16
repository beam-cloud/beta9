package scheduler

import (
	"cmp"
	"slices"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

// WorkerPool represents a pool of workers with specific configuration and controller.
type WorkerPool struct {
	Name       string
	Config     types.WorkerPoolConfig
	Controller WorkerPoolController
}

// WorkerPoolManager manages a collection of WorkerPools using a thread-safe SafeMap.
// It provides additional functionality to filter and retrieve pools based on specific criteria, such as GPU type.
type WorkerPoolManager struct {
	poolMap *common.SafeMap[*WorkerPool]
}

// NewWorkerPoolManager creates a new WorkerPoolManager.
func NewWorkerPoolManager() *WorkerPoolManager {
	return &WorkerPoolManager{
		poolMap: common.NewSafeMap[*WorkerPool](),
	}
}

// GetPool retrieves a WorkerPool by its name.
func (m *WorkerPoolManager) GetPool(name string) (*WorkerPool, bool) {
	return m.poolMap.Get(name)
}

type poolFilters struct {
	GPUType string
	// Preemptable *bool
	// TODO: add preemptable filter back once we have better ways of handling pool state
	// (i.e. if a worker is not appearing in a certain pool)
}

// GetPoolByFilters retrieves all WorkerPools that match the specified filters.
// It returns a slice of WorkerPools that match all specified filters (GPU type and preemptibility),
// sorted by WorkerPoolConfig.Priority in descending order.
func (m *WorkerPoolManager) GetPoolByFilters(filters poolFilters) []*WorkerPool {
	var pools []*WorkerPool

	m.poolMap.Range(func(key string, pool *WorkerPool) bool {
		// Skip pools that do not match the GPU filter.
		if pool.Config.GPUType != filters.GPUType && filters.GPUType != string(types.GPU_ANY) {
			return true
		}

		// If the GPUType filter is ANY and the pool has no GPUType, skip it.
		if filters.GPUType == string(types.GPU_ANY) && pool.Config.GPUType == "" {
			return true
		}

		pools = append(pools, pool)
		return true
	})

	slices.SortFunc(pools, func(a, b *WorkerPool) int {
		return cmp.Compare(b.Config.Priority, a.Config.Priority)
	})

	return pools
}

// GetPoolByGPU retrieves a WorkerPool by its GPU type.
// It returns the first matching WorkerPool found.
func (m *WorkerPoolManager) GetPoolByGPU(gpuType string) (*WorkerPool, bool) {
	var wp *WorkerPool
	var found bool

	m.poolMap.Range(func(key string, pool *WorkerPool) bool {
		if pool.Config.GPUType != gpuType {
			return true
		}

		wp, found = pool, true
		return false
	})

	return wp, found
}

// SetPool adds or updates a WorkerPool in the manager.
func (m *WorkerPoolManager) SetPool(name string, config types.WorkerPoolConfig, controller WorkerPoolController) {
	m.SetPoolAt(name, name, config, controller)
}

func (m *WorkerPoolManager) SetPoolAt(key, name string, config types.WorkerPoolConfig, controller WorkerPoolController) {
	m.poolMap.Set(key, &WorkerPool{Name: name, Config: config, Controller: controller})
}

func (m *WorkerPoolManager) DeletePool(name string) {
	m.poolMap.Delete(name)
}
