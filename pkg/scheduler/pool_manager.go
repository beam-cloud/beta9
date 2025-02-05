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

	m.poolMap.Range(func(key string, value *WorkerPool) bool {
		gpuMatches := value.Config.GPUType == filters.GPUType

		state, err := value.Controller.State()
		if err != nil {
			return true
		}

		// If the pool status is degraded, we don't want to include it in the results
		if state.Status == types.WorkerPoolStatusDegraded {
			return true
		}

		if gpuMatches {
			pools = append(pools, value)
		}

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
	var ok bool

	m.poolMap.Range(func(key string, value *WorkerPool) bool {
		if value.Config.GPUType == gpuType {
			wp, ok = value, true
			return false
		}
		return true
	})

	return wp, ok
}

// GetPoolsByGPU retrieves all WorkerPools by their GPU type.
// It returns a slice of matching WorkerPools. The results are sorted by
// WorkerPoolConfig.Priority in descending order.
func (m *WorkerPoolManager) GetPoolsByGPU(gpuType string) []*WorkerPool {
	var pools []*WorkerPool

	m.poolMap.Range(func(key string, value *WorkerPool) bool {
		if value.Config.GPUType == gpuType {
			pools = append(pools, value)
		}
		return true
	})

	slices.SortFunc(pools, func(a, b *WorkerPool) int {
		return cmp.Compare(b.Config.Priority, a.Config.Priority)
	})

	return pools
}

func (m *WorkerPoolManager) SetPool(name string, config types.WorkerPoolConfig, controller WorkerPoolController) {
	m.poolMap.Set(name, &WorkerPool{Name: name, Config: config, Controller: controller})
}
