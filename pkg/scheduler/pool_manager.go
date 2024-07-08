package scheduler

import (
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

func (m *WorkerPoolManager) SetPool(name string, config types.WorkerPoolConfig, controller WorkerPoolController) {
	m.poolMap.Set(name, &WorkerPool{Name: name, Config: config, Controller: controller})
}
