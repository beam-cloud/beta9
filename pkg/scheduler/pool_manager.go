package scheduler

import (
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

// WorkerPool is a pool this replica has loaded: the config it runs with and
// the controller that adds its workers.
type WorkerPool struct {
	Name       string
	Config     types.WorkerPoolConfig
	Controller WorkerPoolController
}

// WorkerPoolManager holds the pools this replica has loaded. A shared pool is
// kept under its name, a workspace's private pool under a key of its own.
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

// SharedPools returns the pools that any workspace may be routed to. A
// workspace's private pool is registered under a workspace-scoped key and is
// reachable only by naming it.
func (m *WorkerPoolManager) SharedPools() []*WorkerPool {
	var pools []*WorkerPool
	m.poolMap.Range(func(key string, pool *WorkerPool) bool {
		if key == pool.Name {
			pools = append(pools, pool)
		}
		return true
	})
	return pools
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
