package scheduler

import (
	"sync"

	repo "github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
)

// WorkerPool represents a pool of workers with a specific name, configuration,
// and a controller responsible for managing its worker instances.
type WorkerPool struct {
	Name       string
	Config     types.WorkerPoolConfig
	Controller WorkerPoolController
}

// WorkerPoolManager is responsible for managing multiple worker pools. It
// maintains a collection of worker pools, and provides methods to interact with
// and manage these pools. It uses a sync.RWMutex for concurrent access control
// to ensure thread safety.
type WorkerPoolManager struct {
	mu    sync.RWMutex
	pools map[string]*WorkerPool
	repo  repo.WorkerPoolRepository
}

// NewWorkerPoolManager creates a new instance of WorkerPoolManager with the
// specified worker pool repository ('repo'). It initializes an empty map of
// worker pools and associates it with the repository.
func NewWorkerPoolManager(repo repo.WorkerPoolRepository) *WorkerPoolManager {
	return &WorkerPoolManager{
		pools: map[string]*WorkerPool{},
		repo:  repo,
	}
}

// Gets a pool from memory.
// We don't fetch from the db because we don't know how to construct the controller. This is
// the responsibility of the caller calling `SetPool()`.
func (m *WorkerPoolManager) GetPool(name string) (*WorkerPool, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	pool, ok := m.pools[name]
	if !ok {
		return nil, false
	}

	return pool, true
}

func (m *WorkerPoolManager) GetPoolByGPU(request *types.ContainerRequest) (*WorkerPool, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// TODO: since we could have multiple pools per gpu, we need other variables
	// to allow us to choose between the different pools

	// Some ideas:
	//  - machine hourly cost
	//  - machine availability - pool controllers are GPU-specific,
	//  so if we know which provider a pool belongs to, we could try and figure out if we are going to have to wait for a machine

	for _, pool := range m.pools {
		// pool.Controller.HourlyPrice()
		// pool.Controller.EstimatedWait()
		if pool.Config.GPUType == request.Gpu {
			return pool, true
		}
	}

	return nil, false
}

// Set/add WorkerPool.
// This will overwrite any existing WorkerPools with the same name defined in WorkerPoolResource.
func (m *WorkerPoolManager) SetPool(name string, config types.WorkerPoolConfig, controller WorkerPoolController) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	pool := &WorkerPool{
		Name:       name,
		Config:     config,
		Controller: controller,
	}

	if err := m.repo.SetPool(pool.Name, pool.Config); err != nil {
		return err
	}

	m.pools[name] = pool

	return nil
}

// Remove WorkerPool.
// Removes from memory first since failures are a noop, then removes from the db.
func (m *WorkerPoolManager) RemovePool(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.pools, name)
	return m.repo.RemovePool(name)
}
