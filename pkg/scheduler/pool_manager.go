package scheduler

import (
	"context"
	"fmt"
	"log"
	"sync"

	repo "github.com/beam-cloud/beam/pkg/repository"
	"github.com/beam-cloud/beam/pkg/types"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

// An interface used by the WorkerPoolControllerFactory type
type WorkerPoolControllerConfig interface{}

// A function type that is used to initialize a controller through indirection.
// It receives a WorkerPoolResource CR, a WorkerPoolControllerConfig and returns a WorkerPoolController.
type WorkerPoolControllerFactory func(resource *types.WorkerPoolResource, config WorkerPoolControllerConfig) (WorkerPoolController, error)

// Top level struct for WorkerPoolManager.
// It pairs together a WorkerPoolResource CR and a WorkerPoolController.
type WorkerPool struct {
	Resource   *types.WorkerPoolResource
	Controller WorkerPoolController
}

// Manages WorkerPools in a distributed system.
// Contains a RWMutex to allows concurrent getting/reading of WorkerPools, but lock when we write them.
type WorkerPoolManager struct {
	mu    sync.RWMutex
	pools map[string]*WorkerPool
	repo  repo.WorkerPoolRepository
}

// Gets a new WorkerPoolManager.
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

// Set/add WorkerPool.
// This will overwrite any existing WorkerPools with the same name defined in WorkerPoolResource.
func (m *WorkerPoolManager) SetPool(resource types.WorkerPoolResource, controller WorkerPoolController) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	pool := &WorkerPool{
		Resource:   &resource,
		Controller: controller,
	}

	if err := m.repo.SetPool(pool.Resource); err != nil {
		return err
	}

	m.pools[resource.Name] = pool

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

// Loads WorkerPool of a certain factory type.
// This can be called any number of times depending on how many factory controller types you want to load.
func (m *WorkerPoolManager) LoadPools(
	factory WorkerPoolControllerFactory,
	factoryConfig WorkerPoolControllerConfig,
	resources []types.WorkerPoolResource,
) error {
	for _, resource := range resources {
		controller, err := factory(&resource, factoryConfig)
		if err != nil {
			return fmt.Errorf("failed to create a controller: %v", err)
		}

		err = m.SetPool(resource, controller)
		if err != nil {
			return fmt.Errorf("failed to set worker pool: %v", err)
		}

		if pool, ok := m.pools[resource.Name]; ok {
			log.Printf("Loaded WorkerPool: %s\n", pool.Resource.Name)
		} else {
			return fmt.Errorf("failed to load worker pool: %s", resource.Name)
		}
	}

	return nil
}

// Get worker pool resources from Kubernetes
func GetWorkerPoolResources(namespace string) ([]types.WorkerPoolResource, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	// Create a clientset for the worker pool CRD
	cfg.GroupVersion = &schema.GroupVersion{Group: types.WorkerPoolSchemaGroupName, Version: types.WorkerPoolSchemaGroupVersion}
	cfg.APIPath = "/apis"
	cfg.NegotiatedSerializer = serializer.NewCodecFactory(scheme.Scheme)
	kubeClient, err := rest.RESTClientFor(cfg)
	if err != nil {
		return nil, err
	}

	// Get all worker pools
	result := kubeClient.Get().Namespace(namespace).Resource(types.WorkerPoolResourcePlural).Do(context.Background())
	if result.Error() != nil {
		return nil, fmt.Errorf("error getting worker pool resources from kubernetes: %v", result.Error())
	}

	workerPools := types.WorkerPoolList{}
	err = result.Into(&workerPools)
	if err != nil {
		return nil, fmt.Errorf("error deserializing worker pool list: %v", err)
	}

	return workerPools.Items, nil
}
