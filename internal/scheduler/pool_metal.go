package scheduler

import (
	"errors"
	"log"

	"github.com/beam-cloud/beta9/internal/providers"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type MetalWorkerPoolController struct {
	name           string
	config         types.AppConfig
	provider       providers.Provider
	kubeClient     *kubernetes.Clientset
	workerPool     types.WorkerPoolConfig
	workerRepo     repository.WorkerRepository
	workerPoolRepo repository.WorkerPoolRepository
}

func NewMetalWorkerPoolController(
	config types.AppConfig,
	workerPoolName string,
	workerRepo repository.WorkerRepository,
	workerPoolRepo repository.WorkerPoolRepository,
	providerName *types.MachineProvider) (WorkerPoolController, error) {
	var provider providers.Provider = nil
	var err error = nil

	switch *providerName {
	case types.ProviderEC2:
		provider, err = providers.NewEC2Provider(config)
	default:
		return nil, errors.New("invalid provider name")
	}
	if err != nil {
		return nil, err
	}

	// TODO: make this machine specific
	kubeConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	kubeClient, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		return nil, err
	}

	workerPool, _ := config.Worker.Pools[workerPoolName]

	wpc := &MetalWorkerPoolController{
		name:           workerPoolName,
		config:         config,
		kubeClient:     kubeClient,
		workerPool:     workerPool,
		workerRepo:     workerRepo,
		workerPoolRepo: workerPoolRepo,
		provider:       provider,
	}

	return wpc, nil
}

func (wpc *MetalWorkerPoolController) AddWorker(cpu int64, memory int64, gpuType string) (*types.Worker, error) {
	workerId := GenerateWorkerId()

	// Check current machines for capacity
	wpc.provider.ListMachines()

	worker := &types.Worker{Id: workerId, Cpu: cpu, Memory: memory, Gpu: gpuType}
	worker.PoolId = PoolId(wpc.name)

	// Add the worker state
	if err := wpc.workerRepo.AddWorker(worker); err != nil {
		log.Printf("Unable to create worker: %+v\n", err)
		return nil, err
	}
	return worker, nil
}

func (wpc *MetalWorkerPoolController) Name() string {
	return wpc.name
}

func (wpc *MetalWorkerPoolController) FreeCapacity() (*WorkerPoolCapacity, error) {
	workers, err := wpc.workerRepo.GetAllWorkersInPool(PoolId(wpc.name))
	if err != nil {
		return nil, err
	}

	capacity := &WorkerPoolCapacity{
		FreeCpu:    0,
		FreeMemory: 0,
		FreeGpu:    0,
	}

	for _, worker := range workers {
		capacity.FreeCpu += worker.Cpu
		capacity.FreeMemory += worker.Memory

		if worker.Gpu != "" && (worker.Cpu > 0 && worker.Memory > 0) {
			capacity.FreeGpu += 1
		}
	}

	return capacity, nil
}
