package scheduler

import (
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type MetalWorkerPoolController struct {
	name       string
	config     types.AppConfig
	kubeClient *kubernetes.Clientset
	workerPool types.WorkerPoolConfig
	workerRepo repository.WorkerRepository
}

func NewMetalWorkerPoolController(config types.AppConfig, workerPoolName string, workerRepo repository.WorkerRepository) (WorkerPoolController, error) {
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
		name:       workerPoolName,
		config:     config,
		kubeClient: kubeClient,
		workerPool: workerPool,
		workerRepo: workerRepo,
	}

	return wpc, nil
}

func (wpc *MetalWorkerPoolController) AddWorker(cpu int64, memory int64, gpuType string) (*types.Worker, error) {
	workerId := GenerateWorkerId()
	return &types.Worker{Id: workerId}, nil
}

func (wpc *MetalWorkerPoolController) AddWorkerWithId(workerId string, cpu int64, memory int64, gpuType string) (*types.Worker, error) {
	return &types.Worker{Id: workerId}, nil
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
