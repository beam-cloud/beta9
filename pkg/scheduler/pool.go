package scheduler

import (
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
)

const (
	Beta9WorkerLabelKey         string = "run.beam.cloud/role"
	Beta9WorkerLabelValue       string = "worker"
	Beta9WorkerJobPrefix        string = "worker"
	Beta9WorkerLabelIDKey       string = "run.beam.cloud/worker-id"
	Beta9WorkerLabelPoolNameKey string = "run.beam.cloud/worker-pool-name"
	PrometheusPortKey           string = "prometheus.io/port"
	PrometheusScrapeKey         string = "prometheus.io/scrape"
	tmpVolumeName               string = "beta9-tmp"
	logVolumeName               string = "beta9-logs"
	imagesVolumeName            string = "beta9-images"
	cacheVolumeName             string = "beta9-cache"
	defaultContainerName        string = "worker"
	defaultWorkerEntrypoint     string = "/usr/local/bin/worker"
	defaultWorkerLogPath        string = "/var/log/worker"
	defaultImagesPath           string = "/images"
	defaultCachePath            string = "/cache"
)

type WorkerPoolController interface {
	AddWorker(cpu int64, memory int64, gpuType string, gpuCount uint32) (*types.Worker, error)
	AddWorkerToMachine(cpu int64, memory int64, gpuType string, gpuCount uint32, machineId string) (*types.Worker, error)
	Name() string
	FreeCapacity() (*WorkerPoolCapacity, error)
}

type WorkerPoolConfig struct {
	DefaultWorkerCpuRequest    int64
	DefaultWorkerMemoryRequest int64
}

type WorkerPoolCapacity struct {
	FreeCpu    int64
	FreeMemory int64
	FreeGpu    uint
}

func GenerateWorkerId() string {
	return uuid.New().String()[:8]
}

func MonitorPoolSize(wpc WorkerPoolController,
	workerPoolConfig *types.WorkerPoolConfig,
	workerRepo repository.WorkerRepository,
	workerPoolRepo repository.WorkerPoolRepository,
	providerRepo repository.ProviderRepository) error {
	poolSizer, err := NewWorkerPoolSizer(wpc, workerPoolConfig, workerRepo, workerPoolRepo, providerRepo)
	if err != nil {
		return err
	}

	go poolSizer.Start()
	return nil
}

func freePoolCapacity(workerRepo repository.WorkerRepository, wpc WorkerPoolController) (*WorkerPoolCapacity, error) {
	workers, err := workerRepo.GetAllWorkersInPool(wpc.Name())
	if err != nil {
		return nil, err
	}

	capacity := &WorkerPoolCapacity{
		FreeCpu:    0,
		FreeMemory: 0,
		FreeGpu:    0,
	}

	for _, worker := range workers {
		capacity.FreeCpu += worker.FreeCpu
		capacity.FreeMemory += worker.FreeMemory

		if worker.Gpu != "" && (worker.FreeCpu > 0 && worker.FreeMemory > 0) {
			capacity.FreeGpu += uint(worker.FreeGpuCount)
		}
	}

	return capacity, nil
}
