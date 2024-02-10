package scheduler

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/google/uuid"
)

const (
	Beta9WorkerLabelKey     string = "run.beam.cloud/role"
	Beta9WorkerLabelValue   string = "worker"
	Beta9WorkerJobPrefix    string = "worker"
	PrometheusPortKey       string = "prometheus.io/port"
	PrometheusScrapeKey     string = "prometheus.io/scrape"
	tmpVolumeName           string = "beta9-tmp"
	logVolumeName           string = "beta9-logs"
	imagesVolumeName        string = "beta9-images"
	configVolumeName        string = "beta9-config"
	configSecretName        string = "beta9-config"
	configMountPath         string = "/etc/config"
	defaultContainerName    string = "worker"
	defaultWorkerEntrypoint string = "/usr/local/bin/worker"
	defaultWorkerLogPath    string = "/var/log/worker"
	defaultImagesPath       string = "/images"
)

type WorkerPoolController interface {
	AddWorker(cpu int64, memory int64, gpuType string, gpuCount uint32) (*types.Worker, error)
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

func PoolId(name string) string {
	hasher := sha256.New()
	hasher.Write([]byte(name))
	hash := hasher.Sum(nil)
	poolId := hex.EncodeToString(hash[:8])
	return poolId
}

func GenerateWorkerId() string {
	return uuid.New().String()[:8]
}

func MonitorPoolSize(wpc WorkerPoolController, workerPool *types.WorkerPoolConfig) error {
	config, err := ParsePoolSizingConfig(workerPool.PoolSizing)
	if err != nil {
		return err
	}

	poolSizer, err := NewWorkerPoolSizer(wpc, config)
	if err != nil {
		return err
	}

	go poolSizer.Start()
	return nil
}

func freePoolCapacity(workerRepo repository.WorkerRepository, wpc WorkerPoolController) (*WorkerPoolCapacity, error) {
	workers, err := workerRepo.GetAllWorkersInPool(PoolId(wpc.Name()))
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
			capacity.FreeGpu += uint(worker.GpuCount)
		}
	}

	return capacity, nil
}
