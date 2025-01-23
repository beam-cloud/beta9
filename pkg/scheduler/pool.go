package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	Beta9WorkerLabelKey         string  = "run.beam.cloud/role"
	Beta9WorkerLabelValue       string  = "worker"
	Beta9WorkerJobPrefix        string  = "worker"
	Beta9WorkerLabelIDKey       string  = "run.beam.cloud/worker-id"
	Beta9WorkerLabelPoolNameKey string  = "run.beam.cloud/worker-pool-name"
	PrometheusPortKey           string  = "prometheus.io/port"
	PrometheusScrapeKey         string  = "prometheus.io/scrape"
	tmpVolumeName               string  = "beta9-tmp"
	logVolumeName               string  = "beta9-logs"
	imagesVolumeName            string  = "beta9-images"
	defaultContainerName        string  = "worker"
	defaultWorkerEntrypoint     string  = "/usr/local/bin/worker"
	defaultWorkerLogPath        string  = "/var/log/worker"
	defaultImagesPath           string  = "/images"
	defaultSharedMemoryPct      float32 = 0.5
)

type WorkerPoolState struct {
	FreeCpu    int64
	FreeMemory int64
	FreeGpu    uint
}

type WorkerPoolController interface {
	AddWorker(cpu int64, memory int64, gpuCount uint32) (*types.Worker, error)
	AddWorkerToMachine(cpu int64, memory int64, gpuType string, gpuCount uint32, machineId string) (*types.Worker, error)
	Name() string
	FreeCapacity() (*WorkerPoolCapacity, error)
	Context() context.Context
	IsPreemptable() bool
	State() WorkerPoolState
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
	providerRepo repository.ProviderRepository) error {
	poolSizer, err := NewWorkerPoolSizer(wpc, workerPoolConfig, workerRepo, providerRepo)
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
		// Exclude disabled workers from the capacity calculation
		if worker.Status == types.WorkerStatusDisabled {
			continue
		}

		capacity.FreeCpu += worker.FreeCpu
		capacity.FreeMemory += worker.FreeMemory

		if worker.Gpu != "" && (worker.FreeCpu > 0 && worker.FreeMemory > 0) {
			capacity.FreeGpu += uint(worker.FreeGpuCount)
		}
	}

	return capacity, nil
}

func calculateMemoryQuantity(percentStr string, memoryTotal int64) resource.Quantity {
	percent, err := parseMemoryPercentage(percentStr)
	if err != nil {
		percent = defaultSharedMemoryPct
	}

	return resource.MustParse(fmt.Sprintf("%dMi", int64(float32(memoryTotal)*percent)))
}

func parseMemoryPercentage(percentStr string) (float32, error) {
	ps := strings.TrimSuffix(percentStr, "%")

	percent, err := strconv.ParseFloat(ps, 32)
	if err != nil {
		return 0, err
	}

	if percent <= 0 {
		return 0, errors.New("percent must be greater than 0")
	}

	return float32(percent) / 100, nil
}
