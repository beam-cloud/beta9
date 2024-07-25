package scheduler

import (
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
		capacity.FreeCpu += worker.FreeCpu
		capacity.FreeMemory += worker.FreeMemory

		if worker.Gpu != "" && (worker.FreeCpu > 0 && worker.FreeMemory > 0) {
			capacity.FreeGpu += uint(worker.FreeGpuCount)
		}
	}

	return capacity, nil
}

// calculateMemoryQuantity calculates the memory based on the percentage and total memory.
func calculateMemoryQuantity(percent float32, memoryTotal int64) resource.Quantity {
	return resource.MustParse(fmt.Sprintf("%dMi", int64(float32(memoryTotal)*percent)))
}

// getPercentageWithDefault parses the percentage string and returns the default value if there's an error.
func getPercentageWithDefault(percentageStr string, defaultValue float32) float32 {
	percent, err := parsePercentage(percentageStr)
	if err != nil {
		return defaultValue
	}
	return percent
}

// parsePercentage converts a percentage string (e.g., "50%") to a decimal (e.g., 0.5).
func parsePercentage(percentStr string) (float32, error) {
	s := strings.TrimSuffix(percentStr, "%")

	percent, err := strconv.ParseFloat(s, 32)
	if err != nil {
		return 0, err
	}

	return float32(percent) / 100, nil
}
