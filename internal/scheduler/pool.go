package scheduler

import (
	"github.com/beam-cloud/beta9/internal/types"
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
	defaultClusterDomain    string = "cluster.local"
	defaultContainerName    string = "worker"
	defaultWorkerEntrypoint string = "/usr/local/bin/worker"
	defaultWorkerLogPath    string = "/var/log/worker"
)

type WorkerPoolController interface {
	AddWorker(cpu int64, memory int64, gpuType string) (*types.Worker, error)
	AddWorkerWithId(workerId string, cpu int64, memory int64, gpuType string) (*types.Worker, error)
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
