package scheduler

import (
	"github.com/beam-cloud/beam/internal/types"
)

const (
	BeamWorkerLabelKey      string = "run.beam.cloud/role"
	BeamWorkerLabelValue    string = "worker"
	BeamWorkerJobPrefix     string = "worker"
	tmpVolumeName           string = "beam-tmp"
	logVolumeName           string = "beam-logs"
	imagesVolumeName        string = "beam-images"
	configVolumeName        string = "beam-config"
	configSecretName        string = "beam"
	configMountPath         string = "/etc/config"
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

// TODO: Some of these variables are tied to the worker pool jobs and should really live on the CR
// Once things are stable we can move those over.
type WorkerPoolConfig struct {
	DataVolumeName             string
	DefaultWorkerCpuRequest    int64
	DefaultWorkerMemoryRequest int64
}

type WorkerPoolCapacity struct {
	FreeCpu    int64
	FreeMemory int64
	FreeGpu    uint
}
