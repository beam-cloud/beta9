package scheduler

import (
	"fmt"
	"strconv"

	"github.com/beam-cloud/beam/pkg/common"
	"github.com/beam-cloud/beam/pkg/types"
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
	defaultProvisionerLabel string = "karpenter.sh/provisioner-name"
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
	DefaultMaxGpuCpuRequest    int64
	DefaultMaxGpuMemoryRequest int64
	AgentToken                 string
}

type WorkerPoolCapacity struct {
	FreeCpu    int64
	FreeMemory int64
	FreeGpu    uint
}

func NewWorkerPoolConfig() (*WorkerPoolConfig, error) {
	cfg := &WorkerPoolConfig{}

	var err error

	cfg.DataVolumeName = common.Secrets().GetWithDefault("BEAM_DATA_VOLUME_NAME", "beam-data")

	defaultWorkerCpuRequestStr := common.Secrets().GetWithDefault("DEFAULT_WORKER_CPU_REQUEST", "2000")
	cfg.DefaultWorkerCpuRequest, err = strconv.ParseInt(defaultWorkerCpuRequestStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("error parsing DEFAULT_WORKER_CPU_REQUEST: %v", err)
	}

	defaultWorkerMemoryRequestStr := common.Secrets().GetWithDefault("DEFAULT_WORKER_MEMORY_REQUEST", "1024")
	cfg.DefaultWorkerMemoryRequest, err = strconv.ParseInt(defaultWorkerMemoryRequestStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("error parsing DEFAULT_WORKER_MEMORY_REQUEST: %v", err)
	}

	defaultMaxGpuCpuRequestStr := common.Secrets().GetWithDefault("DEFAULT_WORKER_MAX_GPU_CPU_REQUEST", "8000")
	cfg.DefaultMaxGpuCpuRequest, err = strconv.ParseInt(defaultMaxGpuCpuRequestStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("error parsing DEFAULT_WORKER_MAX_GPU_CPU_REQUEST: %v", err)
	}

	defaultMaxGpuMemoryRequestStr := common.Secrets().GetWithDefault("DEFAULT_WORKER_MAX_GPU_MEMORY_REQUEST", "16384")
	cfg.DefaultMaxGpuMemoryRequest, err = strconv.ParseInt(defaultMaxGpuMemoryRequestStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("error parsing DEFAULT_WORKER_MAX_GPU_MEMORY_REQUEST: %v", err)
	}

	return cfg, nil
}
