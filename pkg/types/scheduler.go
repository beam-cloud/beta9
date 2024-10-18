package types

import (
	"fmt"
	"time"
)

type WorkerStatus string

const (
	WorkerStatusAvailable WorkerStatus = "available"
	WorkerStatusPending   WorkerStatus = "pending"
	WorkerStatusDisabled  WorkerStatus = "disabled"
	WorkerStateTtlS       int          = 60
)

type Worker struct {
	Id                   string       `json:"id" redis:"id"`
	Status               WorkerStatus `json:"status" redis:"status"`
	TotalCpu             int64        `json:"total_cpu" redis:"total_cpu"`
	TotalMemory          int64        `json:"total_memory" redis:"total_memory"`
	TotalGpuCount        uint32       `json:"total_gpu_count" redis:"total_gpu_count"`
	FreeCpu              int64        `json:"free_cpu" redis:"free_cpu"`
	FreeMemory           int64        `json:"free_memory" redis:"free_memory"`
	FreeGpuCount         uint32       `json:"free_gpu_count" redis:"gpu_count"`
	Gpu                  string       `json:"gpu" redis:"gpu"`
	PoolName             string       `json:"pool_name" redis:"pool_name"`
	MachineId            string       `json:"machine_id" redis:"machine_id"`
	ResourceVersion      int64        `json:"resource_version" redis:"resource_version"`
	RequiresPoolSelector bool         `json:"requires_pool_selector" redis:"requires_pool_selector"`
	Priority             int32        `json:"priority" redis:"priority"`
	BuildVersion         string       `json:"build_version" redis:"build_version"`
}

type CapacityUpdateType int

const (
	AddCapacity CapacityUpdateType = iota
	RemoveCapacity
)

type ContainerStatus string

const (
	ContainerStatusPending  ContainerStatus = "PENDING"
	ContainerStatusRunning  ContainerStatus = "RUNNING"
	ContainerStatusStopping ContainerStatus = "STOPPING"
)

type ContainerAlreadyScheduledError struct {
	Msg string
}

func (e *ContainerAlreadyScheduledError) Error() string {
	return e.Msg
}

type ContainerState struct {
	ContainerId string          `redis:"container_id" json:"container_id"`
	StubId      string          `redis:"stub_id" json:"stub_id"`
	Status      ContainerStatus `redis:"status" json:"status"`
	ScheduledAt int64           `redis:"scheduled_at" json:"scheduled_at"`
	WorkspaceId string          `redis:"workspace_id" json:"workspace_id"`
	Gpu         string          `redis:"gpu" json:"gpu"`
	GpuCount    uint32          `redis:"gpu_count" json:"gpu_count"`
	Cpu         int64           `redis:"cpu" json:"cpu"`
	Memory      int64           `redis:"memory" json:"memory"`
}

type ContainerRequest struct {
	ContainerId      string          `json:"container_id"`
	EntryPoint       []string        `json:"entry_point"`
	Env              []string        `json:"env"`
	Cpu              int64           `json:"cpu"`
	Memory           int64           `json:"memory"`
	Gpu              string          `json:"gpu"`
	GpuRequest       []string        `json:"gpu_request"`
	GpuCount         uint32          `json:"gpu_count"`
	SourceImage      *string         `json:"source_image"`
	SourceImageCreds string          `json:"source_image_creds"`
	ImageId          string          `json:"image_id"`
	StubId           string          `json:"stub_id"`
	WorkspaceId      string          `json:"workspace_id"`
	Workspace        Workspace       `json:"workspace"`
	Timestamp        time.Time       `json:"timestamp"`
	Mounts           []Mount         `json:"mounts"`
	RetryCount       int             `json:"retry_count"`
	PoolSelector     string          `json:"pool_selector"`
	Stub             StubWithRelated `json:"stub"`
}

const ContainerExitCodeTtlS int = 300

const (
	ContainerDurationEmissionInterval      time.Duration = 5 * time.Second
	ContainerResourceUsageEmissionInterval time.Duration = 3 * time.Second
)
const ContainerStateTtlSWhilePending int = 600
const ContainerStateTtlS int = 60
const WorkspaceQuotaTtlS int = 600

type ErrContainerStateNotFound struct {
	ContainerId string
}

func (e *ErrContainerStateNotFound) Error() string {
	return fmt.Sprintf("container state not found: %s", e.ContainerId)
}

type ErrInvalidWorkerStatus struct {
}

func (e *ErrInvalidWorkerStatus) Error() string {
	return "invalid worker status"
}

type ErrNoSuitableWorkerFound struct {
}

func (e *ErrNoSuitableWorkerFound) Error() string {
	return "no suitable worker found for request"
}

type ErrWorkerNotFound struct {
	WorkerId string
}

func (e *ErrWorkerNotFound) Error() string {
	return fmt.Sprintf("worker not found: %s", e.WorkerId)
}

type WorkerPoolSizingConfig struct {
	MinFreeCpu            int64
	MinFreeMemory         int64
	MinFreeGpu            uint
	DefaultWorkerCpu      int64
	DefaultWorkerMemory   int64
	DefaultWorkerGpuType  string
	DefaultWorkerGpuCount uint32
}

func NewWorkerPoolSizingConfig() *WorkerPoolSizingConfig {
	return &WorkerPoolSizingConfig{
		MinFreeCpu:            0,
		MinFreeMemory:         0,
		MinFreeGpu:            0,
		DefaultWorkerCpu:      1000,
		DefaultWorkerMemory:   1 * 1024, // 1Gi
		DefaultWorkerGpuType:  "",
		DefaultWorkerGpuCount: 0,
	}
}

type ThrottledByConcurrencyLimitError struct {
	Reason string
}

func (e *ThrottledByConcurrencyLimitError) Error() string {
	return "concurrency_limit_reached: " + e.Reason
}

type QuotaDoesNotExistError struct{}

func (e *QuotaDoesNotExistError) Error() string {
	return "quota_does_not_exist"
}
