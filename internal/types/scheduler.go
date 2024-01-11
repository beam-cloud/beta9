package types

import (
	"fmt"
	"time"
)

type WorkerStatus string

const (
	WorkerStatusAvailable WorkerStatus = "available"
	WorkerStatusPending   WorkerStatus = "pending"
)

type Worker struct {
	Id              string       `json:"id" redis:"id"`
	Status          WorkerStatus `json:"status" redis:"status"`
	Cpu             int64        `json:"cpu" redis:"cpu"`
	Memory          int64        `json:"memory" redis:"memory"`
	Gpu             string       `json:"gpu" redis:"gpu"`
	Agent           string       `json:"agent" redis:"agent"`
	PoolId          string       `json:"pool_id" redis:"pool_id"`
	ResourceVersion int64        `json:"resource_version" redis:"resource_version"`
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
	ContainerId string          `redis:"container_id"`
	Status      ContainerStatus `redis:"status"`
	ScheduledAt int64           `redis:"scheduled_at"`
}

type ContainerRequest struct {
	ContainerId     string    `json:"container_id"`
	EntryPoint      []string  `json:"entry_point"`
	Env             []string  `json:"env"`
	Cpu             int64     `json:"cpu"`
	Memory          int64     `json:"memory"`
	Gpu             string    `json:"gpu"`
	SourceImage     *string   `json:"source_image"`
	ImageId         string    `json:"image_id"`
	Timestamp       time.Time `json:"timestamp"`
	ScheduleTimeout float64   `json:"schedule_timeout"`
	Mounts          []Mount   `json:"mounts"`
	OnScheduleChan  chan bool `json:"-"`
}

const ContainerExitCodeTtlS int = 300

// Container Stats
const (
	ContainerDurationStatsKey              string        = "beam.worker.usage.container.duration"
	ContainerLifecycleStatsKey             string        = "beam.worker.usage.container.lifecycle"
	ContainerDurationEmissionInterval      time.Duration = 30 * time.Second
	ContainerResourceUsageEmissionInterval time.Duration = 3 * time.Second

	// Container Statuses
	ContainerStatusRequested string = "REQUESTED"
	ContainerStatusScheduled string = "SCHEDULED"
	ContainerStatusStarted   string = "STARTED"
	ContainerStatusStopped   string = "STOPPED"
)
const ContainerStateTtlSWhilePending int = 600
const ContainerStateTtlS int = 60
const IdentityQuotaTtlS int = 600

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
	MinFreeCpu           int64
	MinFreeMemory        int64
	MinFreeGpu           uint
	DefaultWorkerCpu     int64
	DefaultWorkerMemory  int64
	DefaultWorkerGpuType string
}

func NewWorkerPoolSizingConfig() *WorkerPoolSizingConfig {
	return &WorkerPoolSizingConfig{
		MinFreeCpu:           0,
		MinFreeMemory:        0,
		MinFreeGpu:           0,
		DefaultWorkerCpu:     1000,
		DefaultWorkerMemory:  1 * 1024, // 1Gi
		DefaultWorkerGpuType: "",
	}
}

type ThrottledByConcurrencyLimitError struct{}

func (e *ThrottledByConcurrencyLimitError) Error() string {
	return "concurrency_limit_reached"
}

type QuotaDoesNotExistError struct{}

func (e *QuotaDoesNotExistError) Error() string {
	return "quota_does_not_exist"
}
