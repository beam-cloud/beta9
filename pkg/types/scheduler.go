package types

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	DefaultCPUWorkerPoolName = "default"
)

type WorkerStatus string

const (
	WorkerStatusAvailable WorkerStatus = "available"
	WorkerStatusPending   WorkerStatus = "pending"
	WorkerStatusDisabled  WorkerStatus = "disabled"
	WorkerStateTtlS       int          = 60
)

const (
	StubStateDegraded = "degraded"
	StubStateWarning  = "warning"
	StubStateHealthy  = "healthy"
)

// @go2proto
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
	Preemptable          bool         `json:"preemptable" redis:"preemptable"`
	BuildVersion         string       `json:"build_version" redis:"build_version"`
	ActiveContainers     []Container  `json:"active_containers" redis:"active_containers"`
}

func (w *Worker) ToProto() *pb.Worker {
	containers := make([]*pb.Container, len(w.ActiveContainers))
	for i, c := range w.ActiveContainers {
		containers[i] = c.ToProto()
	}

	return &pb.Worker{
		Id:                   w.Id,
		Status:               string(w.Status),
		TotalCpu:             w.TotalCpu,
		TotalMemory:          w.TotalMemory,
		TotalGpuCount:        w.TotalGpuCount,
		FreeCpu:              w.FreeCpu,
		FreeMemory:           w.FreeMemory,
		FreeGpuCount:         w.FreeGpuCount,
		Gpu:                  w.Gpu,
		PoolName:             w.PoolName,
		MachineId:            w.MachineId,
		ResourceVersion:      w.ResourceVersion,
		RequiresPoolSelector: w.RequiresPoolSelector,
		Priority:             w.Priority,
		Preemptable:          w.Preemptable,
		BuildVersion:         w.BuildVersion,
		ActiveContainers:     containers,
	}
}

func NewWorkerFromProto(in *pb.Worker) *Worker {
	containers := make([]Container, len(in.ActiveContainers))
	for i, c := range in.ActiveContainers {
		containers[i] = *NewContainerFromProto(c)
	}

	return &Worker{
		Id:                   in.Id,
		Status:               WorkerStatus(in.Status),
		TotalCpu:             in.TotalCpu,
		TotalMemory:          in.TotalMemory,
		TotalGpuCount:        in.TotalGpuCount,
		FreeCpu:              in.FreeCpu,
		FreeMemory:           in.FreeMemory,
		FreeGpuCount:         in.FreeGpuCount,
		Gpu:                  in.Gpu,
		PoolName:             in.PoolName,
		MachineId:            in.MachineId,
		ResourceVersion:      in.ResourceVersion,
		RequiresPoolSelector: in.RequiresPoolSelector,
		Priority:             in.Priority,
		Preemptable:          in.Preemptable,
		BuildVersion:         in.BuildVersion,
		ActiveContainers:     containers,
	}
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

// @go2proto
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

// @go2proto
type Container struct {
	ContainerId string          `redis:"container_id" json:"container_id"`
	StubId      string          `redis:"stub_id" json:"stub_id"`
	Status      ContainerStatus `redis:"status" json:"status"`
	ScheduledAt int64           `redis:"scheduled_at" json:"scheduled_at"`
	WorkspaceId string          `redis:"workspace_id" json:"workspace_id"`
	WorkerId    string          `redis:"worker_id" json:"worker_id"`
	MachineId   string          `redis:"machine_id" json:"machine_id"`
}

func (c *Container) ToProto() *pb.Container {
	return &pb.Container{
		ContainerId: c.ContainerId,
		StubId:      c.StubId,
		Status:      string(c.Status),
		ScheduledAt: c.ScheduledAt,
		WorkspaceId: c.WorkspaceId,
		WorkerId:    c.WorkerId,
		MachineId:   c.MachineId,
	}
}

func NewContainerFromProto(in *pb.Container) *Container {
	return &Container{
		ContainerId: in.ContainerId,
		StubId:      in.StubId,
		Status:      ContainerStatus(in.Status),
		ScheduledAt: in.ScheduledAt,
		WorkspaceId: in.WorkspaceId,
		WorkerId:    in.WorkerId,
		MachineId:   in.MachineId,
	}
}

// @go2proto
type BuildOptions struct {
	SourceImage      *string  `json:"source_image"`
	Dockerfile       *string  `json:"dockerfile"`
	BuildCtxObject   *string  `json:"build_context"`
	SourceImageCreds string   `json:"source_image_creds"`
	BuildSecrets     []string `json:"build_secrets"`
}

// @go2proto
type ContainerRequest struct {
	ContainerId       string          `json:"container_id"`
	EntryPoint        []string        `json:"entry_point"`
	Env               []string        `json:"env"`
	Cpu               int64           `json:"cpu"`
	Memory            int64           `json:"memory"`
	Gpu               string          `json:"gpu"`
	GpuRequest        []string        `json:"gpu_request"`
	GpuCount          uint32          `json:"gpu_count"`
	ImageId           string          `json:"image_id"`
	StubId            string          `json:"stub_id"`
	WorkspaceId       string          `json:"workspace_id"`
	Workspace         Workspace       `json:"workspace"`
	Stub              StubWithRelated `json:"stub"`
	Timestamp         time.Time       `json:"timestamp"`
	Mounts            []Mount         `json:"mounts"`
	RetryCount        int             `json:"retry_count"`
	PoolSelector      string          `json:"pool_selector"`
	Preemptable       bool            `json:"preemptable"`
	CheckpointEnabled bool            `json:"checkpoint_enabled"`
	BuildOptions      BuildOptions    `json:"build_options"`
}

func (c *ContainerRequest) RequiresGPU() bool {
	return len(c.GpuRequest) > 0 || c.Gpu != ""
}

// IsBuildRequest checks if the sourceImage or Dockerfile field is not-nil, which means the container request is for a build container
func (c *ContainerRequest) IsBuildRequest() bool {
	return c.BuildOptions.SourceImage != nil || c.BuildOptions.Dockerfile != nil
}

func (c *ContainerRequest) ToProto() *pb.ContainerRequest {
	mounts := make([]*pb.Mount, len(c.Mounts))
	for i, m := range c.Mounts {
		mounts[i] = m.ToProto()
	}

	var buildOptions *pb.BuildOptions
	if c.IsBuildRequest() {
		buildOptions = &pb.BuildOptions{
			SourceImage:      *c.BuildOptions.SourceImage,
			Dockerfile:       *c.BuildOptions.Dockerfile,
			BuildCtxObject:   *c.BuildOptions.BuildCtxObject,
			SourceImageCreds: c.BuildOptions.SourceImageCreds,
			BuildSecrets:     c.BuildOptions.BuildSecrets,
		}
	}

	return &pb.ContainerRequest{
		ContainerId: c.ContainerId,
		EntryPoint:  c.EntryPoint,
		Env:         c.Env,
		Cpu:         c.Cpu,
		Memory:      c.Memory,
		Gpu:         c.Gpu,
		GpuRequest:  c.GpuRequest,
		GpuCount:    c.GpuCount,
		ImageId:     c.ImageId,
		Mounts:      mounts,
		StubId:      c.StubId,
		WorkspaceId: c.WorkspaceId,
		Workspace: &pb.Workspace{
			Name: c.Workspace.Name,
		},
		Stub: &pb.StubWithRelated{
			Stub: &pb.Stub{
				Id: uint32(c.Stub.Stub.Id),
			},
			Workspace: &pb.Workspace{
				Name: c.Stub.Workspace.Name,
				Id:   uint32(c.Stub.Workspace.Id),
			},
			Object: &pb.Object{
				Id: uint32(c.Stub.Object.Id),
			},
		},
		RetryCount:        int64(c.RetryCount),
		PoolSelector:      c.PoolSelector,
		Preemptable:       c.Preemptable,
		CheckpointEnabled: c.CheckpointEnabled,
		Timestamp:         timestamppb.New(c.Timestamp),
		BuildOptions:      buildOptions,
	}
}

func NewContainerRequestFromProto(in *pb.ContainerRequest) *ContainerRequest {
	mounts := make([]Mount, len(in.Mounts))
	for i, m := range in.Mounts {
		mounts[i] = *NewMountFromProto(m)
	}

	var bo BuildOptions
	if in.BuildOptions != nil {
		bo = BuildOptions{
			SourceImage:      &in.BuildOptions.SourceImage,
			Dockerfile:       &in.BuildOptions.Dockerfile,
			BuildCtxObject:   &in.BuildOptions.BuildCtxObject,
			SourceImageCreds: in.BuildOptions.SourceImageCreds,
			BuildSecrets:     in.BuildOptions.BuildSecrets,
		}
	}

	return &ContainerRequest{
		ContainerId: in.ContainerId,
		EntryPoint:  in.EntryPoint,
		Env:         in.Env,
		Cpu:         in.Cpu,
		Memory:      in.Memory,
		Gpu:         in.Gpu,
		GpuRequest:  in.GpuRequest,
		GpuCount:    in.GpuCount,
		ImageId:     in.ImageId,
		Mounts:      mounts,
		WorkspaceId: in.WorkspaceId,
		Workspace: Workspace{
			Name: in.Workspace.GetName(),
		},
		StubId:            in.StubId,
		Timestamp:         in.GetTimestamp().AsTime(),
		CheckpointEnabled: in.CheckpointEnabled,
		Preemptable:       in.Preemptable,
		PoolSelector:      in.PoolSelector,
		BuildOptions:      bo,
	}
}

const ContainerExitCodeTtlS int = 300

const (
	ContainerDurationEmissionInterval      time.Duration = 5 * time.Second
	ContainerResourceUsageEmissionInterval time.Duration = 3 * time.Second
)
const ContainerStateTtlSWhilePending int64 = 600
const ContainerStateTtlS int64 = 120
const WorkspaceQuotaTtlS int64 = 600

const containerStateNotFoundPrefix = "container state not found: "

type ErrContainerStateNotFound struct {
	ContainerId string
}

func (e *ErrContainerStateNotFound) Error() string {
	return fmt.Sprintf("%s%s", containerStateNotFoundPrefix, e.ContainerId)
}

func (e *ErrContainerStateNotFound) From(err error) bool {
	if err == nil {
		return false
	}

	if strings.HasPrefix(err.Error(), containerStateNotFoundPrefix) {
		e.ContainerId = strings.TrimPrefix(err.Error(), containerStateNotFoundPrefix)
		return true
	}

	return false
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

const workerNotFoundPrefix = "worker not found: "

type ErrWorkerNotFound struct {
	WorkerId string
}

func (e *ErrWorkerNotFound) Error() string {
	return fmt.Sprintf("%s%s", workerNotFoundPrefix, e.WorkerId)
}

func (e *ErrWorkerNotFound) From(err error) bool {
	if err == nil {
		return false
	}

	if strings.HasPrefix(err.Error(), workerNotFoundPrefix) {
		e.WorkerId = strings.TrimPrefix(err.Error(), workerNotFoundPrefix)
		return true
	}

	return false
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

type CheckpointStatus string

const (
	CheckpointStatusAvailable        CheckpointStatus = "available"
	CheckpointStatusCheckpointFailed CheckpointStatus = "checkpoint_failed"
	CheckpointStatusRestoreFailed    CheckpointStatus = "restore_failed"
	CheckpointStatusNotFound         CheckpointStatus = "not_found"
)

// @go2proto
type CheckpointState struct {
	StubId      string           `redis:"stub_id" json:"stub_id"`
	ContainerId string           `redis:"container_id" json:"container_id"`
	Status      CheckpointStatus `redis:"status" json:"status"`
	RemoteKey   string           `redis:"remote_key" json:"remote_key"`
}

type ErrCheckpointNotFound struct {
	CheckpointId string
}

func (e *ErrCheckpointNotFound) Error() string {
	return fmt.Sprintf("checkpoint state not found: %s", e.CheckpointId)
}

type StopContainerArgs struct {
	ContainerId string `json:"container_id"`
	Force       bool   `json:"force"`
}

func (a StopContainerArgs) ToMap() (map[string]any, error) {
	data, err := json.Marshal(a)
	if err != nil {
		return nil, err
	}

	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}

	return result, nil
}

func ToStopContainerArgs(m map[string]any) (*StopContainerArgs, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}

	var result StopContainerArgs
	if err = json.Unmarshal(data, &result); err != nil {
		return nil, err
	}

	return &result, nil
}
