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
	DefaultServeContainerTimeout = time.Minute * 10
	DefaultCPUWorkerPoolName     = "default"
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

type ContainerRequestStatus string

const (
	ContainerRequestStatusFailed ContainerRequestStatus = "failed"
	ContainerRequestStatusTTL                           = 10 * time.Minute
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
	StartedAt   int64           `redis:"started_at" json:"started_at"`
}

// @go2proto
type Container struct {
	ContainerId  string          `redis:"container_id" json:"container_id"`
	StubId       string          `redis:"stub_id" json:"stub_id"`
	Status       ContainerStatus `redis:"status" json:"status"`
	ScheduledAt  time.Time       `redis:"scheduled_at" json:"scheduled_at"`
	StartedAt    time.Time       `redis:"started_at" json:"started_at"`
	WorkspaceId  string          `redis:"workspace_id" json:"workspace_id"`
	WorkerId     string          `redis:"worker_id" json:"worker_id"`
	MachineId    string          `redis:"machine_id" json:"machine_id"`
	DeploymentId string          `redis:"deployment_id" json:"deployment_id"`
}

func (c *Container) ToProto() *pb.Container {
	return &pb.Container{
		ContainerId:  c.ContainerId,
		StubId:       c.StubId,
		Status:       string(c.Status),
		ScheduledAt:  timestamppb.New(c.ScheduledAt),
		StartedAt:    timestamppb.New(c.StartedAt),
		WorkspaceId:  c.WorkspaceId,
		WorkerId:     c.WorkerId,
		MachineId:    c.MachineId,
		DeploymentId: c.DeploymentId,
	}
}

func NewContainerFromProto(in *pb.Container) *Container {
	return &Container{
		ContainerId: in.ContainerId,
		StubId:      in.StubId,
		Status:      ContainerStatus(in.Status),
		ScheduledAt: in.ScheduledAt.AsTime(),
		StartedAt:   in.StartedAt.AsTime(),
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
	ContainerId              string          `json:"container_id"`
	EntryPoint               []string        `json:"entry_point"`
	Env                      []string        `json:"env"`
	Cpu                      int64           `json:"cpu"`
	Memory                   int64           `json:"memory"`
	Gpu                      string          `json:"gpu"`
	GpuRequest               []string        `json:"gpu_request"`
	GpuCount                 uint32          `json:"gpu_count"`
	ImageId                  string          `json:"image_id"`
	StubId                   string          `json:"stub_id"`
	WorkspaceId              string          `json:"workspace_id"`
	Workspace                Workspace       `json:"workspace"`
	Stub                     StubWithRelated `json:"stub"`
	Timestamp                time.Time       `json:"timestamp"`
	Mounts                   []Mount         `json:"mounts"`
	RetryCount               int             `json:"retry_count"`
	PoolSelector             string          `json:"pool_selector"`
	Preemptable              bool            `json:"preemptable"`
	CheckpointEnabled        bool            `json:"checkpoint_enabled"`
	BuildOptions             BuildOptions    `json:"build_options"`
	Ports                    []uint32        `json:"ports"`
	CostPerMs                float64         `json:"cost_per_ms"`
	AppId                    string          `json:"app_id"`
	Checkpoint               *Checkpoint     `json:"checkpoint"`
	ConfigPath               string          `json:"config_path"`
	ImageCredentials         string          `json:"image_credentials"`
	BuildRegistryCredentials string          `json:"build_registry_credentials"`
}

func (c *ContainerRequest) RequiresGPU() bool {
	return len(c.GpuRequest) > 0 || c.Gpu != ""
}

// IsBuildRequest checks if the sourceImage or Dockerfile field is not-nil, which means the container request is for a build container
func (c *ContainerRequest) IsBuildRequest() bool {
	return c.BuildOptions.SourceImage != nil || c.BuildOptions.Dockerfile != nil
}

func (c *ContainerRequest) VolumeCacheCompatible() bool {
	if c.IsBuildRequest() || c.CheckpointEnabled || c.StorageAvailable() {
		return false
	}
	return c.Workspace.VolumeCacheEnabled
}

func (c *ContainerRequest) StorageAvailable() bool {
	return c.Workspace.StorageAvailable()
}

func (c *ContainerRequest) ToProto() *pb.ContainerRequest {
	mounts := make([]*pb.Mount, len(c.Mounts))
	for i, m := range c.Mounts {
		mounts[i] = m.ToProto()
	}

	var buildOptions *pb.BuildOptions
	if c.IsBuildRequest() {
		sourceImage := getStringOrDefault(c.BuildOptions.SourceImage)
		dockerfile := getStringOrDefault(c.BuildOptions.Dockerfile)
		buildCtxObject := getStringOrDefault(c.BuildOptions.BuildCtxObject)

		buildOptions = &pb.BuildOptions{
			SourceImage:      sourceImage,
			Dockerfile:       dockerfile,
			BuildCtxObject:   buildCtxObject,
			SourceImageCreds: c.BuildOptions.SourceImageCreds,
			BuildSecrets:     c.BuildOptions.BuildSecrets,
		}
	}

	var checkpoint *pb.Checkpoint
	if c.Checkpoint != nil {
		checkpoint = c.Checkpoint.ToProto()
	}

	return &pb.ContainerRequest{
		ContainerId:              c.ContainerId,
		EntryPoint:               c.EntryPoint,
		Env:                      c.Env,
		Cpu:                      c.Cpu,
		Memory:                   c.Memory,
		Gpu:                      c.Gpu,
		GpuRequest:               c.GpuRequest,
		GpuCount:                 c.GpuCount,
		ImageId:                  c.ImageId,
		Mounts:                   mounts,
		StubId:                   c.StubId,
		AppId:                    c.AppId,
		WorkspaceId:              c.WorkspaceId,
		Workspace:                c.Workspace.ToProto(),
		Stub:                     c.Stub.ToProto(),
		RetryCount:               int64(c.RetryCount),
		PoolSelector:             c.PoolSelector,
		Preemptable:              c.Preemptable,
		Timestamp:                timestamppb.New(c.Timestamp),
		BuildOptions:             buildOptions,
		ImageCredentials:         c.ImageCredentials,
		Ports:                    c.Ports,
		CheckpointEnabled:        c.CheckpointEnabled,
		Checkpoint:               checkpoint,
		BuildRegistryCredentials: c.BuildRegistryCredentials,
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
			SourceImage:      getPointerOrNil(in.BuildOptions.SourceImage),
			Dockerfile:       getPointerOrNil(in.BuildOptions.Dockerfile),
			BuildCtxObject:   getPointerOrNil(in.BuildOptions.BuildCtxObject),
			SourceImageCreds: in.BuildOptions.SourceImageCreds,
			BuildSecrets:     in.BuildOptions.BuildSecrets,
		}
	}

	var checkpoint *Checkpoint
	if in.Checkpoint != nil {
		checkpoint = NewCheckpointFromProto(in.Checkpoint)
	}

	return &ContainerRequest{
		ContainerId:              in.ContainerId,
		EntryPoint:               in.EntryPoint,
		Env:                      in.Env,
		Cpu:                      in.Cpu,
		Memory:                   in.Memory,
		Gpu:                      in.Gpu,
		GpuRequest:               in.GpuRequest,
		GpuCount:                 in.GpuCount,
		ImageId:                  in.ImageId,
		Mounts:                   mounts,
		WorkspaceId:              in.WorkspaceId,
		AppId:                    in.AppId,
		Workspace:                *NewWorkspaceFromProto(in.Workspace),
		Stub:                     *NewStubWithRelatedFromProto(in.Stub),
		StubId:                   in.StubId,
		Timestamp:                in.GetTimestamp().AsTime(),
		CheckpointEnabled:        in.CheckpointEnabled,
		Preemptable:              in.Preemptable,
		PoolSelector:             in.PoolSelector,
		BuildOptions:             bo,
		ImageCredentials:         in.ImageCredentials,
		Ports:                    in.Ports,
		Checkpoint:               checkpoint,
		BuildRegistryCredentials: in.BuildRegistryCredentials,
	}
}

func getPointerOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func getStringOrDefault(s *string) string {
	if s != nil {
		return *s
	}
	return ""
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

const workerPoolStateNotFoundPrefix = "worker pool state not found: "

type ErrWorkerPoolStateNotFound struct {
	PoolName string
}

func (e *ErrWorkerPoolStateNotFound) Error() string {
	return fmt.Sprintf("%s%s", workerPoolStateNotFoundPrefix, e.PoolName)
}

func (e *ErrWorkerPoolStateNotFound) From(err error) bool {
	if err == nil {
		return false
	}

	if strings.HasPrefix(err.Error(), workerPoolStateNotFoundPrefix) {
		e.PoolName = strings.TrimPrefix(err.Error(), workerPoolStateNotFoundPrefix)
		return true
	}

	return false
}

// @go2proto
type WorkerPoolState struct {
	Status             WorkerPoolStatus `redis:"status" json:"status"`
	SchedulingLatency  int64            `redis:"scheduling_latency" json:"scheduling_latency"`
	FreeGpu            uint             `redis:"free_gpu" json:"free_gpu"`
	FreeCpu            int64            `redis:"free_cpu" json:"free_cpu"`
	FreeMemory         int64            `redis:"free_memory" json:"free_memory"`
	PendingWorkers     int64            `redis:"pending_workers" json:"pending_workers"`
	AvailableWorkers   int64            `redis:"available_workers" json:"available_workers"`
	PendingContainers  int64            `redis:"pending_containers" json:"pending_containers"`
	RunningContainers  int64            `redis:"running_containers" json:"running_containers"`
	RegisteredMachines int64            `redis:"registered_machines" json:"registered_machines"`
	PendingMachines    int64            `redis:"pending_machines" json:"pending_machines"`
	ReadyMachines      int64            `redis:"ready_machines" json:"ready_machines"`
}

func (w *WorkerPoolState) ToProto() *pb.WorkerPoolState {
	return &pb.WorkerPoolState{
		Status:             string(w.Status),
		SchedulingLatency:  w.SchedulingLatency,
		FreeGpu:            uint32(w.FreeGpu),
		FreeCpu:            w.FreeCpu,
		FreeMemory:         w.FreeMemory,
		PendingWorkers:     w.PendingWorkers,
		AvailableWorkers:   w.AvailableWorkers,
		PendingContainers:  w.PendingContainers,
		RunningContainers:  w.RunningContainers,
		RegisteredMachines: w.RegisteredMachines,
		PendingMachines:    w.PendingMachines,
	}
}

type WorkerPoolStatus string

const (
	WorkerPoolStatusHealthy  WorkerPoolStatus = "HEALTHY"
	WorkerPoolStatusDegraded WorkerPoolStatus = "DEGRADED"
)

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

type ErrCheckpointNotFound struct {
	CheckpointId string
}

func (e *ErrCheckpointNotFound) Error() string {
	return fmt.Sprintf("checkpoint state not found: %s", e.CheckpointId)
}

type StopContainerReason string

const (
	// StopContainerReasonTtl is used when a container is stopped due to some TTL expiration
	StopContainerReasonTtl StopContainerReason = "TTL"
	// StopContainerReasonUser is used when a container is stopped by a user request
	StopContainerReasonUser StopContainerReason = "USER"
	// StopContainerReasonScheduler is used when a container is stopped by the scheduler
	StopContainerReasonScheduler StopContainerReason = "SCHEDULER"
	// StopContainerReasonAdmin is used when a container is stopped by an admin request (i.e. draining a worker)
	StopContainerReasonAdmin StopContainerReason = "ADMIN"

	StopContainerReasonUnknown StopContainerReason = "UNKNOWN"
)

type StopContainerArgs struct {
	ContainerId string              `json:"container_id"`
	Force       bool                `json:"force"`
	Reason      StopContainerReason `json:"reason"`
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
