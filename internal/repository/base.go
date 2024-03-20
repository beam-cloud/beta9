package repository

import (
	"context"
	"time"

	"github.com/beam-cloud/beta9/internal/types"
)

type SchedulerRepository interface{}

type WorkerRepository interface {
	GetId() string
	GetWorkerById(workerId string) (*types.Worker, error)
	GetAllWorkers() ([]*types.Worker, error)
	GetAllWorkersInPool(poolId string) ([]*types.Worker, error)
	AddWorker(w *types.Worker) error
	ToggleWorkerAvailable(workerId string) error
	RemoveWorker(w *types.Worker) error
	WorkerKeepAlive(workerId string) error
	UpdateWorkerCapacity(w *types.Worker, cr *types.ContainerRequest, ut types.CapacityUpdateType) error
	ScheduleContainerRequest(worker *types.Worker, request *types.ContainerRequest) error
	GetNextContainerRequest(workerId string) (*types.ContainerRequest, error)
	AddContainerRequestToWorker(workerId string, containerId string, request *types.ContainerRequest) error
	RemoveContainerRequestFromWorker(workerId string, containerId string) error
	SetContainerResourceValues(workerId string, containerId string, usage types.ContainerResourceUsage) error
	SetImagePullLock(workerId, imageId string) error
	RemoveImagePullLock(workerId, imageId string) error
}

type ContainerRepository interface {
	GetContainerState(string) (*types.ContainerState, error)
	SetContainerState(string, *types.ContainerState) error
	SetContainerExitCode(string, int) error
	GetContainerExitCode(string) (int, error)
	SetContainerAddress(containerId string, addr string) error
	GetContainerAddress(containerId string) (string, error)
	UpdateContainerStatus(string, types.ContainerStatus, time.Duration) error
	DeleteContainerState(*types.ContainerRequest) error
	SetContainerWorkerHostname(containerId string, addr string) error
	GetContainerWorkerHostname(containerId string) (string, error)
	GetActiveContainersByPrefix(patternPrefix string) ([]types.ContainerState, error)
	GetFailedContainerCountByPrefix(patternPrefix string) (int, error)
}

type BackendRepository interface {
	ListWorkspaces(ctx context.Context) ([]types.Workspace, error)
	CreateWorkspace(ctx context.Context) (types.Workspace, error)
	GetWorkspaceByExternalId(ctx context.Context, externalId string) (types.Workspace, error)
	CreateObject(ctx context.Context, hash string, size int64, workspaceId uint) (types.Object, error)
	GetObjectByHash(ctx context.Context, hash string, workspaceId uint) (types.Object, error)
	GetObjectByExternalId(ctx context.Context, externalId string, workspaceId uint) (types.Object, error)
	CreateToken(ctx context.Context, workspaceId uint, tokenType string, reusable bool) (types.Token, error)
	AuthorizeToken(ctx context.Context, tokenKey string) (*types.Token, *types.Workspace, error)
	RetrieveActiveToken(ctx context.Context, workspaceId uint) (*types.Token, error)
	ListTokens(ctx context.Context, workspaceId uint) ([]types.Token, error)
	GetTask(ctx context.Context, externalId string) (*types.Task, error)
	GetTaskWithRelated(ctx context.Context, externalId string) (*types.TaskWithRelated, error)
	CreateTask(ctx context.Context, containerId string, workspaceId, stubId uint) (*types.Task, error)
	UpdateTask(ctx context.Context, externalId string, updatedTask types.Task) (*types.Task, error)
	DeleteTask(ctx context.Context, externalId string) error
	ListTasks(ctx context.Context) ([]types.Task, error)
	ListTasksWithRelated(ctx context.Context, filters types.TaskFilter) ([]types.TaskWithRelated, error)
	GetOrCreateStub(ctx context.Context, name, stubType string, config types.StubConfigV1, objectId, workspaceId uint, forceCreate bool) (types.Stub, error)
	GetStubByExternalId(ctx context.Context, externalId string) (*types.StubWithRelated, error)
	GetOrCreateVolume(ctx context.Context, workspaceId uint, name string) (*types.Volume, error)
	ListDeployments(ctx context.Context, filters types.DeploymentFilter) ([]types.DeploymentWithRelated, error)
	GetLatestDeploymentByName(ctx context.Context, workspaceId uint, name string, stubType string) (*types.Deployment, error)
	GetDeploymentByExternalId(ctx context.Context, workspaceId uint, deploymentExternalId string) (*types.DeploymentWithRelated, error)
	GetDeploymentByNameAndVersion(ctx context.Context, workspaceId uint, name string, version uint, stubType string) (*types.DeploymentWithRelated, error)
	CreateDeployment(ctx context.Context, workspaceId uint, name string, version uint, stubId uint, stubType string) (*types.Deployment, error)
}

type WorkerPoolRepository interface {
	GetPool(name string) (*types.WorkerPoolConfig, error)
	GetPools() ([]types.WorkerPoolConfig, error)
	SetPool(name string, pool types.WorkerPoolConfig) error
	RemovePool(name string) error
}

type ProviderRepository interface {
	GetMachine(providerName, poolName, machineId string) (*types.ProviderMachineState, error)
	AddMachine(providerName, poolName, machineId string, info *types.ProviderMachineState) error
	RemoveMachine(providerName, poolName, machineId string) error
	RegisterMachine(providerName, poolName, machineId string, info *types.ProviderMachineState) error
	WaitForMachineRegistration(providerName, poolName, machineId string) (*types.ProviderMachineState, error)
	ListAllMachines(providerName, poolName string) ([]*types.ProviderMachine, error)
	SetMachineLock(providerName, poolName, machineId string) error
	RemoveMachineLock(providerName, poolName, machineId string) error
}

type TailscaleRepository interface {
	GetHostnamesForService(serviceName string) ([]string, error)
	SetHostname(serviceName, serviceId, hostName string) error
}

type EventRepository interface {
	PushContainerRequestedEvent(request *types.ContainerRequest)
	PushContainerScheduledEvent(containerID string, workerID string)
	PushContainerStartedEvent(containerID string, workerID string)
	PushContainerStoppedEvent(containerID string, workerID string)
	PushWorkerStartedEvent(workerID string)
	PushWorkerStoppedEvent(workerID string)
}

type MetricsRepository interface {
	Init(source string) error
	IncrementCounter(name string, metadata map[string]interface{}, value float64) error
	SetGauge(name string, metadata map[string]interface{}, value float64) error
}
