package repository

import (
	"context"
	"time"

	"github.com/beam-cloud/beta9/pkg/repository/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

type SchedulerRepository interface{}

type WorkerRepository interface {
	GetId() string
	GetWorkerById(workerId string) (*types.Worker, error)
	GetAllWorkers() ([]*types.Worker, error)
	GetAllWorkersInPool(poolName string) ([]*types.Worker, error)
	GetAllWorkersOnMachine(machineId string) ([]*types.Worker, error)
	AddWorker(w *types.Worker) error
	ToggleWorkerAvailable(workerId string) error
	RemoveWorker(w *types.Worker) error
	SetWorkerKeepAlive(workerId string) error
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
	SetWorkerAddress(containerId string, addr string) error
	SetContainerStateWithConcurrencyLimit(quota *types.ConcurrencyLimit, request *types.ContainerRequest) error
	GetWorkerAddress(containerId string) (string, error)
	GetActiveContainersByStubId(stubId string) ([]types.ContainerState, error)
	GetActiveContainersByWorkspaceId(workspaceId string) ([]types.ContainerState, error)
	GetFailedContainerCountByStubId(stubId string) (int, error)
}

type WorkspaceRepository interface {
	GetConcurrencyLimitByWorkspaceId(workspaceId string) (*types.ConcurrencyLimit, error)
	SetConcurrencyLimitByWorkspaceId(workspaceId string, limit *types.ConcurrencyLimit) error
}

type BackendRepository interface {
	ListWorkspaces(ctx context.Context) ([]types.Workspace, error)
	CreateWorkspace(ctx context.Context) (types.Workspace, error)
	GetWorkspaceByExternalId(ctx context.Context, externalId string) (types.Workspace, error)
	GetWorkspaceByExternalIdWithSigningKey(ctx context.Context, externalId string) (types.Workspace, error)
	CreateObject(ctx context.Context, hash string, size int64, workspaceId uint) (types.Object, error)
	GetObjectByHash(ctx context.Context, hash string, workspaceId uint) (types.Object, error)
	GetObjectByExternalId(ctx context.Context, externalId string, workspaceId uint) (types.Object, error)
	UpdateObjectSizeByExternalId(ctx context.Context, externalId string, size int) error
	DeleteObjectByExternalId(ctx context.Context, externalId string) error
	CreateToken(ctx context.Context, workspaceId uint, tokenType string, reusable bool) (types.Token, error)
	AuthorizeToken(ctx context.Context, tokenKey string) (*types.Token, *types.Workspace, error)
	RetrieveActiveToken(ctx context.Context, workspaceId uint) (*types.Token, error)
	ListTokens(ctx context.Context, workspaceId uint) ([]types.Token, error)
	UpdateTokenAsClusterAdmin(ctx context.Context, tokenId string, disabled bool) error
	GetTask(ctx context.Context, externalId string) (*types.Task, error)
	GetTaskWithRelated(ctx context.Context, externalId string) (*types.TaskWithRelated, error)
	CreateTask(ctx context.Context, params *types.TaskParams) (*types.Task, error)
	UpdateTask(ctx context.Context, externalId string, updatedTask types.Task) (*types.Task, error)
	DeleteTask(ctx context.Context, externalId string) error
	ListTasks(ctx context.Context) ([]types.Task, error)
	ListTasksWithRelated(ctx context.Context, filters types.TaskFilter) ([]types.TaskWithRelated, error)
	ListTasksWithRelatedPaginated(ctx context.Context, filters types.TaskFilter) (common.CursorPaginationInfo[types.TaskWithRelated], error)
	AggregateTasksByTimeWindow(ctx context.Context, filters types.TaskFilter) ([]types.TaskCountByTime, error)
	GetTaskCountPerDeployment(ctx context.Context, filters types.TaskFilter) ([]types.TaskCountPerDeployment, error)
	GetOrCreateStub(ctx context.Context, name, stubType string, config types.StubConfigV1, objectId, workspaceId uint, forceCreate bool) (types.Stub, error)
	GetStubByExternalId(ctx context.Context, externalId string) (*types.StubWithRelated, error)
	GetVolume(ctx context.Context, workspaceId uint, name string) (*types.Volume, error)
	GetOrCreateVolume(ctx context.Context, workspaceId uint, name string) (*types.Volume, error)
	ListVolumesWithRelated(ctx context.Context, workspaceId uint) ([]types.VolumeWithRelated, error)
	ListDeploymentsWithRelated(ctx context.Context, filters types.DeploymentFilter) ([]types.DeploymentWithRelated, error)
	ListDeploymentsPaginated(ctx context.Context, filters types.DeploymentFilter) (common.CursorPaginationInfo[types.DeploymentWithRelated], error)
	GetLatestDeploymentByName(ctx context.Context, workspaceId uint, name string, stubType string) (*types.Deployment, error)
	GetDeploymentByExternalId(ctx context.Context, workspaceId uint, deploymentExternalId string) (*types.DeploymentWithRelated, error)
	GetDeploymentByNameAndVersion(ctx context.Context, workspaceId uint, name string, version uint, stubType string) (*types.DeploymentWithRelated, error)
	CreateDeployment(ctx context.Context, workspaceId uint, name string, version uint, stubId uint, stubType string) (*types.Deployment, error)
	UpdateDeployment(ctx context.Context, deployment types.Deployment) (*types.Deployment, error)
	ListStubs(ctx context.Context, filters types.StubFilter) ([]types.StubWithRelated, error)
	GetConcurrencyLimit(ctx context.Context, concurrenyLimitId uint) (*types.ConcurrencyLimit, error)
	GetConcurrencyLimitByWorkspaceId(ctx context.Context, workspaceId string) (*types.ConcurrencyLimit, error)
	DeleteConcurrencyLimit(ctx context.Context, workspaceId types.Workspace) error
	CreateConcurrencyLimit(ctx context.Context, workspaceId uint, gpuLimit uint32, cpuMillicoreLimit uint32) (*types.ConcurrencyLimit, error)
	UpdateConcurrencyLimit(ctx context.Context, concurrencyLimitId uint, gpuLimit uint32, cpuMillicoreLimit uint32) (*types.ConcurrencyLimit, error)
	CreateSecret(ctx context.Context, workspace *types.Workspace, tokenId uint, name string, value string) (*types.Secret, error)
	GetSecretByName(ctx context.Context, workspace *types.Workspace, name string) (*types.Secret, error)
	GetSecretByNameDecrypted(ctx context.Context, workspace *types.Workspace, name string) (*types.Secret, error)
	ListSecrets(ctx context.Context, workspace *types.Workspace) ([]types.Secret, error)
	UpdateSecret(ctx context.Context, workspace *types.Workspace, tokenId uint, secretId string, value string) (*types.Secret, error)
	DeleteSecret(ctx context.Context, workspace *types.Workspace, secretName string) error
}

type WorkerPoolRepository interface {
	GetPool(name string) (*types.WorkerPoolConfig, error)
	GetPools() ([]types.WorkerPoolConfig, error)
	SetPool(name string, pool types.WorkerPoolConfig) error
	RemovePool(name string) error
	SetPoolLock(name string) error
	RemovePoolLock(name string) error
}

type TaskRepository interface {
	SetTaskState(ctx context.Context, workspaceName, stubId, taskId string, msg []byte) error
	DeleteTaskState(ctx context.Context, workspaceName, stubId, taskId string) error
	GetTasksInFlight(ctx context.Context) ([]*types.TaskMessage, error)
	ClaimTask(ctx context.Context, workspaceName, stubId, taskId, containerId string) error
	IsClaimed(ctx context.Context, workspaceName, stubId, taskId string) (bool, error)
	TasksClaimed(ctx context.Context, workspaceName, stubId string) (int, error)
	TasksInFlight(ctx context.Context, workspaceName, stubId string) (int, error)
}

type ProviderRepository interface {
	GetMachine(providerName, poolName, machineId string) (*types.ProviderMachineState, error)
	AddMachine(providerName, poolName, machineId string, machineInfo *types.ProviderMachineState) error
	RemoveMachine(providerName, poolName, machineId string) error
	SetMachineKeepAlive(providerName, poolName, machineId string) error
	SetLastWorkerSeen(providerName, poolName, machineId string) error
	RegisterMachine(providerName, poolName, machineId string, newMachineInfo *types.ProviderMachineState) error
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
