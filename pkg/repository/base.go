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
	CordonAllPendingWorkersInPool(poolName string) error
	GetAllWorkersOnMachine(machineId string) ([]*types.Worker, error)
	AddWorker(w *types.Worker) error
	ToggleWorkerAvailable(workerId string) error
	UpdateWorkerStatus(workerId string, status types.WorkerStatus) error
	RemoveWorker(workerId string) error
	SetWorkerKeepAlive(workerId string) error
	UpdateWorkerCapacity(w *types.Worker, cr *types.ContainerRequest, ut types.CapacityUpdateType) error
	ScheduleContainerRequest(worker *types.Worker, request *types.ContainerRequest) error
	GetNextContainerRequest(workerId string) (*types.ContainerRequest, error)
	AddContainerToWorker(workerId string, containerId string) error
	RemoveContainerFromWorker(workerId string, containerId string) error
	SetContainerResourceValues(workerId string, containerId string, usage types.ContainerResourceUsage) error
	SetImagePullLock(workerId, imageId string) (string, error)
	RemoveImagePullLock(workerId, imageId, token string) error
	GetContainerIp(networkPrefix string, containerId string) (string, error)
	SetContainerIp(networkPrefix string, containerId, containerIp string) error
	RemoveContainerIp(networkPrefix string, containerId string) error
	GetContainerIps(networkPrefix string) ([]string, error)
	SetNetworkLock(networkPrefix string, ttl, retries int) (string, error)
	RemoveNetworkLock(networkPrefix string, token string) error
	GetGpuCounts() (map[string]int, error)
	GetGpuAvailability() (map[string]bool, error)
	GetFreeGpuCounts() (map[string]int, error)
	GetSpotGpus() []string
}

type ContainerRepository interface {
	GetContainerState(string) (*types.ContainerState, error)
	SetContainerState(string, *types.ContainerState) error
	SetContainerExitCode(string, int) error
	GetContainerExitCode(string) (int, error)
	SetContainerAddress(containerId string, addr string) error
	GetContainerAddress(containerId string) (string, error)
	UpdateContainerStatus(string, types.ContainerStatus, int64) error
	UpdateAssignedContainerGPU(string, string) error
	DeleteContainerState(containerId string) error
	SetContainerRequestStatus(containerId string, status types.ContainerRequestStatus) error
	SetWorkerAddress(containerId string, addr string) error
	GetWorkerAddress(ctx context.Context, containerId string) (string, error)
	SetContainerAddressMap(containerId string, addressMap map[int32]string) error
	GetContainerAddressMap(containerId string) (map[int32]string, error)
	SetContainerStateWithConcurrencyLimit(quota *types.ConcurrencyLimit, request *types.ContainerRequest) error
	GetActiveContainersByStubId(stubId string) ([]types.ContainerState, error)
	GetActiveContainersByWorkspaceId(workspaceId string) ([]types.ContainerState, error)
	GetActiveContainersByWorkerId(workerId string) ([]types.ContainerState, error)
	GetFailedContainersByStubId(stubId string) ([]string, error)
	UpdateCheckpointState(workspaceName, checkpointId string, checkpointState *types.CheckpointState) error
	GetCheckpointState(workspaceName, checkpointId string) (*types.CheckpointState, error)
	GetStubState(stubId string) (string, error)
	SetStubState(stubId, state string) error
	DeleteStubState(stubId string) error
}

type WorkerPoolRepository interface {
	SetWorkerPoolState(ctx context.Context, poolName string, state *types.WorkerPoolState) error
	GetWorkerPoolState(ctx context.Context, poolName string) (*types.WorkerPoolState, error)
	SetWorkerPoolStateLock(poolName string) error
	RemoveWorkerPoolStateLock(poolName string) error
	SetWorkerPoolSizerLock(poolName string) error
	RemoveWorkerPoolSizerLock(poolName string) error
	SetWorkerCleanerLock(poolName string) error
	RemoveWorkerCleanerLock(poolName string) error
}

type WorkspaceRepository interface {
	GetConcurrencyLimitByWorkspaceId(workspaceId string) (*types.ConcurrencyLimit, error)
	SetConcurrencyLimitByWorkspaceId(workspaceId string, limit *types.ConcurrencyLimit) error
	AuthorizeToken(string) (*types.Token, *types.Workspace, error)
	SetAuthorizationToken(*types.Token, *types.Workspace) error
}

type BackendRepository interface {
	ListWorkspaces(ctx context.Context) ([]types.Workspace, error)
	CreateWorkspace(ctx context.Context) (types.Workspace, error)
	GetWorkspaceByExternalId(ctx context.Context, externalId string) (types.Workspace, error)
	GetWorkspaceByExternalIdWithSigningKey(ctx context.Context, externalId string) (types.Workspace, error)
	GetAdminWorkspace(ctx context.Context) (*types.Workspace, error)
	CreateObject(ctx context.Context, hash string, size int64, workspaceId uint) (types.Object, error)
	GetObjectByHash(ctx context.Context, hash string, workspaceId uint) (types.Object, error)
	GetObjectByExternalId(ctx context.Context, externalId string, workspaceId uint) (types.Object, error)
	GetObjectByExternalStubId(ctx context.Context, stubId string, workspaceId uint) (types.Object, error)
	UpdateObjectSizeByExternalId(ctx context.Context, externalId string, size int) error
	DeleteObjectByExternalId(ctx context.Context, externalId string) error
	CreateToken(ctx context.Context, workspaceId uint, tokenType string, reusable bool) (types.Token, error)
	AuthorizeToken(ctx context.Context, tokenKey string) (*types.Token, *types.Workspace, error)
	RetrieveActiveToken(ctx context.Context, workspaceId uint) (*types.Token, error)
	ListTokens(ctx context.Context, workspaceId uint) ([]types.Token, error)
	UpdateTokenAsClusterAdmin(ctx context.Context, tokenId string, disabled bool) error
	ToggleToken(ctx context.Context, workspaceId uint, extTokenId string) (types.Token, error)
	DeleteToken(ctx context.Context, workspaceId uint, extTokenId string) error
	GetTask(ctx context.Context, externalId string) (*types.Task, error)
	GetTaskWithRelated(ctx context.Context, externalId string) (*types.TaskWithRelated, error)
	GetTaskByWorkspace(ctx context.Context, externalId string, workspace *types.Workspace) (*types.TaskWithRelated, error)
	CreateTask(ctx context.Context, params *types.TaskParams) (*types.Task, error)
	UpdateTask(ctx context.Context, externalId string, updatedTask types.Task) (*types.Task, error)
	DeleteTask(ctx context.Context, externalId string) error
	ListTasks(ctx context.Context) ([]types.Task, error)
	ListTasksWithRelated(ctx context.Context, filters types.TaskFilter) ([]types.TaskWithRelated, error)
	ListTasksWithRelatedPaginated(ctx context.Context, filters types.TaskFilter) (common.CursorPaginationInfo[types.TaskWithRelated], error)
	AggregateTasksByTimeWindow(ctx context.Context, filters types.TaskFilter) ([]types.TaskCountByTime, error)
	GetTaskCountPerDeployment(ctx context.Context, filters types.TaskFilter) ([]types.TaskCountPerDeployment, error)
	GetOrCreateStub(ctx context.Context, name, stubType string, config types.StubConfigV1, objectId, workspaceId uint, forceCreate bool) (types.Stub, error)
	UpdateStubConfig(ctx context.Context, stubId uint, config *types.StubConfigV1) error
	GetStubByExternalId(ctx context.Context, externalId string, queryFilters ...types.QueryFilter) (*types.StubWithRelated, error)
	GetDeploymentBySubdomain(ctx context.Context, subdomain string, version uint) (*types.DeploymentWithRelated, error)
	GetVolume(ctx context.Context, workspaceId uint, name string) (*types.Volume, error)
	GetOrCreateVolume(ctx context.Context, workspaceId uint, name string) (*types.Volume, error)
	DeleteVolume(ctx context.Context, workspaceId uint, name string) error
	ListVolumesWithRelated(ctx context.Context, workspaceId uint) ([]types.VolumeWithRelated, error)
	ListDeploymentsWithRelated(ctx context.Context, filters types.DeploymentFilter) ([]types.DeploymentWithRelated, error)
	ListLatestDeploymentsWithRelatedPaginated(ctx context.Context, filters types.DeploymentFilter) (common.CursorPaginationInfo[types.DeploymentWithRelated], error)
	ListDeploymentsPaginated(ctx context.Context, filters types.DeploymentFilter) (common.CursorPaginationInfo[types.DeploymentWithRelated], error)
	GetLatestDeploymentByName(ctx context.Context, workspaceId uint, name string, stubType string, filterDeleted bool) (*types.DeploymentWithRelated, error)
	GetDeploymentByExternalId(ctx context.Context, workspaceId uint, deploymentExternalId string) (*types.DeploymentWithRelated, error)
	GetDeploymentByStubExternalId(ctx context.Context, workspaceId uint, stubExternalId string) (*types.DeploymentWithRelated, error)
	GetDeploymentByNameAndVersion(ctx context.Context, workspaceId uint, name string, version uint, stubType string) (*types.DeploymentWithRelated, error)
	CreateDeployment(ctx context.Context, workspaceId uint, name string, version uint, stubId uint, stubType string) (*types.Deployment, error)
	UpdateDeployment(ctx context.Context, deployment types.Deployment) (*types.Deployment, error)
	DeleteDeployment(ctx context.Context, deployment types.Deployment) error
	ListStubs(ctx context.Context, filters types.StubFilter) ([]types.StubWithRelated, error)
	ListStubsPaginated(ctx context.Context, filters types.StubFilter) (common.CursorPaginationInfo[types.StubWithRelated], error)
	GetConcurrencyLimit(ctx context.Context, concurrenyLimitId uint) (*types.ConcurrencyLimit, error)
	GetConcurrencyLimitByWorkspaceId(ctx context.Context, workspaceId string) (*types.ConcurrencyLimit, error)
	DeleteConcurrencyLimit(ctx context.Context, workspaceId types.Workspace) error
	CreateConcurrencyLimit(ctx context.Context, workspaceId uint, gpuLimit uint32, cpuMillicoreLimit uint32) (*types.ConcurrencyLimit, error)
	UpdateConcurrencyLimit(ctx context.Context, concurrencyLimitId uint, gpuLimit uint32, cpuMillicoreLimit uint32) (*types.ConcurrencyLimit, error)
	CreateSecret(ctx context.Context, workspace *types.Workspace, tokenId uint, name string, value string) (*types.Secret, error)
	GetSecretByName(ctx context.Context, workspace *types.Workspace, name string) (*types.Secret, error)
	GetSecretsByName(ctx context.Context, workspace *types.Workspace, names []string) ([]types.Secret, error)
	GetSecretByNameDecrypted(ctx context.Context, workspace *types.Workspace, name string) (*types.Secret, error)
	GetSecretsByNameDecrypted(ctx context.Context, workspace *types.Workspace, names []string) ([]types.Secret, error)
	ListSecrets(ctx context.Context, workspace *types.Workspace) ([]types.Secret, error)
	UpdateSecret(ctx context.Context, workspace *types.Workspace, tokenId uint, secretId string, value string) (*types.Secret, error)
	DeleteSecret(ctx context.Context, workspace *types.Workspace, secretName string) error
	CreateScheduledJob(ctx context.Context, scheduledJob *types.ScheduledJob) (*types.ScheduledJob, error)
	DeleteScheduledJob(ctx context.Context, scheduledJob *types.ScheduledJob) error
	DeletePreviousScheduledJob(ctx context.Context, deployment *types.Deployment) error
	GetScheduledJob(ctx context.Context, deploymentId uint) (*types.ScheduledJob, error)
	ListenToChannel(ctx context.Context, channel string) (<-chan string, error)
	Ping() error
	GetTaskMetrics(ctx context.Context, periodStart time.Time, periodEnd time.Time) (types.TaskMetrics, error)
}

type TaskRepository interface {
	GetTaskState(ctx context.Context, workspaceName, stubId, taskId string) (*types.TaskMessage, error)
	SetTaskState(ctx context.Context, workspaceName, stubId, taskId string, msg []byte) error
	DeleteTaskState(ctx context.Context, workspaceName, stubId, taskId string) error
	GetTasksInFlight(ctx context.Context) ([]*types.TaskMessage, error)
	ClaimTask(ctx context.Context, workspaceName, stubId, taskId, containerId string) error
	RemoveTaskClaim(ctx context.Context, workspaceName, stubId, taskId string) error
	IsClaimed(ctx context.Context, workspaceName, stubId, taskId string) (bool, error)
	TasksClaimed(ctx context.Context, workspaceName, stubId string) (int, error)
	TasksInFlight(ctx context.Context, workspaceName, stubId string) (int, error)
	SetTaskRetryLock(ctx context.Context, workspaceName, stubId, taskId string) error
	RemoveTaskRetryLock(ctx context.Context, workspaceName, stubId, taskId string) error
}

type ProviderRepository interface {
	GetMachine(providerName, poolName, machineId string) (*types.ProviderMachine, error)
	AddMachine(providerName, poolName, machineId string, machineInfo *types.ProviderMachineState) error
	RemoveMachine(providerName, poolName, machineId string) error
	SetMachineKeepAlive(providerName, poolName, machineId, agentVersion string, metrics *types.ProviderMachineMetrics) error
	SetLastWorkerSeen(providerName, poolName, machineId string) error
	RegisterMachine(providerName, poolName, machineId string, newMachineInfo *types.ProviderMachineState, poolConfig *types.WorkerPoolConfig) error
	WaitForMachineRegistration(providerName, poolName, machineId string) (*types.ProviderMachineState, error)
	ListAllMachines(providerName, poolName string, useLock bool) ([]*types.ProviderMachine, error)
	SetMachineLock(providerName, poolName, machineId string) error
	RemoveMachineLock(providerName, poolName, machineId string) error
	GetGPUAvailability(pools map[string]types.WorkerPoolConfig) (map[string]bool, error)
	GetGPUCounts(pools map[string]types.WorkerPoolConfig) (map[string]int, error)
}

type TailscaleRepository interface {
	GetHostnamesForService(serviceName string) ([]string, error)
	SetHostname(serviceName, serviceId, hostName string) error
}

type EventRepository interface {
	PushContainerRequestedEvent(request *types.ContainerRequest)
	PushContainerScheduledEvent(containerID string, workerID string, request *types.ContainerRequest)
	PushContainerStartedEvent(containerID string, workerID string, request *types.ContainerRequest)
	PushContainerStoppedEvent(containerID string, workerID string, request *types.ContainerRequest)
	PushContainerOOMEvent(containerID string, workerID string, request *types.ContainerRequest)
	PushContainerResourceMetricsEvent(workerID string, request *types.ContainerRequest, metrics types.EventContainerMetricsData)
	PushWorkerStartedEvent(workerID string)
	PushWorkerStoppedEvent(workerID string)
	PushWorkerDeletedEvent(workerID, machineID, poolName string, reason types.DeletedWorkerReason)
	PushDeployStubEvent(workspaceId string, stub *types.Stub)
	PushServeStubEvent(workspaceId string, stub *types.Stub)
	PushRunStubEvent(workspaceId string, stub *types.Stub)
	PushTaskUpdatedEvent(task *types.TaskWithRelated)
	PushTaskCreatedEvent(task *types.TaskWithRelated)
	PushStubStateUnhealthy(workspaceId string, stubId string, currentState, previousState string, reason string, failedContainers []string)
	PushWorkerPoolDegradedEvent(poolName string, reasons []string, poolState *types.WorkerPoolState)
	PushWorkerPoolHealthyEvent(poolName string, poolState *types.WorkerPoolState)
}

type UsageMetricsRepository interface {
	Init(source string) error
	IncrementCounter(name string, metadata map[string]interface{}, value float64) error
	SetGauge(name string, metadata map[string]interface{}, value float64) error
}
