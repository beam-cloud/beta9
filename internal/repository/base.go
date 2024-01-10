package repository

import (
	"context"
	"time"

	"github.com/beam-cloud/beam/internal/types"
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
	UpdateWorkerCapacity(w *types.Worker, cr *types.ContainerRequest, ut types.CapacityUpdateType) error
	ScheduleContainerRequest(worker *types.Worker, request *types.ContainerRequest) error
	GetNextContainerRequest(workerId string) (*types.ContainerRequest, error)
	AddContainerRequestToWorker(workerId string, containerId string, request *types.ContainerRequest) error
	RemoveContainerRequestFromWorker(workerId string, containerId string) error
	SetContainerResourceValues(workerId string, containerId string, usage types.ContainerResourceUsage) error
}

type ContainerRepository interface {
	GetContainerState(string) (*types.ContainerState, error)
	SetContainerState(string, *types.ContainerState) error
	SetContainerExitCode(string, int) error
	SetContainerAddress(containerId string, addr string) error
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
	CreateObject(ctx context.Context, hash string, size int64, workspaceId uint) (types.Object, error)
	GetObjectByHash(ctx context.Context, hash string, workspaceId uint) (types.Object, error)
	GetObjectByExternalId(ctx context.Context, externalId string, workspaceId uint) (types.Object, error)
	CreateToken(ctx context.Context, workspaceId uint) (types.Token, error)
	AuthorizeToken(ctx context.Context, tokenKey string) (*types.Token, *types.Workspace, error)
	RetrieveActiveToken(ctx context.Context, workspaceId uint) (*types.Token, error)
	GetTask(ctx context.Context, externalId string) (*types.Task, error)
	GetTaskWithRelated(ctx context.Context, externalId string) (*types.TaskWithRelated, error)
	CreateTask(ctx context.Context, containerId string, workspaceId, stubId uint) (*types.Task, error)
	UpdateTask(ctx context.Context, externalId string, updatedTask types.Task) (*types.Task, error)
	DeleteTask(ctx context.Context, externalId string) error
	ListTasks(ctx context.Context) ([]types.Task, error)
	ListTasksWithRelated(ctx context.Context, filters []types.FilterFieldMapping, limit uint32) ([]types.TaskWithRelated, error)
	GetOrCreateStub(ctx context.Context, name, stubType string, config types.StubConfigV1, objectId, workspaceId uint, forceCreate bool) (types.Stub, error)
	GetStubByExternalId(ctx context.Context, externalId string) (*types.StubWithRelated, error)
	GetOrCreateVolume(ctx context.Context, workspaceId uint, name string) (*types.Volume, error)
}

type BeamRepository interface {
	GetAgent(name, identityExternalId string) (*types.Agent, error)
	GetAgentByToken(token string) (*types.Agent, error)
	UpdateAgent(agent *types.Agent) (*types.Agent, error)
	GetDeployments(appId string) ([]types.BeamAppDeployment, error)
	GetDeployment(appId string, version *uint) (*types.BeamAppDeployment, *types.BeamApp, *types.BeamAppDeploymentPackage, error)
	GetDeploymentById(appDeploymentId string) (*types.BeamAppDeployment, *types.BeamApp, *types.BeamAppDeploymentPackage, error)
	GetServe(appId string, serveId string) (*types.BeamAppServe, *types.BeamApp, error)
	EndServe(appId string, serveId string, identityId string) error
	UpdateDeployment(appDeploymentId string, status string, errorMsg string) (*types.BeamAppDeployment, error)
	CreateDeploymentTask(appDeploymentId string, taskId string, taskPolicyRaw []byte) (*types.BeamAppTask, error)
	CreateServeTask(appDeploymentId string, taskId string, taskPolicyRaw []byte) (*types.BeamAppTask, error)
	GetAppTask(taskId string) (*types.BeamAppTask, error)
	UpdateActiveTask(taskId string, status string, identityExternalId string) (*types.BeamAppTask, error)
	DeleteTask(taskId string) error
	GetTotalUsageOfUserMs(identity types.Identity) (totalUsageMs float64, err error)
	RetrieveUserByPk(pk uint) (*types.Identity, error)
	AuthorizeApiKey(clientId string, clientSecret string) (bool, *types.Identity, error)
	AuthorizeApiKeyWithAppId(appId string, clientId string, clientSecret string) (bool, error)
	DeploymentRequiresAuthorization(appId string, appVersion string) (bool, error)
	ServeRequiresAuthorization(appId string, serveId string) (bool, error)
	AuthorizeServiceToServiceToken(token string) (*types.Identity, bool, error)
	GetIdentityQuota(identityId string) (*types.IdentityQuota, error)
}

type WorkerPoolRepository interface {
	GetPool(name string) (*types.WorkerPoolResource, error)
	GetPools() ([]types.WorkerPoolResource, error)
	SetPool(pool *types.WorkerPoolResource) error
	RemovePool(name string) error
}

type MetricsStatsdRepository interface {
	ContainerStarted(containerId string, workerId string)
	ContainerStopped(containerId string, workerId string)
	ContainerRequested(containerId string)
	ContainerScheduled(containerId string)
	ContainerDuration(containerId string, workerId string, timestampNs int64, duration time.Duration)
	BeamDeploymentRequestDuration(bucketName string, duration time.Duration)
	BeamDeploymentRequestStatus(bucketName string, status int)
	BeamDeploymentRequestCount(bucketName string)
	WorkerStarted(workerId string)
	WorkerStopped(workerId string)
	WorkerDuration(workerId string, timestampNs int64, duration time.Duration)
}

type MetricsStreamRepository interface {
	ContainerResourceUsage(usage types.ContainerResourceUsage) error
}
