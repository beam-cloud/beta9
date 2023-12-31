package repository

import (
	"context"
	"time"

	"github.com/beam-cloud/beam/internal/types"
	pb "github.com/beam-cloud/beam/proto"
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
}

type BackendRepository interface {
	ListContexts(ctx context.Context) ([]types.Context, error)
	CreateContext(ctx context.Context) (types.Context, error)
	CreateObject(ctx context.Context, hash string, size int64, contextId uint) (types.Object, error)
	GetObjectByHash(ctx context.Context, hash string, contextId uint) (types.Object, error)
	GetObjectByExternalId(ctx context.Context, externalId string, contextId uint) (types.Object, error)
	CreateToken(ctx context.Context, contextId uint) (types.Token, error)
	AuthorizeToken(ctx context.Context, tokenKey string) (*types.Token, *types.Context, error)
	CreateTask(ctx context.Context, containerId string, contextId, stubId uint) (*types.Task, error)
	UpdateTask(ctx context.Context, externalId string, updatedTask types.Task) (*types.Task, error)
	DeleteTask(ctx context.Context, externalId string) error
	ListTasks(ctx context.Context) ([]types.Task, error)
	GetOrCreateStub(ctx context.Context, name, stubType string, config types.StubConfigV1, objectId, contextId uint) (types.Stub, error)
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

type TaskRepository interface {
	StartTask(taskId, queueName, containerId, identityExternalId string) error
	EndTask(taskId, queueName, containerId, containerHostname, identityExternalId string, taskDuration, scaleDownDelay float64) error
	MonitorTask(task *types.BeamAppTask, queueName, containerId, identityExternalId string, timeout int64, stream pb.GatewayService_MonitorTaskServer, timeoutCallback func() error) error
	GetTaskStream(queueName, containerId, identityExternalId string, stream pb.GatewayService_GetTaskStreamServer) error
	GetNextTask(queueName, containerId, identityExternalId string) ([]byte, error)
	GetTasksInFlight(queueName, identityExternalId string) (int, error)
	IncrementTasksInFlight(queueName, identityExternalId string) error
	DecrementTasksInFlight(queueName, identityExternalId string) error
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
