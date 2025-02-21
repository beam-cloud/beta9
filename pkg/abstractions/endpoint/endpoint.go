package endpoint

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"

	"github.com/labstack/echo/v4"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/task"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type EndpointService interface {
	pb.EndpointServiceServer
	StartEndpointServe(in *pb.StartEndpointServeRequest, stream pb.EndpointService_StartEndpointServeServer) error
	StopEndpointServe(ctx context.Context, in *pb.StopEndpointServeRequest) (*pb.StopEndpointServeResponse, error)
}

type HttpEndpointService struct {
	pb.UnimplementedEndpointServiceServer
	ctx               context.Context
	mu                sync.Mutex
	config            types.AppConfig
	rdb               *common.RedisClient
	keyEventManager   *common.KeyEventManager
	scheduler         *scheduler.Scheduler
	backendRepo       repository.BackendRepository
	workspaceRepo     repository.WorkspaceRepository
	containerRepo     repository.ContainerRepository
	eventRepo         repository.EventRepository
	taskRepo          repository.TaskRepository
	endpointInstances *common.SafeMap[*endpointInstance]
	tailscale         *network.Tailscale
	taskDispatcher    *task.Dispatcher
	controller        *abstractions.InstanceController
}

var (
	DefaultEndpointRequestTimeoutS int    = 600 // 10 minutes
	DefaultEndpointRequestTTL      uint32 = 600 // 10 minutes
	ASGIRoutePrefix                string = "/asgi"

	endpointContainerPrefix                 string        = "endpoint"
	endpointRoutePrefix                     string        = "/endpoint"
	endpointServeContainerTimeout           time.Duration = 10 * time.Minute
	endpointServeContainerKeepaliveInterval time.Duration = 30 * time.Second
	endpointRequestHeartbeatInterval        time.Duration = 5 * time.Second
	endpointRequestHeartbeatKeepAlive       time.Duration = 30 * time.Second
	endpointMinRequestBufferSize            int           = 10
)

type EndpointServiceOpts struct {
	Config         types.AppConfig
	RedisClient    *common.RedisClient
	BackendRepo    repository.BackendRepository
	WorkspaceRepo  repository.WorkspaceRepository
	TaskRepo       repository.TaskRepository
	ContainerRepo  repository.ContainerRepository
	Scheduler      *scheduler.Scheduler
	RouteGroup     *echo.Group
	Tailscale      *network.Tailscale
	TaskDispatcher *task.Dispatcher
	EventRepo      repository.EventRepository
}

func NewHTTPEndpointService(
	ctx context.Context,
	opts EndpointServiceOpts,
) (EndpointService, error) {
	keyEventManager, err := common.NewKeyEventManager(opts.RedisClient)
	if err != nil {
		return nil, err
	}

	es := &HttpEndpointService{
		ctx:               ctx,
		mu:                sync.Mutex{},
		config:            opts.Config,
		rdb:               opts.RedisClient,
		keyEventManager:   keyEventManager,
		scheduler:         opts.Scheduler,
		backendRepo:       opts.BackendRepo,
		workspaceRepo:     opts.WorkspaceRepo,
		containerRepo:     opts.ContainerRepo,
		taskRepo:          opts.TaskRepo,
		endpointInstances: common.NewSafeMap[*endpointInstance](),
		tailscale:         opts.Tailscale,
		taskDispatcher:    opts.TaskDispatcher,
		eventRepo:         opts.EventRepo,
	}

	// Listen for container events with a certain prefix
	// For example if a container is created, destroyed, or updated
	eventManager, err := abstractions.NewContainerEventManager(ctx, endpointContainerPrefix, keyEventManager, es.InstanceFactory)
	if err != nil {
		return nil, err
	}
	eventManager.Listen()

	es.controller = abstractions.NewInstanceController(ctx, es.InstanceFactory, []string{types.StubTypeEndpointDeployment, types.StubTypeASGIDeployment}, es.backendRepo, es.rdb)
	err = es.controller.Init()
	if err != nil {
		return nil, err
	}

	// Register task dispatcher
	es.taskDispatcher.Register(string(types.ExecutorEndpoint), es.endpointTaskFactory)

	// Register HTTP routes
	authMiddleware := auth.AuthMiddleware(es.backendRepo, es.workspaceRepo)
	registerEndpointRoutes(opts.RouteGroup.Group(endpointRoutePrefix, authMiddleware), es)
	registerASGIRoutes(opts.RouteGroup.Group(ASGIRoutePrefix, authMiddleware), es)

	return es, nil
}

func (es *HttpEndpointService) endpointTaskFactory(ctx context.Context, msg types.TaskMessage) (types.TaskInterface, error) {
	return &EndpointTask{
		es:  es,
		msg: &msg,
	}, nil
}

func (es *HttpEndpointService) IsPublic(stubId string) (*types.Workspace, error) {
	instance, err := es.getOrCreateEndpointInstance(es.ctx, stubId)
	if err != nil {
		return nil, err
	}

	if instance.StubConfig.Authorized {
		return nil, errors.New("unauthorized")
	}

	return instance.Workspace, nil
}

// Forward request to endpoint
func (es *HttpEndpointService) forwardRequest(
	ctx echo.Context,
	authInfo *auth.AuthInfo,
	stubId string,
) error {
	instance, err := es.getOrCreateEndpointInstance(ctx.Request().Context(), stubId)
	if err != nil {
		return err
	}

	tasksInFlight, err := es.taskRepo.TasksInFlight(ctx.Request().Context(), authInfo.Workspace.Name, stubId)
	if err != nil {
		return ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error": err.Error(),
		})
	}

	if tasksInFlight >= int(instance.StubConfig.MaxPendingTasks) {
		err := types.ErrExceededTaskLimit{MaxPendingTasks: instance.StubConfig.MaxPendingTasks}
		return ctx.JSON(http.StatusTooManyRequests, map[string]interface{}{
			"error": err.Error(),
		})
	}

	// Needed for backwards compatibility
	ttl := instance.StubConfig.TaskPolicy.TTL
	if ttl == 0 {
		ttl = DefaultEndpointRequestTTL
	}

	task, err := es.taskDispatcher.Send(ctx.Request().Context(), string(types.ExecutorEndpoint), authInfo, stubId, &types.TaskPayload{}, types.TaskPolicy{
		MaxRetries: 0,
		Timeout:    instance.StubConfig.TaskPolicy.Timeout,
		Expires:    time.Now().Add(time.Duration(ttl) * time.Second),
	})
	if err != nil {
		return err
	}

	return task.Execute(ctx.Request().Context(), ctx)
}

func (es *HttpEndpointService) InstanceFactory(ctx context.Context, stubId string, options ...func(abstractions.IAutoscaledInstance)) (abstractions.IAutoscaledInstance, error) {
	return es.getOrCreateEndpointInstance(ctx, stubId)
}

func (es *HttpEndpointService) getOrCreateEndpointInstance(ctx context.Context, stubId string, options ...func(*endpointInstance)) (*endpointInstance, error) {
	instance, exists := es.endpointInstances.Get(stubId)
	if exists {
		return instance, nil
	}

	// The reason we lock here, and then check again -- is because if the instance does not exist, we may have two separate
	// goroutines trying to create the instance. So, we check first, then get the mutex. If another
	// routine got the lock, it should have created the instance, so we check once again. That way
	// we don't create two instances of the same stub, but we also ensure that we return quickly if the instance
	// _does_ already exist.
	es.mu.Lock()
	defer es.mu.Unlock()

	instance, exists = es.endpointInstances.Get(stubId)
	if exists {
		return instance, nil
	}

	stub, err := es.backendRepo.GetStubByExternalId(es.ctx, stubId)
	if err != nil {
		return nil, errors.New("invalid stub id")
	}

	var stubConfig *types.StubConfigV1 = &types.StubConfigV1{}
	err = json.Unmarshal([]byte(stub.Config), stubConfig)
	if err != nil {
		return nil, err
	}

	token, err := es.backendRepo.RetrieveActiveToken(es.ctx, stub.Workspace.Id)
	if err != nil {
		return nil, err
	}

	requestBufferSize := int(stubConfig.MaxPendingTasks) + 1
	if requestBufferSize < endpointMinRequestBufferSize {
		requestBufferSize = endpointMinRequestBufferSize
	}

	// Create endpoint instance to hold endpoint specific methods/fields
	instance = &endpointInstance{}

	// Create base autoscaled instance
	autoscaledInstance, err := abstractions.NewAutoscaledInstance(es.ctx, &abstractions.AutoscaledInstanceConfig{
		Name:                fmt.Sprintf("%s-%s", stub.Name, stub.ExternalId),
		AppConfig:           es.config,
		Rdb:                 es.rdb,
		Stub:                stub,
		StubConfig:          stubConfig,
		Object:              &stub.Object,
		Workspace:           &stub.Workspace,
		Token:               token,
		Scheduler:           es.scheduler,
		ContainerRepo:       es.containerRepo,
		BackendRepo:         es.backendRepo,
		EventRepo:           es.eventRepo,
		TaskRepo:            es.taskRepo,
		InstanceLockKey:     Keys.endpointInstanceLock(stub.Workspace.Name, stubId),
		StartContainersFunc: instance.startContainers,
		StopContainersFunc:  instance.stopContainers,
	})
	if err != nil {
		return nil, err
	}

	if stub.Type.Kind() == types.StubTypeASGI {
		instance.isASGI = true
	}

	instance.buffer = NewRequestBuffer(autoscaledInstance.Ctx, es.rdb, &stub.Workspace, stubId, requestBufferSize, es.containerRepo, es.keyEventManager, stubConfig, es.tailscale, es.config.Tailscale, instance.isASGI)

	// Embed autoscaled instance struct
	instance.AutoscaledInstance = autoscaledInstance

	// Set all options on the instance
	for _, o := range options {
		o(instance)
	}

	if instance.Autoscaler == nil {
		if stub.Type.IsDeployment() {
			instance.Autoscaler = abstractions.NewAutoscaler(instance, endpointSampleFunc, endpointDeploymentScaleFunc)
		} else if stub.Type.IsServe() {
			instance.Autoscaler = abstractions.NewAutoscaler(instance, endpointSampleFunc, endpointServeScaleFunc)
		}
	}

	if len(instance.EntryPoint) == 0 {
		instance.EntryPoint = []string{instance.StubConfig.PythonVersion, "-m", "beta9.runner.endpoint"}
	}

	es.endpointInstances.Set(stubId, instance)

	// Monitor and then clean up the instance once it's done
	go instance.Monitor()
	go func(i *endpointInstance) {
		<-i.Ctx.Done()
		es.endpointInstances.Delete(stubId)
	}(instance)

	return instance, nil
}

var Keys = &keys{}

type keys struct{}

var (
	endpointKeepWarmLock     string = "endpoint:%s:%s:keep_warm_lock:%s"
	endpointInstanceLock     string = "endpoint:%s:%s:instance_lock"
	endpointRequestTokens    string = "endpoint:%s:%s:request_tokens:%s"
	endpointRequestHeartbeat string = "endpoint:%s:%s:request_heartbeat:%s:%s"
	endpointServeLock        string = "endpoint:%s:%s:serve_lock"
)

func (k *keys) endpointKeepWarmLock(workspaceName, stubId, containerId string) string {
	return fmt.Sprintf(endpointKeepWarmLock, workspaceName, stubId, containerId)
}

func (k *keys) endpointInstanceLock(workspaceName, stubId string) string {
	return fmt.Sprintf(endpointInstanceLock, workspaceName, stubId)
}

func (k *keys) endpointRequestTokens(workspaceName, stubId, containerId string) string {
	return fmt.Sprintf(endpointRequestTokens, workspaceName, stubId, containerId)
}

func (k *keys) endpointRequestHeartbeat(workspaceName, stubId, taskId, containerId string) string {
	return fmt.Sprintf(endpointRequestHeartbeat, workspaceName, stubId, taskId, containerId)
}

func (k *keys) endpointServeLock(workspaceName, stubId string) string {
	return fmt.Sprintf(endpointServeLock, workspaceName, stubId)
}
