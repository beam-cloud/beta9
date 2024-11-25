package container_app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/labstack/echo/v4"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type ContainerAppService interface {
	pb.ContainerServiceServer
	// StartContainerAppServe(in *pb.StartContainerAppServeRequest, stream pb.EndpointService_StartEndpointServeServer) error
	// StopContainerAppServe(ctx context.Context, in *pb.StopEndpointServeRequest) (*pb.StopEndpointServeResponse, error)
}

type TCPContainerAppService struct {
	pb.ContainerServiceServer
	ctx             context.Context
	config          types.AppConfig
	rdb             *common.RedisClient
	keyEventManager *common.KeyEventManager
	scheduler       *scheduler.Scheduler
	backendRepo     repository.BackendRepository
	containerRepo   repository.ContainerRepository
	eventRepo       repository.EventRepository
	appInstances    *common.SafeMap[*appInstance]
	tailscale       *network.Tailscale
}

var (
	DefaultEndpointRequestTimeoutS     int           = 600  // 10 minutes
	DefaultEndpointRequestTTL          uint32        = 1200 // 20 minutes
	appContainerPrefix                 string        = "app"
	appRoutePrefix                     string        = "/app"
	appServeContainerTimeout           time.Duration = 10 * time.Minute
	appServeContainerKeepaliveInterval time.Duration = 30 * time.Second
	appRequestHeartbeatInterval        time.Duration = 30 * time.Second
	appMinRequestBufferSize            int           = 10
)

type ContainerAppServiceOpts struct {
	Config        types.AppConfig
	RedisClient   *common.RedisClient
	BackendRepo   repository.BackendRepository
	ContainerRepo repository.ContainerRepository
	Scheduler     *scheduler.Scheduler
	RouteGroup    *echo.Group
	Tailscale     *network.Tailscale
	EventRepo     repository.EventRepository
}

func NewTCPContainerAppService(
	ctx context.Context,
	opts ContainerAppServiceOpts,
) (ContainerAppService, error) {
	keyEventManager, err := common.NewKeyEventManager(opts.RedisClient)
	if err != nil {
		return nil, err
	}

	as := &TCPContainerAppService{
		ctx:             ctx,
		config:          opts.Config,
		rdb:             opts.RedisClient,
		keyEventManager: keyEventManager,
		scheduler:       opts.Scheduler,
		backendRepo:     opts.BackendRepo,
		containerRepo:   opts.ContainerRepo,
		appInstances:    common.NewSafeMap[*appInstance](),
		tailscale:       opts.Tailscale,
		eventRepo:       opts.EventRepo,
	}

	// Listen for container events with a certain prefix
	// For example if a container is created, destroyed, or updated
	eventManager, err := abstractions.NewContainerEventManager(appContainerPrefix, keyEventManager, as.InstanceFactory)
	if err != nil {
		return nil, err
	}
	eventManager.Listen(ctx)

	eventBus := common.NewEventBus(
		opts.RedisClient,
		common.EventBusSubscriber{Type: common.EventTypeReloadInstance, Callback: func(e *common.Event) bool {
			stubId := e.Args["stub_id"].(string)
			stubType := e.Args["stub_type"].(string)

			if stubType != types.StubTypeEndpointDeployment && stubType != types.StubTypeASGIDeployment {
				// Assume the callback succeeded to avoid retries
				return true
			}

			instance, err := as.getOrCreateAppInstance(stubId)
			if err != nil {
				return false
			}

			instance.Reload()

			return true
		}},
	)

	go eventBus.ReceiveEvents(ctx)

	// Register HTTP routes
	// authMiddleware := auth.AuthMiddleware(as.backendRepo)
	// registerAppRoutes(opts.RouteGroup.Group(appRoutePrefix, authMiddleware), as)

	return as, nil
}

func (as *TCPContainerAppService) isPublic(stubId string) (*types.Workspace, error) {
	instance, err := as.getOrCreateAppInstance(stubId)
	if err != nil {
		return nil, err
	}

	if instance.StubConfig.Authorized {
		return nil, errors.New("unauthorized")
	}

	return instance.Workspace, nil
}

// Forward request to endpoint
func (es *TCPContainerAppService) forwardRequest(
	ctx echo.Context,
	authInfo *auth.AuthInfo,
	stubId string,
) error {
	_, err := es.getOrCreateAppInstance(stubId)
	if err != nil {
		return err
	}

	return nil // task.Execute(ctx.Request().Context(), ctx)
}

func (as *TCPContainerAppService) InstanceFactory(stubId string, options ...func(abstractions.IAutoscaledInstance)) (abstractions.IAutoscaledInstance, error) {
	return as.getOrCreateAppInstance(stubId)
}

func (as *TCPContainerAppService) getOrCreateAppInstance(stubId string, options ...func(*appInstance)) (*appInstance, error) {
	instance, exists := as.appInstances.Get(stubId)
	if exists {
		return instance, nil
	}

	stub, err := as.backendRepo.GetStubByExternalId(as.ctx, stubId)
	if err != nil {
		return nil, errors.New("invalid stub id")
	}

	var stubConfig *types.StubConfigV1 = &types.StubConfigV1{}
	err = json.Unmarshal([]byte(stub.Config), stubConfig)
	if err != nil {
		return nil, err
	}

	token, err := as.backendRepo.RetrieveActiveToken(as.ctx, stub.Workspace.Id)
	if err != nil {
		return nil, err
	}

	requestBufferSize := int(stubConfig.MaxPendingTasks)
	if requestBufferSize < appMinRequestBufferSize {
		requestBufferSize = appMinRequestBufferSize
	}

	// Create endpoint instance to hold endpoint specific methods/fields
	instance = &appInstance{}

	// Create base autoscaled instance
	autoscaledInstance, err := abstractions.NewAutoscaledInstance(as.ctx, &abstractions.AutoscaledInstanceConfig{
		Name:                fmt.Sprintf("%s-%s", stub.Name, stub.ExternalId),
		Rdb:                 as.rdb,
		Stub:                stub,
		StubConfig:          stubConfig,
		Object:              &stub.Object,
		Workspace:           &stub.Workspace,
		Token:               token,
		Scheduler:           as.scheduler,
		ContainerRepo:       as.containerRepo,
		BackendRepo:         as.backendRepo,
		InstanceLockKey:     Keys.appInstanceLock(stub.Workspace.Name, stubId),
		StartContainersFunc: instance.startContainers,
		StopContainersFunc:  instance.stopContainers,
	})
	if err != nil {
		return nil, err
	}

	instance.buffer = NewConnectionBuffer(autoscaledInstance.Ctx, as.rdb, &stub.Workspace, stubId, requestBufferSize, as.containerRepo, stubConfig, as.tailscale, as.config.Tailscale)

	// Embed autoscaled instance struct
	instance.AutoscaledInstance = autoscaledInstance

	// Set all options on the instance
	for _, o := range options {
		o(instance)
	}

	// if instance.Autoscaler == nil {
	// 	if stub.Type.IsDeployment() {
	// 		instance.Autoscaler = abstractions.NewAutoscaler(instance, endpointSampleFunc, endpointDeploymentScaleFunc)
	// 	} else if stub.Type.IsServe() {
	// 		instance.Autoscaler = abstractions.NewAutoscaler(instance, endpointSampleFunc, endpointServeScaleFunc)
	// 	}
	// }

	if len(instance.EntryPoint) == 0 {
		instance.EntryPoint = []string{instance.StubConfig.PythonVersion, "-m", "beta9.runner.app"}
	}

	as.appInstances.Set(stubId, instance)

	// Monitor and then clean up the instance once it's done
	go instance.Monitor()
	go func(i *appInstance) {
		<-i.Ctx.Done()
		as.appInstances.Delete(stubId)
	}(instance)

	return instance, nil
}

var Keys = &keys{}

type keys struct{}

var (
	appKeepWarmLock     string = "app:%s:%s:keep_warm_lock:%s"
	appInstanceLock     string = "app:%s:%s:instance_lock"
	appRequestsInFlight string = "app:%s:%s:requests_in_flight:%s"
	appRequestHeartbeat string = "app:%s:%s:request_heartbeat:%s"
	appServeLock        string = "app:%s:%s:serve_lock"
)

func (k *keys) appKeepWarmLock(workspaceName, stubId, containerId string) string {
	return fmt.Sprintf(appKeepWarmLock, workspaceName, stubId, containerId)
}

func (k *keys) appInstanceLock(workspaceName, stubId string) string {
	return fmt.Sprintf(appInstanceLock, workspaceName, stubId)
}

func (k *keys) appRequestsInFlight(workspaceName, stubId, containerId string) string {
	return fmt.Sprintf(appRequestsInFlight, workspaceName, stubId, containerId)
}

func (k *keys) appRequestHeartbeat(workspaceName, stubId, taskId string) string {
	return fmt.Sprintf(appRequestHeartbeat, workspaceName, stubId, taskId)
}

func (k *keys) appServeLock(workspaceName, stubId string) string {
	return fmt.Sprintf(appServeLock, workspaceName, stubId)
}
