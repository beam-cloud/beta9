package endpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	abstractions "github.com/beam-cloud/beta9/internal/abstractions/common"
	"github.com/beam-cloud/beta9/internal/task"

	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/network"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/scheduler"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/labstack/echo/v4"

	pb "github.com/beam-cloud/beta9/proto"
)

type EndpointService interface {
	pb.EndpointServiceServer
	StartEndpointServe(in *pb.StartEndpointServeRequest, stream pb.EndpointService_StartEndpointServeServer) error
	StopEndpointServe(ctx context.Context, in *pb.StopEndpointServeRequest) (*pb.StopEndpointServeResponse, error)
}

type HttpEndpointService struct {
	pb.UnimplementedEndpointServiceServer
	ctx               context.Context
	config            types.AppConfig
	rdb               *common.RedisClient
	scheduler         *scheduler.Scheduler
	backendRepo       repository.BackendRepository
	containerRepo     repository.ContainerRepository
	taskRepo          repository.TaskRepository
	endpointInstances *common.SafeMap[*endpointInstance]
	tailscale         *network.Tailscale
	keyEventManager   *common.KeyEventManager
	keyEventChan      chan common.KeyEvent
	taskDispatcher    *task.Dispatcher
}

var (
	endpointContainerPrefix                 string        = "endpoint"
	endpointRoutePrefix                     string        = "/endpoint"
	endpointRequestTimeoutS                 int           = 180
	endpointServeContainerTimeout           time.Duration = 600 * time.Second
	endpointServeContainerKeepaliveInterval time.Duration = 30 * time.Second
	endpointRequestHeartbeatInterval        time.Duration = 30 * time.Second
	endpointMinRequestBufferSize            int           = 10
)

type EndpointServiceOpts struct {
	Config         types.AppConfig
	RedisClient    *common.RedisClient
	BackendRepo    repository.BackendRepository
	TaskRepo       repository.TaskRepository
	ContainerRepo  repository.ContainerRepository
	Scheduler      *scheduler.Scheduler
	RouteGroup     *echo.Group
	Tailscale      *network.Tailscale
	TaskDispatcher *task.Dispatcher
}

func NewEndpointService(
	ctx context.Context,
	opts EndpointServiceOpts,
) (EndpointService, error) {
	keyEventChan := make(chan common.KeyEvent)
	keyEventManager, err := common.NewKeyEventManager(opts.RedisClient)
	if err != nil {
		return nil, err
	}

	go keyEventManager.ListenForPattern(ctx, common.RedisKeys.SchedulerContainerState(endpointContainerPrefix), keyEventChan)

	configManager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		return nil, err
	}
	config := configManager.GetConfig()

	es := &HttpEndpointService{
		ctx:               ctx,
		config:            config,
		rdb:               opts.RedisClient,
		keyEventChan:      keyEventChan,
		keyEventManager:   keyEventManager,
		scheduler:         opts.Scheduler,
		backendRepo:       opts.BackendRepo,
		containerRepo:     opts.ContainerRepo,
		taskRepo:          opts.TaskRepo,
		endpointInstances: common.NewSafeMap[*endpointInstance](),
		tailscale:         opts.Tailscale,
		taskDispatcher:    opts.TaskDispatcher,
	}

	go es.handleContainerEvents()

	// Register task dispatcher
	es.taskDispatcher.Register(string(types.ExecutorEndpoint), es.endpointTaskFactory)

	// Register HTTP routes
	authMiddleware := auth.AuthMiddleware(es.backendRepo)
	registerEndpointRoutes(opts.RouteGroup.Group(endpointRoutePrefix, authMiddleware), es)

	return es, nil
}

func (es *HttpEndpointService) endpointTaskFactory(ctx context.Context, msg types.TaskMessage) (types.TaskInterface, error) {
	return &EndpointTask{
		es:  es,
		msg: &msg,
	}, nil
}

func (es *HttpEndpointService) handleContainerEvents() {
	for {
		select {
		case event := <-es.keyEventChan:
			containerId := fmt.Sprintf("%s%s", endpointContainerPrefix, event.Key)

			operation := event.Operation
			containerIdParts := strings.Split(containerId, "-")
			stubId := strings.Join(containerIdParts[1:6], "-")

			instance, exists := es.endpointInstances.Get(stubId)
			if !exists {
				err := es.createEndpointInstance(stubId)
				if err != nil {
					continue
				}

				instance, _ = es.endpointInstances.Get(stubId)
			}

			switch operation {
			case common.KeyOperationSet, common.KeyOperationHSet:
				instance.ContainerEventChan <- types.ContainerEvent{
					ContainerId: containerId,
					Change:      +1,
				}
			case common.KeyOperationDel, common.KeyOperationExpired:
				instance.ContainerEventChan <- types.ContainerEvent{
					ContainerId: containerId,
					Change:      -1,
				}
			}

		case <-es.ctx.Done():
			return
		}
	}
}

// Forward request to endpoint
func (es *HttpEndpointService) forwardRequest(
	ctx echo.Context,
	stubId string,
) error {
	payload, err := task.SerializeHttpPayload(ctx)
	if err != nil {
		return err
	}

	instance, exists := es.endpointInstances.Get(stubId)
	if !exists {
		err := es.createEndpointInstance(stubId)
		if err != nil {
			return err
		}

		instance, _ = es.endpointInstances.Get(stubId)
	}

	task, err := es.taskDispatcher.Send(ctx.Request().Context(), string(types.ExecutorEndpoint), instance.Workspace.Name, stubId, payload, types.TaskPolicy{
		MaxRetries: 0,
		Timeout:    instance.StubConfig.TaskPolicy.Timeout,
		Expires:    time.Now().Add(time.Duration(endpointRequestTimeoutS) * time.Second),
	})
	if err != nil {
		return err
	}

	return task.Execute(ctx.Request().Context(), ctx)
}

func (es *HttpEndpointService) createEndpointInstance(stubId string, options ...func(*endpointInstance)) error {
	_, exists := es.endpointInstances.Get(stubId)
	if exists {
		return errors.New("endpoint already in memory")
	}

	stub, err := es.backendRepo.GetStubByExternalId(es.ctx, stubId)
	if err != nil {
		return errors.New("invalid stub id")
	}

	var stubConfig *types.StubConfigV1 = &types.StubConfigV1{}
	err = json.Unmarshal([]byte(stub.Config), stubConfig)
	if err != nil {
		return err
	}

	token, err := es.backendRepo.RetrieveActiveToken(es.ctx, stub.Workspace.Id)
	if err != nil {
		return err
	}

	requestBufferSize := int(stubConfig.MaxPendingTasks)
	if requestBufferSize < endpointMinRequestBufferSize {
		requestBufferSize = endpointMinRequestBufferSize
	}

	// Create endpoint instance to hold endpoint specific methods/fields
	instance := &endpointInstance{
		buffer: NewRequestBuffer(es.ctx, es.rdb, &stub.Workspace, stubId, requestBufferSize, es.containerRepo, stubConfig),
	}

	// Create base autoscaled instance
	autoscaledInstance, err := abstractions.NewAutoscaledInstance(es.ctx, &abstractions.AutoscaledInstanceConfig{
		Name:                fmt.Sprintf("%s-%s", stub.Name, stub.ExternalId),
		Rdb:                 es.rdb,
		Stub:                &stub.Stub,
		StubConfig:          stubConfig,
		Object:              &stub.Object,
		Workspace:           &stub.Workspace,
		Token:               token,
		Scheduler:           es.scheduler,
		ContainerRepo:       es.containerRepo,
		BackendRepo:         es.backendRepo,
		TaskRepo:            es.taskRepo,
		InstanceLockKey:     Keys.endpointInstanceLock(stub.Workspace.Name, stubId),
		StartContainersFunc: instance.startContainers,
		StopContainersFunc:  instance.stopContainers,
	})
	if err != nil {
		return err
	}

	// Embed autoscaled instance struct
	instance.AutoscaledInstance = autoscaledInstance

	// Set all options on the instance
	for _, o := range options {
		o(instance)
	}

	if instance.Autoscaler == nil {
		switch instance.Stub.Type {
		case types.StubTypeEndpointDeployment:
			instance.Autoscaler = abstractions.NewAutoscaler(instance, endpointSampleFunc, endpointDeploymentScaleFunc)
		case types.StubTypeEndpointServe:
			instance.Autoscaler = abstractions.NewAutoscaler(instance, endpointSampleFunc, endpointServeScaleFunc)
		}
	}

	if len(instance.EntryPoint) == 0 {
		instance.EntryPoint = []string{instance.StubConfig.PythonVersion, "-m", "beta9.runner.endpoint"}
	}

	es.endpointInstances.Set(stubId, instance)

	go instance.Monitor()

	// Clean up the queue instance once it's done
	go func(q *endpointInstance) {
		<-q.Ctx.Done()
		es.endpointInstances.Delete(stubId)
	}(instance)

	return nil
}

var Keys = &keys{}

type keys struct{}

var (
	endpointKeepWarmLock     string = "endpoint:%s:%s:keep_warm_lock:%s"
	endpointInstanceLock     string = "endpoint:%s:%s:instance_lock"
	endpointRequestsInFlight string = "endpoint:%s:%s:requests_in_flight:%s"
	endpointRequestHeartbeat string = "endpoint:%s:%s:request_heartbeat:%s"
	endpointServeLock        string = "endpoint:%s:%s:serve_lock"
)

func (k *keys) endpointKeepWarmLock(workspaceName, stubId, containerId string) string {
	return fmt.Sprintf(endpointKeepWarmLock, workspaceName, stubId, containerId)
}

func (k *keys) endpointInstanceLock(workspaceName, stubId string) string {
	return fmt.Sprintf(endpointInstanceLock, workspaceName, stubId)
}

func (k *keys) endpointRequestsInFlight(workspaceName, stubId, containerId string) string {
	return fmt.Sprintf(endpointRequestsInFlight, workspaceName, stubId, containerId)
}

func (k *keys) endpointRequestHeartbeat(workspaceName, stubId, taskId string) string {
	return fmt.Sprintf(endpointRequestHeartbeat, workspaceName, stubId, taskId)
}

func (k *keys) endpointServeLock(workspaceName, stubId string) string {
	return fmt.Sprintf(endpointServeLock, workspaceName, stubId)
}
