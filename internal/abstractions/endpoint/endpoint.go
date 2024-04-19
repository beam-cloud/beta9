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

func (es *HttpEndpointService) StartEndpointServe(in *pb.StartEndpointServeRequest, stream pb.EndpointService_StartEndpointServeServer) error {
	ctx := stream.Context()
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	err := es.createEndpointInstance(in.StubId,
		withEntryPoint(func(instance *endpointInstance) []string {
			return []string{instance.stubConfig.PythonVersion, "-m", "beta9.runner.serve"}
		}),
		withAutoscaler(func(instance *endpointInstance) *abstractions.AutoScaler[*endpointInstance, *endpointAutoscalerSample] {
			return abstractions.NewAutoscaler(instance, endpointSampleFunc, endpointServeScaleFunc)
		}),
	)
	if err != nil {
		return err
	}

	// Set lock (used by autoscaler to scale up the single serve container)
	instance, _ := es.endpointInstances.Get(in.StubId)
	instance.rdb.SetEx(
		context.Background(),
		Keys.endpointServeLock(instance.workspace.Name, instance.stub.ExternalId),
		1,
		endpointServeContainerTimeout,
	)

	container, err := es.waitForContainer(ctx, in.StubId)
	if err != nil {
		return err
	}

	sendCallback := func(o common.OutputMsg) error {
		if err := stream.Send(&pb.StartEndpointServeResponse{Output: o.Msg, Done: o.Done}); err != nil {
			return err
		}

		return nil
	}

	exitCallback := func(exitCode int32) error {
		if err := stream.Send(&pb.StartEndpointServeResponse{Done: true, ExitCode: int32(exitCode)}); err != nil {
			return err
		}
		return nil
	}

	// Keep serve container active for as long as user has their terminal open
	// We can handle timeouts on the client side
	go func() {
		ticker := time.NewTicker(endpointServeContainerKeepaliveInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				instance.rdb.SetEx(
					context.Background(),
					Keys.endpointServeLock(instance.workspace.Name, instance.stub.ExternalId),
					1,
					endpointServeContainerTimeout,
				)
			}
		}
	}()

	logStream, err := abstractions.NewLogStream(abstractions.LogStreamOpts{
		SendCallback:    sendCallback,
		ExitCallback:    exitCallback,
		ContainerRepo:   es.containerRepo,
		Config:          es.config,
		Tailscale:       es.tailscale,
		KeyEventManager: es.keyEventManager,
	})
	if err != nil {
		return err
	}

	return logStream.Stream(ctx, authInfo, container.ContainerId)
}

func (es *HttpEndpointService) StopEndpointServe(ctx context.Context, in *pb.StopEndpointServeRequest) (*pb.StopEndpointServeResponse, error) {
	_, exists := es.endpointInstances.Get(in.StubId)
	if !exists {
		err := es.createEndpointInstance(in.StubId,
			withEntryPoint(func(instance *endpointInstance) []string {
				return []string{instance.stubConfig.PythonVersion, "-m", "beta9.runner.serve"}
			}),
			withAutoscaler(func(instance *endpointInstance) *abstractions.AutoScaler[*endpointInstance, *endpointAutoscalerSample] {
				return abstractions.NewAutoscaler(instance, endpointSampleFunc, endpointServeScaleFunc)
			}),
		)
		if err != nil {
			return &pb.StopEndpointServeResponse{Ok: false}, nil
		}
	}

	instance, _ := es.endpointInstances.Get(in.StubId)

	// Delete serve timeout lock
	instance.rdb.Del(
		context.Background(),
		Keys.endpointServeLock(instance.workspace.Name, instance.stub.ExternalId),
	)

	// Delete all keep warms
	// With serves, there should only ever be one container running, but this is the easiest way to find that container
	containers, err := instance.containerRepo.GetActiveContainersByStubId(instance.stub.ExternalId)
	if err != nil {
		return nil, err
	}

	for _, container := range containers {
		if container.Status == types.ContainerStatusStopping || container.Status == types.ContainerStatusPending {
			continue
		}

		instance.rdb.Del(
			context.Background(),
			Keys.endpointKeepWarmLock(instance.workspace.Name, instance.stub.ExternalId, container.ContainerId),
		)

	}

	return &pb.StopEndpointServeResponse{Ok: true}, nil
}

func (es *HttpEndpointService) waitForContainer(ctx context.Context, stubId string) (*types.ContainerState, error) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	timeout := time.After(endpointServeContainerTimeout)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeout:
			return nil, errors.New("timed out waiting for a container")
		case <-ticker.C:
			containers, err := es.containerRepo.GetActiveContainersByStubId(stubId)
			if err != nil {
				return nil, err
			}

			if len(containers) > 0 {
				return &containers[0], nil
			}
		}
	}
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
				instance.containerEventChan <- types.ContainerEvent{
					ContainerId: containerId,
					Change:      +1,
				}
			case common.KeyOperationDel, common.KeyOperationExpired:
				instance.containerEventChan <- types.ContainerEvent{
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

	task, err := es.taskDispatcher.Send(ctx.Request().Context(), string(types.ExecutorEndpoint), instance.workspace.Name, stubId, payload, types.TaskPolicy{
		MaxRetries: 0,
		Timeout:    instance.stubConfig.TaskPolicy.Timeout,
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

	ctx, cancelFunc := context.WithCancel(es.ctx)
	lock := common.NewRedisLock(es.rdb)

	requestBufferSize := int(stubConfig.MaxPendingTasks)
	if requestBufferSize < endpointMinRequestBufferSize {
		requestBufferSize = endpointMinRequestBufferSize
	}

	// Create endpoint instance & override any default options
	instance := &endpointInstance{
		ctx:                ctx,
		cancelFunc:         cancelFunc,
		lock:               lock,
		name:               fmt.Sprintf("%s-%s", stub.Name, stub.ExternalId),
		workspace:          &stub.Workspace,
		stub:               &stub.Stub,
		object:             &stub.Object,
		token:              token,
		stubConfig:         stubConfig,
		scheduler:          es.scheduler,
		taskRepo:           es.taskRepo,
		containerRepo:      es.containerRepo,
		containerEventChan: make(chan types.ContainerEvent, 1),
		containers:         make(map[string]bool),
		scaleEventChan:     make(chan int, 1),
		rdb:                es.rdb,
		buffer:             NewRequestBuffer(ctx, es.rdb, &stub.Workspace, stubId, requestBufferSize, es.containerRepo, stubConfig),
	}
	for _, o := range options {
		o(instance)
	}

	if instance.autoscaler == nil {
		switch instance.stub.Type {
		case types.StubTypeEndpointDeployment:
			instance.autoscaler = abstractions.NewAutoscaler(instance, endpointSampleFunc, endpointDeploymentScaleFunc)
		case types.StubTypeEndpointServe:
			instance.autoscaler = abstractions.NewAutoscaler(instance, endpointSampleFunc, endpointServeScaleFunc)
		}
	}

	if len(instance.entryPoint) == 0 {
		instance.entryPoint = []string{instance.stubConfig.PythonVersion, "-m", "beta9.runner.endpoint"}
	}

	es.endpointInstances.Set(stubId, instance)
	go instance.monitor()

	// Clean up the queue instance once it's done
	go func(q *endpointInstance) {
		<-q.ctx.Done()
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
