package endpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
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
	EndpointServe(in *pb.EndpointServeRequest, stream pb.EndpointService_EndpointServeServer) error
}

type HttpEndpointService struct {
	pb.UnimplementedEndpointServiceServer
	ctx               context.Context
	config            types.AppConfig
	rdb               *common.RedisClient
	scheduler         *scheduler.Scheduler
	backendRepo       repository.BackendRepository
	containerRepo     repository.ContainerRepository
	endpointInstances *common.SafeMap[*endpointInstance]
	tailscale         *network.Tailscale
	keyEventManager   *common.KeyEventManager
	keyEventChan      chan common.KeyEvent
	taskDispatcher    *task.Dispatcher
}

var (
	endpointContainerPrefix       string        = "endpoint"
	endpointRoutePrefix           string        = "/endpoint"
	endpointRingBufferSize        int           = 10000000
	endpointRequestTimeout        time.Duration = 180 * time.Second
	endpointServeContainerTimeout time.Duration = 120 * time.Second
)

type EndpointServiceOpts struct {
	Config         types.AppConfig
	RedisClient    *common.RedisClient
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

	backendRepo, err := repository.NewBackendPostgresRepository(config.Database.Postgres)
	if err != nil {
		return nil, err
	}

	containerRepo := repository.NewContainerRedisRepository(opts.RedisClient)

	es := &HttpEndpointService{
		ctx:               ctx,
		config:            config,
		rdb:               opts.RedisClient,
		keyEventChan:      keyEventChan,
		keyEventManager:   keyEventManager,
		scheduler:         opts.Scheduler,
		backendRepo:       backendRepo,
		containerRepo:     containerRepo,
		endpointInstances: common.NewSafeMap[*endpointInstance](),
		tailscale:         opts.Tailscale,
		taskDispatcher:    opts.TaskDispatcher,
	}

	go es.handleContainerEvents()

	es.taskDispatcher.Register(string(types.ExecutorTaskQueue), es.endpointTaskFactory)

	// Register HTTP routes
	authMiddleware := auth.AuthMiddleware(backendRepo)
	registerEndpointRoutes(opts.RouteGroup.Group(endpointRoutePrefix, authMiddleware), es)

	return es, nil
}

func (es *HttpEndpointService) endpointTaskFactory(ctx context.Context, msg types.TaskMessage) (types.TaskInterface, error) {
	return &EndpointTask{
		es:  es,
		msg: &msg,
	}, nil
}

func (es *HttpEndpointService) EndpointServe(in *pb.EndpointServeRequest, stream pb.EndpointService_EndpointServeServer) error {
	ctx := stream.Context()
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	// TODO: check auth here (on stubId/authInfo)

	err := es.createEndpointInstance(in.StubId,
		withEntryPoint(func(instance *endpointInstance) []string {
			return []string{instance.stubConfig.PythonVersion, "-m", "beta9.runner.serve"}
		}),
		withAutoscaler(func(instance *endpointInstance) *abstractions.AutoScaler[*endpointInstance, *endpointAutoscalerSample] {
			return abstractions.NewAutoscaler(instance, endpointServeSampleFunc, endpointServeScaleFunc)
		}))
	if err != nil {
		return err
	}

	container, err := es.waitForContainer(ctx, in.StubId)
	if err != nil {
		return err
	}

	sendCallback := func(o common.OutputMsg) error {
		if err := stream.Send(&pb.EndpointServeResponse{Output: o.Msg, Done: o.Done}); err != nil {
			return err
		}

		return nil
	}

	exitCallback := func(exitCode int32) error {
		if err := stream.Send(&pb.EndpointServeResponse{Done: true, ExitCode: int32(exitCode)}); err != nil {
			return err
		}
		return nil
	}

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

	task, err := es.taskDispatcher.Send(ctx.Request().Context(), string(types.ExecutorTaskQueue), instance.workspace.Name, stubId, payload, instance.stubConfig.TaskPolicy)
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
		containerRepo:      es.containerRepo,
		containerEventChan: make(chan types.ContainerEvent, 1),
		containers:         make(map[string]bool),
		scaleEventChan:     make(chan int, 1),
		rdb:                es.rdb,
		buffer:             NewRequestBuffer(ctx, es.rdb, &stub.Workspace, stubId, endpointRingBufferSize, es.containerRepo, stubConfig),
	}
	for _, o := range options {
		o(instance)
	}

	if instance.autoscaler == nil {
		instance.autoscaler = abstractions.NewAutoscaler(instance, endpointDeploymentSampleFunc, endpointDeploymentScaleFunc)
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
		log.Println("cleaned up my guy.")
	}(instance)

	log.Println("created the instance.")

	return nil
}

var Keys = &keys{}

type keys struct{}

var (
	endpointKeepWarmLock string = "endpoint:%s:%s:keep_warm_lock:%s"
	endpointInstanceLock string = "endpoint:%s:%s:instance_lock"
)

func (k *keys) endpointKeepWarmLock(workspaceName, stubId, containerId string) string {
	return fmt.Sprintf(endpointKeepWarmLock, workspaceName, stubId, containerId)
}

func (k *keys) endpointInstanceLock(workspaceName, stubId string) string {
	return fmt.Sprintf(endpointInstanceLock, workspaceName, stubId)
}
