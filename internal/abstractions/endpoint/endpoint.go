package endpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/scheduler"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/labstack/echo/v4"

	pb "github.com/beam-cloud/beta9/proto"
)

type EndpointService interface {
	pb.EndpointServiceServer
}

type RingBufferEndpointService struct {
	pb.UnimplementedEndpointServiceServer
	ctx               context.Context
	rdb               *common.RedisClient
	scheduler         *scheduler.Scheduler
	backendRepo       repository.BackendRepository
	containerRepo     repository.ContainerRepository
	endpointInstances *common.SafeMap[*endpointInstance]
}

var (
	endpointContainerPrefix string = "endpoint"
	endpointRoutePrefix     string = "/endpoint"
)

var RingBufferSize int = 10000000
var RequestTimeout = 120 * time.Second

type EndpointServiceOpts struct {
	Config         types.AppConfig
	RedisClient    *common.RedisClient
	Scheduler      *scheduler.Scheduler
	BaseRouteGroup *echo.Group
}

func NewEndpointService(
	ctx context.Context,
	opts EndpointServiceOpts,
) (EndpointService, error) {
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

	es := &RingBufferEndpointService{
		ctx:               ctx,
		rdb:               opts.RedisClient,
		scheduler:         opts.Scheduler,
		backendRepo:       backendRepo,
		containerRepo:     containerRepo,
		endpointInstances: common.NewSafeMap[*endpointInstance](),
	}

	// Register HTTP routes
	authMiddleware := auth.AuthMiddleware(backendRepo)
	registerEndpointRoutes(opts.BaseRouteGroup.Group(endpointRoutePrefix, authMiddleware), es)

	return es, nil
}

func (es *RingBufferEndpointService) forwardRequest(
	ctx echo.Context,
	stubId string,
) error {
	// Forward request to endpoint
	instances, exists := es.endpointInstances.Get(stubId)

	if !exists {
		err := es.createEndpointInstance(stubId)
		if err != nil {
			return err
		}

		instances, _ = es.endpointInstances.Get(stubId)
	}

	return instances.buffer.ForwardRequest(ctx)
}

func (es *RingBufferEndpointService) createEndpointInstance(stubId string) error {
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
	}

	instance.buffer = NewRequestBuffer(ctx, es.rdb, &stub.Workspace, stubId, RingBufferSize, es.containerRepo, stubConfig)
	autoscaler := newAutoscaler(instance)
	instance.autoscaler = autoscaler

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
	endpointKeepWarmLock string = "endpoint:%s:%s:keep_warm_lock:%s"
	endpointInstanceLock string = "endpoint:%s:%s:instance_lock"
)

func (k *keys) endpointKeepWarmLock(workspaceName, stubId, containerId string) string {
	return fmt.Sprintf(endpointKeepWarmLock, workspaceName, stubId, containerId)
}

func (k *keys) endpointInstanceLock(workspaceName, stubId string) string {
	return fmt.Sprintf(endpointInstanceLock, workspaceName, stubId)
}
