package endpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

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
	EndpointServe(context.Context, *pb.EndpointServeRequest) (*pb.EndpointServeResponse, error)
}

type HttpEndpointService struct {
	pb.UnimplementedEndpointServiceServer
	ctx               context.Context
	rdb               *common.RedisClient
	scheduler         *scheduler.Scheduler
	backendRepo       repository.BackendRepository
	containerRepo     repository.ContainerRepository
	endpointInstances *common.SafeMap[*endpointInstance]
	tailscale         *network.Tailscale
	keyEventManager   *common.KeyEventManager
	keyEventChan      chan common.KeyEvent
}

var (
	endpointContainerPrefix string = "endpoint"
	endpointRoutePrefix     string = "/endpoint"
)

var RingBufferSize int = 10000000
var RequestTimeout = 180 * time.Second

type EndpointServiceOpts struct {
	Config      types.AppConfig
	RedisClient *common.RedisClient
	Scheduler   *scheduler.Scheduler
	RouteGroup  *echo.Group
	Tailscale   *network.Tailscale
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
		rdb:               opts.RedisClient,
		keyEventChan:      keyEventChan,
		keyEventManager:   keyEventManager,
		scheduler:         opts.Scheduler,
		backendRepo:       backendRepo,
		containerRepo:     containerRepo,
		endpointInstances: common.NewSafeMap[*endpointInstance](),
		tailscale:         opts.Tailscale,
	}

	go es.handleContainerEvents()

	// Register HTTP routes
	authMiddleware := auth.AuthMiddleware(backendRepo)
	registerEndpointRoutes(opts.RouteGroup.Group(endpointRoutePrefix, authMiddleware), es)

	return es, nil
}

func (es *HttpEndpointService) EndpointServe(ctx context.Context, in *pb.EndpointServeRequest) (*pb.EndpointServeResponse, error) {
	// authInfo, _ := auth.AuthInfoFromContext(ctx)
	// workspaceName := authInfo.Workspace.Name
	// TODO: check auth here (on stubId/authInfo)

	err := es.createEndpointInstance(in.StubId)
	return &pb.EndpointServeResponse{
		Ok: err == nil,
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

func (es *HttpEndpointService) createEndpointInstance(stubId string) error {
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
