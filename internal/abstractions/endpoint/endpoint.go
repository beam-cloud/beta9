package endpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/scheduler"
	"github.com/beam-cloud/beta9/internal/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/labstack/echo/v4"
)

type EndpointService interface {
	pb.EndpointServiceServer
	EndpointRequest(ctx context.Context, in *pb.EndpointRequestRequest) (*pb.EndpointRequestResponse, error)
}

type RingBufferEndpointService struct {
	pb.UnimplementedEndpointServiceServer
	ctx               context.Context
	rdb               *common.RedisClient
	scheduler         *scheduler.Scheduler
	backendRepo       repository.BackendRepository
	containerRepo     repository.ContainerRepository
	endpointInstances *common.SafeMap[*endpointInstance]
	client            *RingBufferEndpointClient
}

var (
	endpointContainerPrefix string = "endpoint"
	endpointRoutePrefix     string = "/endpoint"
)

var RingBufferSize int = 10000000

func NewEndpointService(
	ctx context.Context,
	rdb *common.RedisClient,
	scheduler *scheduler.Scheduler,
	baseRouteGroup *echo.Group,
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

	containerRepo := repository.NewContainerRedisRepository(rdb)

	ws := &RingBufferEndpointService{
		ctx:               ctx,
		rdb:               rdb,
		scheduler:         scheduler,
		backendRepo:       backendRepo,
		containerRepo:     containerRepo,
		endpointInstances: common.NewSafeMap[*endpointInstance](),
		client:            NewRingBufferEndpointClient(RingBufferSize, containerRepo),
	}

	// Register HTTP routes
	authMiddleware := auth.AuthMiddleware(backendRepo)
	registerWebServerRoutes(baseRouteGroup.Group(endpointRoutePrefix, authMiddleware), ws)

	return ws, nil
}

func (ws *RingBufferEndpointService) EndpointRequest(ctx context.Context, in *pb.EndpointRequestRequest) (*pb.EndpointRequestResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	log.Println(authInfo)

	// Forward request to endpoint
	payloadReader := bytes.NewReader(in.Payload)
	bodyReader := ioutil.NopCloser(payloadReader)

	var headers map[string][]string

	if in.Headers != nil {
		err := json.Unmarshal(in.Headers, &headers)
		if err != nil {
			return &pb.EndpointRequestResponse{
				Response: []byte{},
				Ok:       false,
			}, err
		}
	}

	requestData := RequestData{
		ctx:     ctx,
		Method:  in.Method,
		URL:     in.Path,
		Headers: headers,
		Body:    bodyReader,
	}

	response, err := ws.forwardRequest(ctx, in.StubId, requestData)

	return &pb.EndpointRequestResponse{
		Ok:       err == nil,
		Response: response,
	}, nil
}

func (ws *RingBufferEndpointService) forwardRequest(ctx context.Context, stubId string, requestData RequestData) ([]byte, error) {
	// Forward request to endpoint
	endpoint, exists := ws.endpointInstances.Get(stubId)

	if !exists {
		err := ws.createEndpointInstance(stubId)
		if err != nil {
			return []byte{}, err
		}

		endpoint, _ = ws.endpointInstances.Get(stubId)
	}

	requestData.stubId = endpoint.stub.ExternalId

	resp, err := ws.client.ForwardRequest(requestData)
	if err != nil {
		return resp, err
	}

	return resp, nil
}

func (ws *RingBufferEndpointService) createEndpointInstance(stubId string) error {
	_, exists := ws.endpointInstances.Get(stubId)
	if exists {
		return errors.New("endpoint already in memory")
	}

	stub, err := ws.backendRepo.GetStubByExternalId(ws.ctx, stubId)
	if err != nil {
		return errors.New("invalid stub id")
	}

	var stubConfig *types.StubConfigV1 = &types.StubConfigV1{}
	err = json.Unmarshal([]byte(stub.Config), stubConfig)
	if err != nil {
		return err
	}

	token, err := ws.backendRepo.RetrieveActiveToken(ws.ctx, stub.Workspace.Id)
	if err != nil {
		return err
	}

	ctx, cancelFunc := context.WithCancel(ws.ctx)
	lock := common.NewRedisLock(ws.rdb)
	endpoint := &endpointInstance{
		ctx:                ctx,
		cancelFunc:         cancelFunc,
		lock:               lock,
		name:               fmt.Sprintf("%s-%s", stub.Name, stub.ExternalId),
		workspace:          &stub.Workspace,
		stub:               &stub.Stub,
		object:             &stub.Object,
		token:              token,
		stubConfig:         stubConfig,
		scheduler:          ws.scheduler,
		containerRepo:      ws.containerRepo,
		containerEventChan: make(chan types.ContainerEvent, 1),
		containers:         make(map[string]bool),
		scaleEventChan:     make(chan int, 1),
		rdb:                ws.rdb,
		// client:             ws.queueClient,
	}

	autoscaler := newAutoscaler(endpoint)
	endpoint.autoscaler = autoscaler

	ws.endpointInstances.Set(stubId, endpoint)
	go endpoint.monitor()

	// Clean up the queue instance once it's done
	go func(q *endpointInstance) {
		<-q.ctx.Done()
		ws.endpointInstances.Delete(stubId)
	}(endpoint)

	return nil
}
