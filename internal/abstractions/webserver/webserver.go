package webserver

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

type WebserverService interface {
	pb.WebserverServiceServer
	WebserverRequest(ctx context.Context, in *pb.WebserverRequestRequest) (*pb.WebserverRequestResponse, error)
}

type RingBufferWebserverService struct {
	pb.UnimplementedWebserverServiceServer
	ctx                context.Context
	rdb                *common.RedisClient
	scheduler          *scheduler.Scheduler
	backendRepo        repository.BackendRepository
	containerRepo      repository.ContainerRepository
	webserverInstances *common.SafeMap[*webserverInstance]
	client             *RingBufferWebserverClient
}

var (
	webserverContainerPrefix string = "webserver"
	webserverRoutePrefix     string = "/webserver"
)

var RingBufferSize int = 10000000

func NewWebserverService(
	ctx context.Context,
	rdb *common.RedisClient,
	scheduler *scheduler.Scheduler,
	baseRouteGroup *echo.Group,
) (WebserverService, error) {
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

	ws := &RingBufferWebserverService{
		ctx:                ctx,
		rdb:                rdb,
		scheduler:          scheduler,
		backendRepo:        backendRepo,
		containerRepo:      containerRepo,
		webserverInstances: common.NewSafeMap[*webserverInstance](),
		client:             NewRingBufferWebserverClient(RingBufferSize, containerRepo),
	}

	// Register HTTP routes
	authMiddleware := auth.AuthMiddleware(backendRepo)
	registerWebServerRoutes(baseRouteGroup.Group(webserverRoutePrefix, authMiddleware), ws)

	return ws, nil
}

func (ws *RingBufferWebserverService) WebserverRequest(ctx context.Context, in *pb.WebserverRequestRequest) (*pb.WebserverRequestResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	log.Println(authInfo)

	// Forward request to webserver
	payloadReader := bytes.NewReader(in.Payload)
	bodyReader := ioutil.NopCloser(payloadReader)

	var headers map[string][]string

	if in.Headers != nil {
		err := json.Unmarshal(in.Headers, &headers)
		if err != nil {
			return &pb.WebserverRequestResponse{
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

	return &pb.WebserverRequestResponse{
		Ok:       err == nil,
		Response: response,
	}, nil
}

func (ws *RingBufferWebserverService) forwardRequest(ctx context.Context, stubId string, requestData RequestData) ([]byte, error) {
	// Forward request to webserver
	webserver, exists := ws.webserverInstances.Get(stubId)

	if !exists {
		err := ws.createWebserverInstance(stubId)
		if err != nil {
			return []byte{}, err
		}

		webserver, _ = ws.webserverInstances.Get(stubId)
	}

	requestData.stubId = webserver.stub.ExternalId

	resp, err := ws.client.ForwardRequest(requestData)
	if err != nil {
		return resp, err
	}

	return resp, nil
}

func (ws *RingBufferWebserverService) createWebserverInstance(stubId string) error {
	_, exists := ws.webserverInstances.Get(stubId)
	if exists {
		return errors.New("webserver already in memory")
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
	webserver := &webserverInstance{
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

	autoscaler := newAutoscaler(webserver)
	webserver.autoscaler = autoscaler

	ws.webserverInstances.Set(stubId, webserver)
	go webserver.monitor()

	// Clean up the queue instance once it's done
	go func(q *webserverInstance) {
		<-q.ctx.Done()
		ws.webserverInstances.Delete(stubId)
	}(webserver)

	return nil
}
