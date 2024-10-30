package bot

import (
	"encoding/json"
	"errors"
	"fmt"

	"context"

	"github.com/labstack/echo/v4"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"

	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/task"
	pb "github.com/beam-cloud/beta9/proto"
)

type BotServiceOpts struct {
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

type BotService interface {
	pb.BotServiceServer
	StartBotServe(in *pb.StartBotServeRequest, stream pb.BotService_StartBotServeServer) error
	StopBotServe(ctx context.Context, in *pb.StopBotServeRequest) (*pb.StopBotServeResponse, error)
	BotServeKeepAlive(ctx context.Context, in *pb.BotServeKeepAliveRequest) (*pb.BotServeKeepAliveResponse, error)
	SendBotMessage(ctx context.Context, in *pb.SendBotMessageRequest) (*pb.SendBotMessageResponse, error)
}

type PetriBotService struct {
	pb.UnimplementedBotServiceServer
	ctx             context.Context
	config          types.AppConfig
	rdb             *common.RedisClient
	keyEventManager *common.KeyEventManager
	scheduler       *scheduler.Scheduler
	backendRepo     repository.BackendRepository
	workspaceRepo   repository.WorkspaceRepository
	containerRepo   repository.ContainerRepository
	eventRepo       repository.EventRepository
	taskRepo        repository.TaskRepository
	tailscale       *network.Tailscale
	taskDispatcher  *task.Dispatcher
	botInstances    *common.SafeMap[*botInstance]
	botStateManager *botStateManager
}

func NewPetriBotService(ctx context.Context, opts BotServiceOpts) (BotService, error) {
	keyEventManager, err := common.NewKeyEventManager(opts.RedisClient)
	if err != nil {
		return nil, err
	}

	pbs := &PetriBotService{
		ctx:             ctx,
		config:          opts.Config,
		rdb:             opts.RedisClient,
		keyEventManager: keyEventManager,
		scheduler:       opts.Scheduler,
		backendRepo:     opts.BackendRepo,
		workspaceRepo:   opts.WorkspaceRepo,
		containerRepo:   opts.ContainerRepo,
		taskRepo:        opts.TaskRepo,
		tailscale:       opts.Tailscale,
		taskDispatcher:  opts.TaskDispatcher,
		eventRepo:       opts.EventRepo,
		botInstances:    common.NewSafeMap[*botInstance](),
		botStateManager: newBotStateManager(opts.RedisClient),
	}

	// Register task dispatcher
	pbs.taskDispatcher.Register(string(types.ExecutorBot), pbs.botTaskFactory)

	return pbs, nil
}

func (pbs *PetriBotService) botTaskFactory(ctx context.Context, msg types.TaskMessage) (types.TaskInterface, error) {
	return &BotTask{
		pbs: pbs,
		msg: &msg,
	}, nil
}

func (pbs *PetriBotService) getOrCreateBotInstance(stubId string) (*botInstance, error) {
	instance, exists := pbs.botInstances.Get(stubId)
	if exists {
		return instance, nil
	}

	stub, err := pbs.backendRepo.GetStubByExternalId(pbs.ctx, stubId)
	if err != nil {
		return nil, errors.New("invalid stub id")
	}

	var stubConfig *types.StubConfigV1 = &types.StubConfigV1{}
	err = json.Unmarshal([]byte(stub.Config), stubConfig)
	if err != nil {
		return nil, err
	}

	var botConfig BotConfig
	err = json.Unmarshal(stubConfig.Extra, &botConfig)
	if err != nil {
		return nil, err
	}

	token, err := pbs.backendRepo.RetrieveActiveToken(pbs.ctx, stub.Workspace.Id)
	if err != nil {
		return nil, err
	}

	instance, err = newBotInstance(pbs.ctx, pbs.config, pbs.scheduler, token, stub, stubConfig, botConfig, pbs.botStateManager)
	if err != nil {
		return nil, err
	}

	pbs.botInstances.Set(stubId, instance)

	// Monitor and then clean up the instance once it's done
	go instance.Start()
	go func(i *botInstance) {
		<-i.ctx.Done()
		pbs.botInstances.Delete(stubId)
	}(instance)

	return instance, nil
}

func (s *PetriBotService) SendBotMessage(ctx context.Context, in *pb.SendBotMessageRequest) (*pb.SendBotMessageResponse, error) {
	instance, err := s.getOrCreateBotInstance(in.StubId)
	if err != nil {
		return &pb.SendBotMessageResponse{Ok: false}, nil
	}

	return &pb.SendBotMessageResponse{Ok: instance.botInterface.pushInput(in.Message) == nil}, nil
}

var Keys = &keys{}

type keys struct{}

var (
	botSessionStateLock string = "bot:%s:%s:session_state_lock:%s"
	botSessionState     string = "bot:%s:%s:session_state:%s"
	botMarkerIndex      string = "bot:%s:%s:marker_index:%s"
	botMarkers          string = "bot:%s:%s:markers:%s:%s"
)

func (k *keys) botSessionStateLock(workspaceName, stubId, sessionId string) string {
	return fmt.Sprintf(botSessionStateLock, workspaceName, stubId, sessionId)
}

func (k *keys) botSessionState(workspaceName, stubId, sessionId string) string {
	return fmt.Sprintf(botSessionState, workspaceName, stubId, sessionId)
}

func (k *keys) botMarkerIndex(workspaceName, stubId, sessionId string) string {
	return fmt.Sprintf(botMarkerIndex, workspaceName, stubId, sessionId)
}

func (k *keys) botMarkers(workspaceName, stubId, sessionId, locationName string) string {
	return fmt.Sprintf(botMarkers, workspaceName, stubId, sessionId, locationName)
}
