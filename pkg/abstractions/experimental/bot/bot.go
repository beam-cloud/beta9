package bot

import (
	"encoding/json"
	"errors"
	"fmt"

	"context"

	"github.com/labstack/echo/v4"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"

	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/task"
	pb "github.com/beam-cloud/beta9/proto"
)

const (
	botRoutePrefix string = "/bot"
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
	StartBotServe(ctx context.Context, in *pb.StartBotServeRequest) (*pb.StartBotServeResponse, error)
	BotServeKeepAlive(ctx context.Context, in *pb.BotServeKeepAliveRequest) (*pb.BotServeKeepAliveResponse, error)
}

type PetriBotService struct {
	pb.UnimplementedBotServiceServer
	ctx             context.Context
	config          types.AppConfig
	rdb             *common.RedisClient
	keyEventManager *common.KeyEventManager
	routeGroup      *echo.Group
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
		routeGroup:      opts.RouteGroup,
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

	// Register HTTP routes
	authMiddleware := auth.AuthMiddleware(pbs.backendRepo, pbs.workspaceRepo)
	registerBotRoutes(pbs.routeGroup.Group(botRoutePrefix, authMiddleware), pbs)

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

	instance, err = newBotInstance(pbs.ctx, botInstanceOpts{
		AppConfig:      pbs.config,
		Scheduler:      pbs.scheduler,
		Token:          token,
		Stub:           stub,
		StubConfig:     stubConfig,
		BotConfig:      botConfig,
		StateManager:   pbs.botStateManager,
		TaskDispatcher: pbs.taskDispatcher,
	})
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

var Keys = &keys{}

type keys struct{}

var (
	botLock            string = "bot:%s:%s:session_state_lock:%s"
	botInputBuffer     string = "bot:%s:%s:input_buffer:%s"
	botOutputBuffer    string = "bot:%s:%s:output_buffer:%s"
	botSessionIndex    string = "bot:%s:%s:session_index"
	botSessionState    string = "bot:%s:%s:session_state:%s"
	botMarkers         string = "bot:%s:%s:markers:%s:%s"
	botTransitionTasks string = "bot:%s:%s:transition_tasks:%s:%s"
)

func (k *keys) botLock(workspaceName, stubId, sessionId string) string {
	return fmt.Sprintf(botLock, workspaceName, stubId, sessionId)
}

func (k *keys) botSessionIndex(workspaceName, stubId string) string {
	return fmt.Sprintf(botSessionIndex, workspaceName, stubId)
}

func (k *keys) botInputBuffer(workspaceName, stubId, sessionId string) string {
	return fmt.Sprintf(botInputBuffer, workspaceName, stubId, sessionId)
}

func (k *keys) botOutputBuffer(workspaceName, stubId, sessionId string) string {
	return fmt.Sprintf(botOutputBuffer, workspaceName, stubId, sessionId)
}

func (k *keys) botSessionState(workspaceName, stubId, sessionId string) string {
	return fmt.Sprintf(botSessionState, workspaceName, stubId, sessionId)
}

func (k *keys) botMarkers(workspaceName, stubId, sessionId, locationName string) string {
	return fmt.Sprintf(botMarkers, workspaceName, stubId, sessionId, locationName)
}

func (k *keys) botTransitionTasks(workspaceName, stubId, sessionId, transitionName string) string {
	return fmt.Sprintf(botTransitionTasks, workspaceName, stubId, sessionId, transitionName)
}
