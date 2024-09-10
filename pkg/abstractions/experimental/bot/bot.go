package bot

import (
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

type BotConfig struct {
	Locations   map[string]BotLocationConfig   `json:"locations"`
	Transitions map[string]BotTransitionConfig `json:"transitions"`
}

type BotLocationConfig struct {
	Name string `json:"name"`
}

type BotTransitionConfig struct {
	Cpu         int64          `json:"cpu"`
	Gpu         types.GpuType  `json:"gpu"`
	Memory      int64          `json:"memory"`
	ImageId     string         `json:"image_id"`
	Timeout     int            `json:"timeout"`
	KeepWarm    int            `json:"keep_warm"`
	MaxPending  int            `json:"max_pending"`
	Volumes     []string       `json:"volumes"`
	Secrets     []string       `json:"secrets"`
	Handler     string         `json:"handler"`
	CallbackUrl string         `json:"callback_url"`
	TaskPolicy  string         `json:"task_policy"`
	Name        string         `json:"name"`
	Inputs      map[string]int `json:"inputs"`
}

type BotService interface {
	pb.BotServiceServer
	StartBotServe(in *pb.StartBotServeRequest, stream pb.BotService_StartBotServeServer) error
	StopBotServe(ctx context.Context, in *pb.StopBotServeRequest) (*pb.StopBotServeResponse, error)
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
}

func NewPetriBotService(ctx context.Context, opts BotServiceOpts) (BotService, error) {
	keyEventManager, err := common.NewKeyEventManager(opts.RedisClient)
	if err != nil {
		return nil, err
	}

	return &PetriBotService{
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
	}, nil
}
