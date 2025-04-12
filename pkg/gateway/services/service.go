package gatewayservices

import (
	"context"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/task"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type GatewayService struct {
	ctx             context.Context
	appConfig       types.AppConfig
	backendRepo     repository.BackendRepository
	containerRepo   repository.ContainerRepository
	scheduler       *scheduler.Scheduler
	taskDispatcher  *task.Dispatcher
	redisClient     *common.RedisClient
	eventRepo       repository.EventRepository
	workerRepo      repository.WorkerRepository
	workerPoolRepo  repository.WorkerPoolRepository
	tailscale       *network.Tailscale
	keyEventManager *common.KeyEventManager
	pb.UnimplementedGatewayServiceServer
}

type GatewayServiceOpts struct {
	Ctx             context.Context
	Config          types.AppConfig
	BackendRepo     repository.BackendRepository
	ContainerRepo   repository.ContainerRepository
	Scheduler       *scheduler.Scheduler
	TaskDispatcher  *task.Dispatcher
	RedisClient     *common.RedisClient
	EventRepo       repository.EventRepository
	WorkerRepo      repository.WorkerRepository
	WorkerPoolRepo  repository.WorkerPoolRepository
	Tailscale       *network.Tailscale
	KeyEventManager *common.KeyEventManager
}

func NewGatewayService(opts *GatewayServiceOpts) (*GatewayService, error) {
	keyEventManager, err := common.NewKeyEventManager(opts.RedisClient)
	if err != nil {
		return nil, err
	}

	return &GatewayService{
		ctx:             opts.Ctx,
		appConfig:       opts.Config,
		backendRepo:     opts.BackendRepo,
		containerRepo:   opts.ContainerRepo,
		scheduler:       opts.Scheduler,
		taskDispatcher:  opts.TaskDispatcher,
		redisClient:     opts.RedisClient,
		eventRepo:       opts.EventRepo,
		workerRepo:      opts.WorkerRepo,
		workerPoolRepo:  opts.WorkerPoolRepo,
		tailscale:       opts.Tailscale,
		keyEventManager: keyEventManager,
	}, nil
}
