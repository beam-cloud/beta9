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
	ctx              context.Context
	appConfig        types.AppConfig
	backendRepo      repository.BackendRepository
	containerRepo    repository.ContainerRepository
	providerRepo     repository.ProviderRepository
	scheduler        *scheduler.Scheduler
	taskDispatcher   *task.Dispatcher
	redisClient      *common.RedisClient
	eventRepo        repository.EventRepository
	workerRepo       repository.WorkerRepository
	workerPoolRepo   repository.WorkerPoolRepository
	usageMetricsRepo repository.UsageMetricsRepository
	tailscale        *network.Tailscale
	keyEventManager  *common.KeyEventManager
	pb.UnimplementedGatewayServiceServer
}

type GatewayServiceOpts struct {
	Ctx              context.Context
	Config           types.AppConfig
	BackendRepo      repository.BackendRepository
	ContainerRepo    repository.ContainerRepository
	ProviderRepo     repository.ProviderRepository
	Scheduler        *scheduler.Scheduler
	TaskDispatcher   *task.Dispatcher
	RedisClient      *common.RedisClient
	EventRepo        repository.EventRepository
	WorkerRepo       repository.WorkerRepository
	WorkerPoolRepo   repository.WorkerPoolRepository
	UsageMetricsRepo repository.UsageMetricsRepository
	Tailscale        *network.Tailscale
	KeyEventManager  *common.KeyEventManager
}

func NewGatewayService(opts *GatewayServiceOpts) (*GatewayService, error) {
	keyEventManager, err := common.NewKeyEventManager(opts.RedisClient)
	if err != nil {
		return nil, err
	}

	return &GatewayService{
		ctx:              opts.Ctx,
		appConfig:        opts.Config,
		backendRepo:      opts.BackendRepo,
		containerRepo:    opts.ContainerRepo,
		providerRepo:     opts.ProviderRepo,
		scheduler:        opts.Scheduler,
		taskDispatcher:   opts.TaskDispatcher,
		redisClient:      opts.RedisClient,
		eventRepo:        opts.EventRepo,
		workerRepo:       opts.WorkerRepo,
		workerPoolRepo:   opts.WorkerPoolRepo,
		usageMetricsRepo: opts.UsageMetricsRepo,
		tailscale:        opts.Tailscale,
		keyEventManager:  keyEventManager,
	}, nil
}
