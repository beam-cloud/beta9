package gatewayservices

import (
	"context"
	"fmt"
	"sync"

	"github.com/beam-cloud/beta9/pkg/common"
	computesvc "github.com/beam-cloud/beta9/pkg/gateway/services/compute"
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
	computeRepo      repository.ComputeRepository
	computeService   *computesvc.Service
	usageMetricsRepo repository.UsageMetricsRepository
	tailscale        *network.Tailscale
	keyEventManager  *common.KeyEventManager
	clientCache      *sync.Map
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
	ComputeRepo      repository.ComputeRepository
	ComputeService   *computesvc.Service
	UsageMetricsRepo repository.UsageMetricsRepository
	Tailscale        *network.Tailscale
	KeyEventManager  *common.KeyEventManager
}

func NewGatewayService(opts *GatewayServiceOpts) (*GatewayService, error) {
	keyEventManager, err := common.NewKeyEventManager(opts.RedisClient)
	if err != nil {
		return nil, err
	}
	computeRepo := opts.ComputeRepo
	if computeRepo == nil {
		if opts.RedisClient == nil {
			return nil, fmt.Errorf("compute repository requires redis client")
		}
		computeRepo = repository.NewComputeRedisRepository(opts.RedisClient)
	}
	computeService := opts.ComputeService
	if computeService == nil {
		computeService = computesvc.New(computesvc.Options{
			Config:           opts.Config,
			BackendRepo:      opts.BackendRepo,
			ContainerRepo:    opts.ContainerRepo,
			Scheduler:        opts.Scheduler,
			EventRepo:        opts.EventRepo,
			WorkerRepo:       opts.WorkerRepo,
			WorkerPoolRepo:   opts.WorkerPoolRepo,
			UsageMetricsRepo: opts.UsageMetricsRepo,
			ComputeRepo:      computeRepo,
			KeyEventManager:  keyEventManager,
			RedisClient:      opts.RedisClient,
			Tailscale:        opts.Tailscale,
		})
		computeService.Start(opts.Ctx)
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
		computeRepo:      computeRepo,
		computeService:   computeService,
		usageMetricsRepo: opts.UsageMetricsRepo,
		tailscale:        opts.Tailscale,
		keyEventManager:  keyEventManager,
		clientCache:      &sync.Map{},
	}, nil
}
