package gatewayservices

import (
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/task"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type GatewayService struct {
	appConfig      types.AppConfig
	backendRepo    repository.BackendRepository
	containerRepo  repository.ContainerRepository
	providerRepo   repository.ProviderRepository
	scheduler      *scheduler.Scheduler
	taskDispatcher *task.Dispatcher
	redisClient    *common.RedisClient
	eventRepo      repository.EventRepository
	workerRepo     repository.WorkerRepository
	workerPoolRepo repository.WorkerPoolRepository
	pb.UnimplementedGatewayServiceServer
}

type GatewayServiceOpts struct {
	Config         types.AppConfig
	BackendRepo    repository.BackendRepository
	ContainerRepo  repository.ContainerRepository
	ProviderRepo   repository.ProviderRepository
	Scheduler      *scheduler.Scheduler
	TaskDispatcher *task.Dispatcher
	RedisClient    *common.RedisClient
	EventRepo      repository.EventRepository
	WorkerRepo     repository.WorkerRepository
	WorkerPoolRepo repository.WorkerPoolRepository
}

func NewGatewayService(opts *GatewayServiceOpts) (*GatewayService, error) {
	return &GatewayService{
		appConfig:      opts.Config,
		backendRepo:    opts.BackendRepo,
		containerRepo:  opts.ContainerRepo,
		providerRepo:   opts.ProviderRepo,
		scheduler:      opts.Scheduler,
		taskDispatcher: opts.TaskDispatcher,
		redisClient:    opts.RedisClient,
		eventRepo:      opts.EventRepo,
		workerRepo:     opts.WorkerRepo,
		workerPoolRepo: opts.WorkerPoolRepo,
	}, nil
}
