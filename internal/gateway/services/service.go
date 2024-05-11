package gatewayservices

import (
	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/scheduler"
	"github.com/beam-cloud/beta9/internal/task"
	"github.com/beam-cloud/beta9/internal/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type GatewayService struct {
	appConfig      types.AppConfig
	backendRepo    repository.BackendRepository
	containerRepo  repository.ContainerRepository
	scheduler      *scheduler.Scheduler
	taskDispatcher *task.Dispatcher
	redisClient    *common.RedisClient
	pb.UnimplementedGatewayServiceServer
}

func NewGatewayService(appConfig types.AppConfig, backendRepo repository.BackendRepository, containerRepo repository.ContainerRepository, scheduler *scheduler.Scheduler, taskDispatcher *task.Dispatcher, redisClient *common.RedisClient) (*GatewayService, error) {
	return &GatewayService{
		appConfig:      appConfig,
		backendRepo:    backendRepo,
		containerRepo:  containerRepo,
		scheduler:      scheduler,
		taskDispatcher: taskDispatcher,
		redisClient:    redisClient,
	}, nil
}
