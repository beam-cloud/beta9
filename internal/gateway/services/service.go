package gatewayservices

import (
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/scheduler"
	"github.com/beam-cloud/beta9/internal/task"
	pb "github.com/beam-cloud/beta9/proto"
)

type GatewayService struct {
	backendRepo    repository.BackendRepository
	scheduler      *scheduler.Scheduler
	taskDispatcher *task.Dispatcher
	pb.UnimplementedGatewayServiceServer
}

func NewGatewayService(backendRepo repository.BackendRepository, scheduler *scheduler.Scheduler, taskDispatcher *task.Dispatcher) (*GatewayService, error) {
	return &GatewayService{
		backendRepo:    backendRepo,
		scheduler:      scheduler,
		taskDispatcher: taskDispatcher,
	}, nil
}
