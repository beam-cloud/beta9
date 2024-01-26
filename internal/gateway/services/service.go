package gatewayservices

import (
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/scheduler"
	pb "github.com/beam-cloud/beta9/proto"
)

type GatewayService struct {
	backendRepo repository.BackendRepository
	scheduler   *scheduler.Scheduler
	pb.UnimplementedGatewayServiceServer
}

func NewGatewayService(backendRepo repository.BackendRepository, scheduler *scheduler.Scheduler) (*GatewayService, error) {
	return &GatewayService{
		backendRepo: backendRepo,
		scheduler:   scheduler,
	}, nil
}
