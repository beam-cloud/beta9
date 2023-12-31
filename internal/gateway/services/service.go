package gatewayservices

import (
	"github.com/beam-cloud/beam/internal/repository"
	pb "github.com/beam-cloud/beam/proto"
)

type GatewayService struct {
	backendRepo repository.BackendRepository
	pb.UnimplementedGatewayServiceServer
}

func NewGatewayService(backendRepo repository.BackendRepository) (*GatewayService, error) {
	return &GatewayService{
		backendRepo: backendRepo,
	}, nil
}
