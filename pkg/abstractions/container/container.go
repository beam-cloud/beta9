package container

import (
	"context"

	container_cmd "github.com/beam-cloud/beta9/pkg/abstractions/container/cmd"
	container_common "github.com/beam-cloud/beta9/pkg/abstractions/container/common"
	container_tunnel "github.com/beam-cloud/beta9/pkg/abstractions/container/tunnel"
	pb "github.com/beam-cloud/beta9/proto"
)

type ContainerService interface {
	pb.ContainerServiceServer
	ExecuteCommand(in *pb.CommandExecutionRequest, stream pb.ContainerService_ExecuteCommandServer) error
	CreateTunnel(ctx context.Context, in *pb.CreateTunnelRequest) (*pb.CreateTunnelResponse, error)
}

type _ContainerService struct {
	pb.ContainerServiceServer
	cmdSvc    *container_cmd.CmdContainerService
	tunnelSvc *container_tunnel.ContainerTunnelService
}

func NewContainerService(
	ctx context.Context,
	opts container_common.ContainerServiceOpts,
) (ContainerService, error) {
	cmdSvc, err := container_cmd.NewCmdContainerService(ctx, opts)
	if err != nil {
		return nil, err
	}

	tunnelSvc, err := container_tunnel.NewContainerTunnelService(ctx, opts)
	if err != nil {
		return nil, err
	}

	svc := &_ContainerService{
		cmdSvc:    cmdSvc,
		tunnelSvc: tunnelSvc,
	}

	return svc, nil
}

func (s *_ContainerService) ExecuteCommand(in *pb.CommandExecutionRequest, stream pb.ContainerService_ExecuteCommandServer) error {
	return s.cmdSvc.ExecuteCommand(in, stream)
}

func (s *_ContainerService) CreateTunnel(ctx context.Context, in *pb.CreateTunnelRequest) (*pb.CreateTunnelResponse, error) {
	return s.tunnelSvc.CreateTunnel(ctx, in)
}
