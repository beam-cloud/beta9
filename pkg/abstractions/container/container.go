package container

import (
	"context"

	container_cmd "github.com/beam-cloud/beta9/pkg/abstractions/container/cmd"
	container_common "github.com/beam-cloud/beta9/pkg/abstractions/container/common"
	pb "github.com/beam-cloud/beta9/proto"
)

type ContainerService interface {
	pb.ContainerServiceServer
	ExecuteCommand(in *pb.CommandExecutionRequest, stream pb.ContainerService_ExecuteCommandServer) error
}

type ContainerServiceImpl struct {
	pb.ContainerServiceServer
}

func NewContainerService(
	ctx context.Context,
	opts container_common.ContainerServiceOpts,
) (ContainerService, error) {
	cmdService, err := container_cmd.NewCmdContainerService(ctx, opts)
	if err != nil {
		return nil, err
	}

	svc := ContainerServiceImpl{
		cmdService,
	}

	return svc, nil
}
