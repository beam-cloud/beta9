package gatewayservices

import (
	"context"
	"time"

	"github.com/beam-cloud/beta9/internal/auth"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (gws GatewayService) ListContainers(ctx context.Context, in *pb.ListContainersRequest) (*pb.ListContainersResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	workspaceId := authInfo.Workspace.ExternalId

	containerStates, err := gws.containerRepo.GetActiveContainersByWorkspaceId(workspaceId)
	if err != nil {
		return &pb.ListContainersResponse{
			Ok:       false,
			ErrorMsg: "Unable to list containers",
		}, nil
	}

	containers := make([]*pb.Container, 0, len(containerStates))

	for _, state := range containerStates {
		containers = append(containers, &pb.Container{
			ContainerId: state.ContainerId,
			StubId:      state.StubId,
			WorkspaceId: state.WorkspaceId,
			Status:      string(state.Status),
			ScheduledAt: timestamppb.New(time.Unix(state.ScheduledAt, 0)),
		})
	}

	return &pb.ListContainersResponse{
		Ok:         true,
		Containers: containers,
	}, nil
}
