package gatewayservices

import (
	"context"
	"errors"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (gws GatewayService) ListContainers(ctx context.Context, in *pb.ListContainersRequest) (*pb.ListContainersResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	var (
		workspaceId        = authInfo.Workspace.ExternalId
		containerStates    = []types.ContainerState{}
		containerWorkerMap = map[string]containerDetails{}
	)

	var err error
	if isAdmin, _ := isClusterAdmin(ctx); isAdmin {
		containerStates, containerWorkerMap, err = gws.getContainersAsAdmin()
		if err != nil {
			return &pb.ListContainersResponse{Ok: false, ErrorMsg: err.Error()}, nil
		}
	} else {
		containerStates, err = gws.containerRepo.GetActiveContainersByWorkspaceId(workspaceId)
		if err != nil {
			return &pb.ListContainersResponse{Ok: false, ErrorMsg: "Unable to list containers"}, nil
		}
	}

	containers := make([]*pb.Container, len(containerStates))
	for i, state := range containerStates {
		containers[i] = &pb.Container{
			ContainerId: state.ContainerId,
			StubId:      state.StubId,
			WorkspaceId: state.WorkspaceId,
			Status:      string(state.Status),
			ScheduledAt: timestamppb.New(time.Unix(state.ScheduledAt, 0)),
			WorkerId:    containerWorkerMap[state.ContainerId].WorkerId,
			MachineId:   containerWorkerMap[state.ContainerId].MachineId,
		}
	}

	return &pb.ListContainersResponse{
		Ok:         true,
		Containers: containers,
	}, nil
}

type containerDetails struct {
	WorkerId  string
	MachineId string
}

func (gws GatewayService) getContainersAsAdmin() ([]types.ContainerState, map[string]containerDetails, error) {
	workers, err := gws.workerRepo.GetAllWorkers()
	if err != nil {
		return nil, nil, errors.New("unable to list workers")
	}

	containerStates := []types.ContainerState{}
	containerWorkerMap := map[string]containerDetails{}

	for _, worker := range workers {
		states, err := gws.containerRepo.GetActiveContainersByWorkerId(worker.Id)
		if err != nil {
			return nil, nil, errors.New("unable to list containers")
		}

		containerStates = append(containerStates, states...)

		for _, state := range states {
			containerWorkerMap[state.ContainerId] = containerDetails{WorkerId: worker.Id, MachineId: worker.MachineId}
		}
	}

	return containerStates, containerWorkerMap, nil
}

func (gws GatewayService) StopContainer(ctx context.Context, in *pb.StopContainerRequest) (*pb.StopContainerResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	workspaceId := authInfo.Workspace.ExternalId

	state, err := gws.containerRepo.GetContainerState(in.ContainerId)
	if err != nil {
		return &pb.StopContainerResponse{
			Ok:       false,
			ErrorMsg: "Container not found",
		}, nil
	}

	if state.WorkspaceId != workspaceId {
		return &pb.StopContainerResponse{
			Ok:       false,
			ErrorMsg: "Container not found",
		}, nil
	}

	err = gws.scheduler.Stop(&types.StopContainerArgs{ContainerId: in.ContainerId})
	if err != nil {
		return &pb.StopContainerResponse{
			Ok:       false,
			ErrorMsg: "Unable to stop container",
		}, nil
	}

	return &pb.StopContainerResponse{
		Ok: true,
	}, nil
}
