package gatewayservices

import (
	"context"
	"errors"
	"fmt"
	"time"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/rs/zerolog/log"
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

	containers := []*pb.Container{}
	for _, state := range containerStates {
		deploymentId := ""
		deployment, err := gws.backendRepo.GetDeploymentByStubExternalId(ctx, authInfo.Workspace.Id, state.StubId)
		if err == nil && deployment != nil {
			deploymentId = deployment.ExternalId
		}

		containers = append(containers, &pb.Container{
			ContainerId:  state.ContainerId,
			StubId:       state.StubId,
			WorkspaceId:  state.WorkspaceId,
			Status:       string(state.Status),
			ScheduledAt:  timestamppb.New(time.Unix(state.ScheduledAt, 0)),
			StartedAt:    timestamppb.New(time.Unix(state.StartedAt, 0)),
			WorkerId:     containerWorkerMap[state.ContainerId].WorkerId,
			MachineId:    containerWorkerMap[state.ContainerId].MachineId,
			DeploymentId: deploymentId,
		})
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
			ErrorMsg: fmt.Sprintf("Container not found: %s", in.ContainerId),
		}, nil
	}

	if state.WorkspaceId != workspaceId {
		return &pb.StopContainerResponse{
			Ok:       false,
			ErrorMsg: fmt.Sprintf("Container not found: %s", in.ContainerId),
		}, nil
	}

	err = gws.scheduler.Stop(&types.StopContainerArgs{ContainerId: in.ContainerId})
	if err != nil {
		log.Error().Err(err).Msg("unable to stop container")
		return &pb.StopContainerResponse{
			Ok:       false,
			ErrorMsg: fmt.Sprintf("Unable to stop container: %s", in.ContainerId),
		}, nil
	}

	return &pb.StopContainerResponse{
		Ok: true,
	}, nil
}

func (gws *GatewayService) AttachToContainer(in *pb.AttachToContainerRequest, stream pb.GatewayService_AttachToContainerServer) error {
	ctx := stream.Context()
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	container, err := gws.containerRepo.GetContainerState(in.ContainerId)
	if err != nil {
		return stream.Send(&pb.AttachToContainerResponse{
			Done:     true,
			ExitCode: 1,
			Output:   "Container not found",
		})
	}

	sendCallback := func(o common.OutputMsg) error {
		if err := stream.Send(&pb.AttachToContainerResponse{Output: o.Msg}); err != nil {
			return err
		}

		return nil
	}

	exitCallback := func(exitCode int32) error {
		output := "\nContainer was stopped."
		if exitCode != 0 {
			output = fmt.Sprintf("Container failed with exit code %d", exitCode)
			if exitCode == types.WorkerContainerExitCodeOomKill {
				output = "Container was killed due to an out-of-memory error"
			}
		}

		return stream.Send(&pb.AttachToContainerResponse{
			Done:     true,
			ExitCode: int32(exitCode),
			Output:   output,
		})
	}

	ctx, cancel := common.MergeContexts(gws.ctx, ctx)
	defer cancel()

	logStream, err := abstractions.NewLogStream(abstractions.LogStreamOpts{
		SendCallback:    sendCallback,
		ExitCallback:    exitCallback,
		ContainerRepo:   gws.containerRepo,
		Config:          gws.appConfig,
		Tailscale:       gws.tailscale,
		KeyEventManager: gws.keyEventManager,
	})
	if err != nil {
		return err
	}

	return logStream.Stream(ctx, authInfo, container.ContainerId)
}
