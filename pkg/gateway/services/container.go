package gatewayservices

import (
	"context"
	"errors"
	"fmt"
	"io"
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

	err = gws.scheduler.Stop(&types.StopContainerArgs{ContainerId: in.ContainerId, Reason: types.StopContainerReasonUser})
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

func (gws *GatewayService) AttachToContainer(stream pb.GatewayService_AttachToContainerServer) error {
	ctx := stream.Context()
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	initMsg, err := stream.Recv()
	if err != nil {
		return err
	}

	containerNotFoundResponse := &pb.AttachToContainerResponse{
		Done:     true,
		ExitCode: 1,
		Output:   "Container not found",
	}

	attachReq := initMsg.GetAttachRequest()
	if attachReq == nil {
		return stream.Send(containerNotFoundResponse)
	}

	container, err := gws.containerRepo.GetContainerState(attachReq.ContainerId)
	if err != nil {
		return stream.Send(containerNotFoundResponse)
	}

	stub, err := gws.backendRepo.GetStubByExternalId(ctx, container.StubId)
	if err != nil || stub == nil {
		return stream.Send(containerNotFoundResponse)
	}

	serveTimeout := types.DefaultServeContainerTimeout

	if types.StubType(stub.Type).IsServe() {
		lockKey := common.RedisKeys.SchedulerServeLock(stub.Workspace.Name, stub.ExternalId)
		timeoutValue, err := gws.redisClient.Get(context.Background(), lockKey).Result()
		if err == nil {
			serveTimeout, _ = time.ParseDuration(timeoutValue)
			if serveTimeout <= 0 {
				serveTimeout = types.DefaultServeContainerTimeout
			}
		}

		// Delete the serve lock key when we detach from the container
		defer func() {
			gws.redisClient.Del(context.Background(), lockKey)
		}()
	}

	sendCallback := func(o common.OutputMsg) error {
		return stream.Send(&pb.AttachToContainerResponse{
			Output: o.Msg,
		})
	}

	exitCallback := func(exitCode int32) error {
		output := fmt.Sprintf("\nContainer was stopped.\n\nExit code: %d", exitCode)
		if exitCode != 0 {
			exitCodeMessage, ok := types.ExitCodeMessages[types.ContainerExitCode(exitCode)]
			if ok {
				output = exitCodeMessage
			}
		}
		return stream.Send(&pb.AttachToContainerResponse{
			Done:     true,
			ExitCode: exitCode,
			Output:   output,
		})
	}

	ctx, cancel := common.MergeContexts(gws.ctx, ctx)
	defer cancel()

	syncQueue := make(chan *pb.SyncContainerWorkspaceRequest)

	containerStream, err := abstractions.NewContainerStream(abstractions.ContainerStreamOpts{
		SendCallback:    sendCallback,
		ExitCallback:    exitCallback,
		ContainerRepo:   gws.containerRepo,
		Config:          gws.appConfig,
		Tailscale:       gws.tailscale,
		KeyEventManager: gws.keyEventManager,
		SyncQueue:       syncQueue,
	})
	if err != nil {
		return err
	}

	// Run the container stream async
	streamErrCh := make(chan error, 1)
	go func() {
		streamErrCh <- containerStream.Stream(ctx, authInfo, container.ContainerId)
	}()

	// RX incoming client messages
	clientMsgErrCh := make(chan error, 1)
	go func() {
		for {
			inMsg, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					clientMsgErrCh <- nil
				} else {
					clientMsgErrCh <- err
				}
				return
			}

			switch payload := inMsg.Payload.(type) {
			case *pb.ContainerStreamMessage_SyncContainerWorkspace:
				if types.StubType(stub.Type).IsServe() {
					gws.redisClient.Expire(ctx, common.RedisKeys.SchedulerServeLock(stub.Workspace.Name, stub.ExternalId), serveTimeout)
				}

				syncQueue <- payload.SyncContainerWorkspace
			default:
			}
		}
	}()

	// Wait for the container stream or the client message loop to finish
	select {
	case err := <-streamErrCh:
		return err
	case err := <-clientMsgErrCh:
		cancel()
		return err
	}
}
