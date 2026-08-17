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
	"github.com/beam-cloud/beta9/pkg/network"
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
			ScheduledAt:  containerTimestamp(state.ScheduledAt),
			StartedAt:    containerTimestamp(state.StartedAt),
			WorkerId:     firstContainerDetail(state.WorkerId, containerWorkerMap[state.ContainerId].WorkerId),
			MachineId:    firstContainerDetail(state.MachineId, containerWorkerMap[state.ContainerId].MachineId),
			DeploymentId: deploymentId,
		})
	}

	return &pb.ListContainersResponse{
		Ok:         true,
		Containers: containers,
	}, nil
}

func firstContainerDetail(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func containerTimestamp(unixSeconds int64) *timestamppb.Timestamp {
	if unixSeconds <= 0 {
		return nil
	}
	return timestamppb.New(time.Unix(unixSeconds, 0))
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

func (gws GatewayService) SnapshotContainerState(ctx context.Context, in *pb.GatewaySnapshotContainerStateRequest) (*pb.GatewaySnapshotContainerStateResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceId := authInfo.Workspace.ExternalId

	if !auth.HasPermission(authInfo) {
		return &pb.GatewaySnapshotContainerStateResponse{
			Ok:       false,
			ErrorMsg: "Unauthorized Access",
		}, nil
	}
	if in.OperationId == "" {
		return &pb.GatewaySnapshotContainerStateResponse{ErrorMsg: "operation_id is required"}, nil
	}
	if in.Mode != "live" && in.Mode != "terminal" {
		return &pb.GatewaySnapshotContainerStateResponse{ErrorMsg: "mode must be live or terminal"}, nil
	}
	if in.Mode == "live" && in.IncludeMemory {
		return &pb.GatewaySnapshotContainerStateResponse{ErrorMsg: "live snapshots cannot include memory"}, nil
	}
	if in.Publish {
		return &pb.GatewaySnapshotContainerStateResponse{ErrorMsg: "public whole-root state publishing is disabled"}, nil
	}

	var existing *types.StateSnapshot
	existing, err := gws.backendRepo.GetStateSnapshotByOperationForWorkspace(ctx, authInfo.Workspace.Id, in.ContainerId, in.OperationId)
	if err == nil {
		if existing.Mode != in.Mode || existing.IncludeMemory != in.IncludeMemory || existing.Visible != in.Visible {
			return &pb.GatewaySnapshotContainerStateResponse{ErrorMsg: "operation_id conflicts with different immutable snapshot inputs"}, nil
		}
		if existing.Status == types.StateSnapshotStatusAvailable || existing.Status == types.StateSnapshotStatusFailed {
			return gatewayStateSnapshotResponse(existing), nil
		}
	} else {
		var notFound *types.ErrStateSnapshotNotFound
		if !errors.As(err, &notFound) {
			return &pb.GatewaySnapshotContainerStateResponse{ErrorMsg: err.Error()}, nil
		}
		existing = nil
	}

	client, _, err := gws.getClient(ctx, in.ContainerId, authInfo.Token.Key, workspaceId)
	if err != nil {
		if existing != nil && existing.Status == types.StateSnapshotStatusPending {
			return gatewayStateSnapshotResponse(existing), nil
		}
		return &pb.GatewaySnapshotContainerStateResponse{
			Ok:       false,
			ErrorMsg: fmt.Sprintf("unable to snapshot container state: %s", in.ContainerId),
		}, nil
	}

	resp, err := client.SnapshotContainerState(ctx, in.ContainerId, common.SnapshotContainerStateOptions{
		OperationId: in.OperationId, Mode: in.Mode, Publish: in.Publish,
		IncludeMemory: in.IncludeMemory, Visible: in.Visible,
	})
	if err != nil {
		return &pb.GatewaySnapshotContainerStateResponse{
			Ok:       false,
			ErrorMsg: fmt.Sprintf("unable to snapshot container state: %v", err),
		}, nil
	}

	if !resp.Ok {
		return &pb.GatewaySnapshotContainerStateResponse{
			Ok:       false,
			ErrorMsg: fmt.Sprintf("unable to snapshot container state: %s", resp.ErrorMsg),
		}, nil
	}

	return &pb.GatewaySnapshotContainerStateResponse{
		Ok: true, StateSnapshotId: resp.StateSnapshotId, Status: resp.Status,
		ImageDigest: resp.ImageDigest, RuntimeProfile: resp.RuntimeProfile,
		CheckpointId: resp.CheckpointId, HasMemory: resp.HasMemory,
		Generations: resp.Generations, RestoreMode: resp.RestoreMode,
		FallbackReason: resp.FallbackReason,
	}, nil
}

func gatewayStateSnapshotResponse(snapshot *types.StateSnapshot) *pb.GatewaySnapshotContainerStateResponse {
	if snapshot == nil {
		return &pb.GatewaySnapshotContainerStateResponse{ErrorMsg: "state snapshot is unavailable"}
	}
	generations := make([]*pb.StateGeneration, 0, len(snapshot.Generations))
	for _, generation := range snapshot.Generations {
		generations = append(generations, &pb.StateGeneration{
			VolumeId: generation.VolumeId, GenerationId: generation.GenerationId,
			ParentGenerationId:      generation.ParentGenerationId,
			CloneParentGenerationId: generation.CloneParentGenerationId,
			Name:                    generation.Name, MountPath: generation.MountPath, ReadOnly: generation.ReadOnly,
			Root: generation.Root, Generation: generation.Generation,
		})
	}
	return &pb.GatewaySnapshotContainerStateResponse{
		Ok: snapshot.Status != types.StateSnapshotStatusFailed, ErrorMsg: snapshot.Reason,
		StateSnapshotId: snapshot.ExternalId, Status: string(snapshot.Status),
		ImageDigest: snapshot.ImageDigest, RuntimeProfile: snapshot.RuntimeProfile,
		CheckpointId: snapshot.CheckpointId, HasMemory: snapshot.CheckpointId != "",
		Generations: generations, RestoreMode: snapshot.RestoreMode, FallbackReason: snapshot.FallbackReason,
	}
}

func (gws GatewayService) RetainStateSnapshotReference(ctx context.Context, in *pb.StateSnapshotReferenceRequest) (*pb.StateSnapshotReferenceResponse, error) {
	authInfo, ok := auth.AuthInfoFromContext(ctx)
	if !ok || authInfo == nil || authInfo.Workspace == nil || !auth.HasPermission(authInfo) {
		return &pb.StateSnapshotReferenceResponse{ErrorMsg: "Unauthorized Access"}, nil
	}
	if in == nil {
		return &pb.StateSnapshotReferenceResponse{ErrorMsg: "state snapshot reference is required"}, nil
	}
	reference, err := gws.backendRepo.RetainStateSnapshotReference(ctx, authInfo.Workspace.Id,
		in.StateSnapshotId, in.Kind, in.ReferenceId)
	if err != nil {
		return &pb.StateSnapshotReferenceResponse{ErrorMsg: err.Error()}, nil
	}
	return &pb.StateSnapshotReferenceResponse{Ok: true, StateSnapshotId: reference.SnapshotExternalId,
		Kind: reference.Kind, ReferenceId: reference.ReferenceId, Status: "active"}, nil
}

func (gws GatewayService) ReleaseStateSnapshotReference(ctx context.Context, in *pb.StateSnapshotReferenceRequest) (*pb.StateSnapshotReferenceResponse, error) {
	authInfo, ok := auth.AuthInfoFromContext(ctx)
	if !ok || authInfo == nil || authInfo.Workspace == nil || !auth.HasPermission(authInfo) {
		return &pb.StateSnapshotReferenceResponse{ErrorMsg: "Unauthorized Access"}, nil
	}
	if in == nil {
		return &pb.StateSnapshotReferenceResponse{ErrorMsg: "state snapshot reference is required"}, nil
	}
	reference, err := gws.backendRepo.ReleaseStateSnapshotReference(ctx, authInfo.Workspace.Id,
		in.StateSnapshotId, in.Kind, in.ReferenceId)
	if err != nil {
		return &pb.StateSnapshotReferenceResponse{ErrorMsg: err.Error()}, nil
	}
	return &pb.StateSnapshotReferenceResponse{Ok: true, StateSnapshotId: reference.SnapshotExternalId,
		Kind: reference.Kind, ReferenceId: reference.ReferenceId, Status: "released"}, nil
}

func (gws *GatewayService) getClient(ctx context.Context, containerId, token string, workspaceId string) (*common.ContainerClient, *types.ContainerState, error) {
	container, err := gws.containerRepo.GetContainerState(containerId)
	if err != nil {
		return nil, nil, err
	}

	if container == nil {
		return nil, nil, errors.New("container not found")
	}

	if container.WorkspaceId != workspaceId {
		return nil, nil, errors.New("invalid workspace")
	}

	cacheKey := containerId + ":" + token
	if cached, ok := gws.clientCache.Load(cacheKey); ok {
		if client, ok := cached.(*common.ContainerClient); ok {
			return client, container, nil
		}
	}

	hostname, err := gws.containerRepo.GetWorkerAddress(ctx, containerId)
	if err != nil {
		return nil, nil, err
	}

	conn, err := network.ConnectToBackend(ctx, hostname, time.Second*30, gws.tailscale, gws.appConfig.Tailscale, gws.containerRepo)
	if err != nil {
		return nil, nil, err
	}

	client, err := common.NewContainerClient(hostname, token, conn)
	if err != nil {
		return nil, nil, err
	}

	gws.clientCache.Store(cacheKey, client)
	return client, container, nil
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

	if isAdmin, _ := isClusterAdmin(ctx); state.WorkspaceId != workspaceId && !isAdmin {
		return &pb.StopContainerResponse{
			Ok:       false,
			ErrorMsg: fmt.Sprintf("Container not found: %s", in.ContainerId),
		}, nil
	}

	err = gws.scheduler.Stop(&types.StopContainerArgs{ContainerId: in.ContainerId, Reason: types.StopContainerReasonUser})
	if err != nil {
		return &pb.StopContainerResponse{
			Ok:       false,
			ErrorMsg: fmt.Sprintf("Unable to stop container: %s", in.ContainerId),
		}, nil
	}

	return &pb.StopContainerResponse{
		Ok: true,
	}, nil
}

const (
	containerStreamKeepaliveInterval = 10 * time.Second
)

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

	if !auth.HasInteractivePermission(authInfo) || authInfo.Workspace == nil {
		return stream.Send(containerNotFoundResponse)
	}

	container, err := gws.containerRepo.GetContainerState(attachReq.ContainerId)
	if err != nil || container == nil || container.WorkspaceId != authInfo.Workspace.ExternalId {
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

	// Send periodic keepalive messages to the client to keep the connection alive
	keepaliveTicker := time.NewTicker(containerStreamKeepaliveInterval)
	defer keepaliveTicker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-keepaliveTicker.C:
				stream.Send(&pb.AttachToContainerResponse{
					Output: "",
				})
			}
		}
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
				syncRequest := bindSyncRequestToContainer(payload.SyncContainerWorkspace, container.ContainerId)
				if syncRequest == nil {
					continue
				}

				if types.StubType(stub.Type).IsServe() {
					gws.redisClient.Expire(ctx, common.RedisKeys.SchedulerServeLock(stub.Workspace.Name, stub.ExternalId), serveTimeout)
				}

				syncQueue <- syncRequest
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

func bindSyncRequestToContainer(request *pb.SyncContainerWorkspaceRequest, containerId string) *pb.SyncContainerWorkspaceRequest {
	if request == nil {
		return nil
	}

	boundRequest := *request
	boundRequest.ContainerId = containerId
	return &boundRequest
}
