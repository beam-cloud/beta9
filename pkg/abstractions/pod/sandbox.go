package pod

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/metrics"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	sandboxConnectWorkerAddressTimeout = 1500 * time.Millisecond
	sandboxConnectWorkerDialTimeout    = 2 * time.Second
	sandboxStatusMetadataTimeout       = 500 * time.Millisecond
)

func (s *GenericPodService) SandboxExec(ctx context.Context, in *pb.PodSandboxExecRequest) (*pb.PodSandboxExecResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	cacheKey := sandboxClientCacheKey(in.ContainerId, authInfo.Token.Key)

	client, _, err := s.getClient(ctx, in.ContainerId, authInfo.Token.Key, authInfo.Workspace.ExternalId)
	if err != nil {
		logSandboxConnectFailure(err, in.ContainerId)
		return &pb.PodSandboxExecResponse{
			Ok:       false,
			ErrorMsg: sandboxConnectErrorMessage(err),
		}, nil
	}

	resp, err := client.SandboxExecContext(ctx, in.ContainerId, in.Command, in.Env, in.Cwd)
	if err != nil && isTransientSandboxConnectFailure(err) {
		s.evictClient(cacheKey)
		client, _, retryErr := s.getClient(ctx, in.ContainerId, authInfo.Token.Key, authInfo.Workspace.ExternalId)
		if retryErr != nil {
			logSandboxConnectFailure(retryErr, in.ContainerId)
			err = retryErr
		} else {
			resp, err = client.SandboxExecContext(ctx, in.ContainerId, in.Command, in.Env, in.Cwd)
		}
	}
	if err != nil {
		return &pb.PodSandboxExecResponse{
			Ok:       false,
			ErrorMsg: "Failed to execute command",
		}, nil
	}

	if !resp.Ok {
		return &pb.PodSandboxExecResponse{
			Ok:       false,
			ErrorMsg: resp.ErrorMsg,
		}, nil
	}

	return &pb.PodSandboxExecResponse{
		Ok:  true,
		Pid: resp.Pid,
	}, nil
}

func (s *GenericPodService) SandboxStatus(ctx context.Context, in *pb.PodSandboxStatusRequest) (*pb.PodSandboxStatusResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	if in.Pid == 0 {
		container, err := s.getSandboxContainerStateForStatus(ctx, in.ContainerId)
		if err != nil || container == nil {
			return &pb.PodSandboxStatusResponse{
				Ok:       false,
				ErrorMsg: "Failed to get sandbox status",
			}, nil
		}

		if container.WorkspaceId != authInfo.Workspace.ExternalId {
			return &pb.PodSandboxStatusResponse{
				Ok:       false,
				ErrorMsg: "Failed to get sandbox status",
			}, nil
		}

		status := "pending"
		switch container.Status {
		case types.ContainerStatusRunning:
			status = "running"
		case types.ContainerStatusStopping:
			status = "stopping"
		}

		return &pb.PodSandboxStatusResponse{
			Ok:       true,
			Status:   status,
			ExitCode: -1,
		}, nil
	}

	cacheKey := sandboxClientCacheKey(in.ContainerId, authInfo.Token.Key)

	client, _, err := s.getClient(ctx, in.ContainerId, authInfo.Token.Key, authInfo.Workspace.ExternalId)
	if err != nil {
		return &pb.PodSandboxStatusResponse{
			Ok:       false,
			ErrorMsg: "Failed to connect to sandbox",
		}, nil
	}

	resp, err := client.SandboxStatusContext(ctx, in.ContainerId, in.Pid)
	if err != nil && isTransientSandboxConnectFailure(err) {
		s.evictClient(cacheKey)
		client, _, retryErr := s.getClient(ctx, in.ContainerId, authInfo.Token.Key, authInfo.Workspace.ExternalId)
		if retryErr != nil {
			err = retryErr
		} else {
			resp, err = client.SandboxStatusContext(ctx, in.ContainerId, in.Pid)
		}
	}
	if err != nil {
		return &pb.PodSandboxStatusResponse{
			Ok:       false,
			ErrorMsg: "Failed to get sandbox status",
		}, nil
	}

	if !resp.Ok {
		return &pb.PodSandboxStatusResponse{
			Ok:       false,
			ErrorMsg: resp.ErrorMsg,
		}, nil
	}

	return &pb.PodSandboxStatusResponse{
		Ok:       true,
		Status:   resp.Status,
		ExitCode: resp.ExitCode,
	}, nil
}

func (s *GenericPodService) getSandboxContainerStateForStatus(ctx context.Context, containerId string) (*types.ContainerState, error) {
	if s.rdb == nil {
		return s.containerRepo.GetContainerState(containerId)
	}

	readCtx, cancel := context.WithTimeout(ctx, sandboxStatusMetadataTimeout)
	defer cancel()

	stateKey := common.RedisKeys.SchedulerContainerState(containerId)
	res, err := s.rdb.HGetAll(readCtx, stateKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get container state: %w", err)
	}
	if len(res) == 0 {
		return nil, &types.ErrContainerStateNotFound{ContainerId: containerId}
	}

	state := &types.ContainerState{}
	if err := common.ToStruct(res, state); err != nil {
		return nil, fmt.Errorf("failed to deserialize container state <%s>: %w", stateKey, err)
	}

	return state, nil
}

func (s *GenericPodService) SandboxStdout(ctx context.Context, in *pb.PodSandboxStdoutRequest) (*pb.PodSandboxStdoutResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	client, _, err := s.getClient(ctx, in.ContainerId, authInfo.Token.Key, authInfo.Workspace.ExternalId)
	if err != nil {
		return &pb.PodSandboxStdoutResponse{
			Ok:       false,
			ErrorMsg: "Failed to connect to sandbox",
		}, nil
	}

	resp, err := client.SandboxStdout(in.ContainerId, in.Pid)
	if err != nil {
		return &pb.PodSandboxStdoutResponse{
			Ok:       false,
			ErrorMsg: "Failed to get sandbox stdout",
		}, nil
	}

	return &pb.PodSandboxStdoutResponse{
		Ok:     true,
		Stdout: resp.Stdout,
	}, nil
}

func (s *GenericPodService) SandboxStderr(ctx context.Context, in *pb.PodSandboxStderrRequest) (*pb.PodSandboxStderrResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	client, _, err := s.getClient(ctx, in.ContainerId, authInfo.Token.Key, authInfo.Workspace.ExternalId)
	if err != nil {
		return &pb.PodSandboxStderrResponse{
			Ok:       false,
			ErrorMsg: "Failed to connect to sandbox",
		}, nil
	}

	resp, err := client.SandboxStderr(in.ContainerId, in.Pid)
	if err != nil {
		return &pb.PodSandboxStderrResponse{
			Ok:       false,
			ErrorMsg: "Failed to get sandbox stderr",
		}, nil
	}

	return &pb.PodSandboxStderrResponse{
		Ok:     true,
		Stderr: resp.Stderr,
	}, nil
}

func (s *GenericPodService) SandboxKill(ctx context.Context, in *pb.PodSandboxKillRequest) (*pb.PodSandboxKillResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	client, _, err := s.getClient(ctx, in.ContainerId, authInfo.Token.Key, authInfo.Workspace.ExternalId)
	if err != nil {
		return &pb.PodSandboxKillResponse{
			Ok:       false,
			ErrorMsg: "Failed to connect to sandbox",
		}, nil
	}

	resp, err := client.SandboxKill(in.ContainerId, in.Pid)
	if err != nil {
		return &pb.PodSandboxKillResponse{
			Ok:       false,
			ErrorMsg: resp.ErrorMsg,
		}, nil
	}

	if !resp.Ok {
		return &pb.PodSandboxKillResponse{
			Ok:       false,
			ErrorMsg: resp.ErrorMsg,
		}, nil
	}

	return &pb.PodSandboxKillResponse{
		Ok: resp.Ok,
	}, nil
}

func (s *GenericPodService) SandboxUploadFile(ctx context.Context, in *pb.PodSandboxUploadFileRequest) (*pb.PodSandboxUploadFileResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	client, _, err := s.getClient(ctx, in.ContainerId, authInfo.Token.Key, authInfo.Workspace.ExternalId)
	if err != nil {
		return &pb.PodSandboxUploadFileResponse{
			Ok:       false,
			ErrorMsg: "Failed to connect to sandbox",
		}, nil
	}

	resp, err := client.SandboxUploadFile(in.ContainerId, in.ContainerPath, in.Data, in.Mode)
	if err != nil {
		return &pb.PodSandboxUploadFileResponse{
			Ok:       false,
			ErrorMsg: fmt.Sprintf("Failed to upload file '%s': %s", in.ContainerPath, err.Error()),
		}, nil
	}

	if !resp.Ok {
		return &pb.PodSandboxUploadFileResponse{
			Ok:       false,
			ErrorMsg: resp.ErrorMsg,
		}, nil
	}

	return &pb.PodSandboxUploadFileResponse{
		Ok: resp.Ok,
	}, nil
}

func (s *GenericPodService) SandboxDownloadFile(ctx context.Context, in *pb.PodSandboxDownloadFileRequest) (*pb.PodSandboxDownloadFileResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	client, _, err := s.getClient(ctx, in.ContainerId, authInfo.Token.Key, authInfo.Workspace.ExternalId)
	if err != nil {
		return &pb.PodSandboxDownloadFileResponse{
			Ok:       false,
			ErrorMsg: "Failed to connect to sandbox",
		}, nil
	}

	resp, err := client.SandboxDownloadFile(in.ContainerId, in.ContainerPath)
	if err != nil {
		return &pb.PodSandboxDownloadFileResponse{
			Ok:       false,
			ErrorMsg: fmt.Sprintf("Failed to download file '%s': %s", in.ContainerPath, err.Error()),
		}, nil
	}

	if !resp.Ok {
		return &pb.PodSandboxDownloadFileResponse{
			Ok:       false,
			ErrorMsg: resp.ErrorMsg,
		}, nil
	}

	return &pb.PodSandboxDownloadFileResponse{
		Ok:   resp.Ok,
		Data: resp.Data,
	}, nil
}

func (s *GenericPodService) SandboxDeleteFile(ctx context.Context, in *pb.PodSandboxDeleteFileRequest) (*pb.PodSandboxDeleteFileResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	client, _, err := s.getClient(ctx, in.ContainerId, authInfo.Token.Key, authInfo.Workspace.ExternalId)
	if err != nil {
		return &pb.PodSandboxDeleteFileResponse{
			Ok:       false,
			ErrorMsg: "Failed to connect to sandbox",
		}, nil
	}

	resp, err := client.SandboxDeleteFile(in.ContainerId, in.ContainerPath)
	if err != nil {
		return &pb.PodSandboxDeleteFileResponse{
			Ok:       false,
			ErrorMsg: fmt.Sprintf("Failed to delete file '%s': %s", in.ContainerPath, err.Error()),
		}, nil
	}

	if !resp.Ok {
		return &pb.PodSandboxDeleteFileResponse{
			Ok:       false,
			ErrorMsg: resp.ErrorMsg,
		}, nil
	}

	return &pb.PodSandboxDeleteFileResponse{
		Ok: resp.Ok,
	}, nil
}

func (s *GenericPodService) SandboxCreateDirectory(ctx context.Context, in *pb.PodSandboxCreateDirectoryRequest) (*pb.PodSandboxCreateDirectoryResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	client, _, err := s.getClient(ctx, in.ContainerId, authInfo.Token.Key, authInfo.Workspace.ExternalId)
	if err != nil {
		return &pb.PodSandboxCreateDirectoryResponse{
			Ok:       false,
			ErrorMsg: "Failed to connect to sandbox",
		}, nil
	}

	resp, err := client.SandboxCreateDirectory(in.ContainerId, in.ContainerPath, in.Mode)
	if err != nil {
		return &pb.PodSandboxCreateDirectoryResponse{
			Ok:       false,
			ErrorMsg: fmt.Sprintf("Failed to create directory '%s': %s", in.ContainerPath, err.Error()),
		}, nil
	}

	if !resp.Ok {
		return &pb.PodSandboxCreateDirectoryResponse{
			Ok:       false,
			ErrorMsg: resp.ErrorMsg,
		}, nil
	}

	return &pb.PodSandboxCreateDirectoryResponse{
		Ok: resp.Ok,
	}, nil
}

func (s *GenericPodService) SandboxDeleteDirectory(ctx context.Context, in *pb.PodSandboxDeleteDirectoryRequest) (*pb.PodSandboxDeleteDirectoryResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	client, _, err := s.getClient(ctx, in.ContainerId, authInfo.Token.Key, authInfo.Workspace.ExternalId)
	if err != nil {
		return &pb.PodSandboxDeleteDirectoryResponse{
			Ok:       false,
			ErrorMsg: "Failed to connect to sandbox",
		}, nil
	}

	resp, err := client.SandboxDeleteDirectory(in.ContainerId, in.ContainerPath)
	if err != nil {
		return &pb.PodSandboxDeleteDirectoryResponse{
			Ok:       false,
			ErrorMsg: fmt.Sprintf("Failed to delete directory '%s': %s", in.ContainerPath, err.Error()),
		}, nil
	}

	if !resp.Ok {
		return &pb.PodSandboxDeleteDirectoryResponse{
			Ok:       false,
			ErrorMsg: resp.ErrorMsg,
		}, nil
	}

	return &pb.PodSandboxDeleteDirectoryResponse{
		Ok: resp.Ok,
	}, nil
}

func (s *GenericPodService) SandboxExposePort(ctx context.Context, in *pb.PodSandboxExposePortRequest) (*pb.PodSandboxExposePortResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	client, _, err := s.getClient(ctx, in.ContainerId, authInfo.Token.Key, authInfo.Workspace.ExternalId)
	if err != nil {
		return &pb.PodSandboxExposePortResponse{
			Ok:       false,
			ErrorMsg: "Failed to connect to sandbox",
		}, nil
	}

	resp, err := client.SandboxExposePort(in.ContainerId, in.Port)
	if err != nil {
		return &pb.PodSandboxExposePortResponse{
			Ok:       false,
			ErrorMsg: "Failed to expose port",
		}, nil
	}

	if !resp.Ok {
		return &pb.PodSandboxExposePortResponse{
			Ok:       false,
			ErrorMsg: resp.ErrorMsg,
		}, nil
	}

	instance, err := s.getOrCreatePodInstance(in.StubId)
	if err != nil {
		return nil, err
	}

	return &pb.PodSandboxExposePortResponse{
		Ok:  resp.Ok,
		Url: common.BuildSandboxURL(s.config.GatewayService.HTTP.GetExternalURL(), s.config.GatewayService.InvokeURLType, instance.Stub, in.Port),
	}, nil
}

func (s *GenericPodService) SandboxUpdateNetworkPermissions(ctx context.Context, in *pb.PodSandboxUpdateNetworkPermissionsRequest) (*pb.PodSandboxUpdateNetworkPermissionsResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	client, _, err := s.getClient(ctx, in.ContainerId, authInfo.Token.Key, authInfo.Workspace.ExternalId)
	if err != nil {
		return &pb.PodSandboxUpdateNetworkPermissionsResponse{
			Ok:       false,
			ErrorMsg: "Failed to connect to sandbox",
		}, nil
	}

	resp, err := client.SandboxUpdateNetworkPermissions(in.ContainerId, in.BlockNetwork, in.AllowList)
	if err != nil {
		return &pb.PodSandboxUpdateNetworkPermissionsResponse{
			Ok:       false,
			ErrorMsg: "Failed to update network permissions",
		}, nil
	}

	return &pb.PodSandboxUpdateNetworkPermissionsResponse{
		Ok:       resp.Ok,
		ErrorMsg: resp.ErrorMsg,
	}, nil
}

func (s *GenericPodService) SandboxStatFile(ctx context.Context, in *pb.PodSandboxStatFileRequest) (*pb.PodSandboxStatFileResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	client, _, err := s.getClient(ctx, in.ContainerId, authInfo.Token.Key, authInfo.Workspace.ExternalId)
	if err != nil {
		return &pb.PodSandboxStatFileResponse{
			Ok:       false,
			ErrorMsg: "Failed to connect to sandbox",
		}, nil
	}

	resp, err := client.SandboxStatFile(in.ContainerId, in.ContainerPath)
	if err != nil {
		return &pb.PodSandboxStatFileResponse{
			Ok:       false,
			ErrorMsg: fmt.Sprintf("Failed to get file info for '%s': %s", in.ContainerPath, err.Error()),
		}, nil
	}

	if !resp.Ok {
		return &pb.PodSandboxStatFileResponse{
			Ok:       false,
			ErrorMsg: resp.ErrorMsg,
		}, nil
	}

	fileInfo := &pb.PodSandboxFileInfo{
		Mode:        resp.FileInfo.Mode,
		Size:        resp.FileInfo.Size,
		ModTime:     resp.FileInfo.ModTime,
		Permissions: resp.FileInfo.Permissions,
		Owner:       resp.FileInfo.Owner,
		Group:       resp.FileInfo.Group,
		IsDir:       resp.FileInfo.IsDir,
		Name:        resp.FileInfo.Name,
	}

	return &pb.PodSandboxStatFileResponse{
		Ok:       true,
		FileInfo: fileInfo,
	}, nil
}

func (s *GenericPodService) SandboxListFiles(ctx context.Context, in *pb.PodSandboxListFilesRequest) (*pb.PodSandboxListFilesResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	client, _, err := s.getClient(ctx, in.ContainerId, authInfo.Token.Key, authInfo.Workspace.ExternalId)
	if err != nil {
		return &pb.PodSandboxListFilesResponse{
			Ok:       false,
			ErrorMsg: "Failed to connect to sandbox",
		}, nil
	}

	resp, err := client.SandboxListFiles(in.ContainerId, in.ContainerPath)
	if err != nil {
		return &pb.PodSandboxListFilesResponse{
			Ok:       false,
			ErrorMsg: fmt.Sprintf("Failed to list files in '%s': %s", in.ContainerPath, err.Error()),
		}, nil
	}

	files := make([]*pb.PodSandboxFileInfo, 0)
	for _, file := range resp.Files {
		files = append(files, &pb.PodSandboxFileInfo{
			Mode:        file.Mode,
			Size:        file.Size,
			ModTime:     file.ModTime,
			Permissions: file.Permissions,
			Owner:       file.Owner,
			Group:       file.Group,
			IsDir:       file.IsDir,
			Name:        file.Name,
		})
	}

	return &pb.PodSandboxListFilesResponse{
		Ok:    true,
		Files: files,
	}, nil
}

func (s *GenericPodService) SandboxReplaceInFiles(ctx context.Context, in *pb.PodSandboxReplaceInFilesRequest) (*pb.PodSandboxReplaceInFilesResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	client, _, err := s.getClient(ctx, in.ContainerId, authInfo.Token.Key, authInfo.Workspace.ExternalId)
	if err != nil {
		return &pb.PodSandboxReplaceInFilesResponse{

			Ok:       false,
			ErrorMsg: "Failed to connect to sandbox",
		}, nil
	}

	resp, err := client.SandboxReplaceInFiles(in.ContainerId, in.ContainerPath, in.Pattern, in.NewString)
	if err != nil {
		return &pb.PodSandboxReplaceInFilesResponse{
			Ok:       false,
			ErrorMsg: fmt.Sprintf("Failed to replace in files at '%s': %s", in.ContainerPath, err.Error()),
		}, nil
	}

	if !resp.Ok {
		return &pb.PodSandboxReplaceInFilesResponse{
			Ok:       false,
			ErrorMsg: resp.ErrorMsg,
		}, nil
	}

	return &pb.PodSandboxReplaceInFilesResponse{
		Ok: resp.Ok,
	}, nil
}

func (s *GenericPodService) SandboxFindInFiles(ctx context.Context, in *pb.PodSandboxFindInFilesRequest) (*pb.PodSandboxFindInFilesResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	client, _, err := s.getClient(ctx, in.ContainerId, authInfo.Token.Key, authInfo.Workspace.ExternalId)
	if err != nil {
		return &pb.PodSandboxFindInFilesResponse{
			Ok:       false,
			ErrorMsg: "Failed to connect to sandbox",
		}, nil
	}

	resp, err := client.SandboxFindInFiles(in.ContainerId, in.ContainerPath, in.Pattern)
	if err != nil {
		return &pb.PodSandboxFindInFilesResponse{
			Ok:       false,
			ErrorMsg: fmt.Sprintf("Failed to find '%s' in '%s': %s", in.Pattern, in.ContainerPath, err.Error()),
		}, nil
	}

	if !resp.Ok {
		return &pb.PodSandboxFindInFilesResponse{
			Ok:       false,
			ErrorMsg: resp.ErrorMsg,
		}, nil
	}

	return &pb.PodSandboxFindInFilesResponse{
		Ok:      true,
		Results: resp.Results,
	}, nil
}

func (s *GenericPodService) SandboxConnect(ctx context.Context, in *pb.PodSandboxConnectRequest) (*pb.PodSandboxConnectResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	_, container, err := s.getClient(ctx, in.ContainerId, authInfo.Token.Key, authInfo.Workspace.ExternalId)
	if err != nil {
		return &pb.PodSandboxConnectResponse{
			Ok:       false,
			ErrorMsg: "Failed to connect to sandbox",
		}, nil
	}

	return &pb.PodSandboxConnectResponse{
		Ok:     true,
		StubId: container.StubId,
	}, nil
}

func (s *GenericPodService) getClient(ctx context.Context, containerId, token string, workspaceId string) (*common.ContainerClient, *types.ContainerState, error) {
	phaseStart := time.Now()
	container, err := s.containerRepo.GetContainerState(containerId)
	if err != nil {
		metrics.RecordSandboxConnectPhase("container_state_lookup", workspaceId, "", "", "state_lookup_failed", false, time.Since(phaseStart))
		return nil, nil, err
	}

	if container == nil {
		metrics.RecordSandboxConnectPhase("container_state_lookup", workspaceId, "", "", "container_not_found", false, time.Since(phaseStart))
		return nil, nil, errors.New("container not found")
	}

	if container.WorkspaceId != workspaceId {
		metrics.RecordSandboxConnectPhase("container_state_lookup", workspaceId, container.StubId, string(container.Status), "invalid_workspace", false, time.Since(phaseStart))
		return nil, nil, errors.New("invalid workspace")
	}
	metrics.RecordSandboxConnectPhase("container_state_lookup", workspaceId, container.StubId, string(container.Status), "", true, time.Since(phaseStart))

	cacheKey := sandboxClientCacheKey(containerId, token)
	if cached, ok := s.clientCache.Load(cacheKey); ok {
		if client, ok := cached.(*common.ContainerClient); ok {
			metrics.RecordSandboxConnectPhase("client_cache", workspaceId, container.StubId, string(container.Status), "", true, 0)
			return client, container, nil
		}
	}

	phaseStart = time.Now()
	addressCtx, cancel := context.WithTimeout(ctx, sandboxConnectWorkerAddressTimeout)
	hostname, err := s.containerRepo.GetWorkerAddress(addressCtx, containerId)
	cancel()
	if err != nil {
		metrics.RecordSandboxConnectPhase("worker_address_lookup", workspaceId, container.StubId, string(container.Status), "worker_address_unavailable", false, time.Since(phaseStart))
		return nil, nil, err
	}
	metrics.RecordSandboxConnectPhase("worker_address_lookup", workspaceId, container.StubId, string(container.Status), "", true, time.Since(phaseStart))

	phaseStart = time.Now()
	dialCtx, cancel := context.WithTimeout(ctx, sandboxConnectWorkerDialTimeout)
	conn, err := network.ConnectToBackend(dialCtx, hostname, sandboxConnectWorkerDialTimeout, s.tailscale, s.config.Tailscale, s.containerRepo)
	cancel()
	if err != nil {
		metrics.RecordSandboxConnectPhase("worker_dial", workspaceId, container.StubId, string(container.Status), "worker_dial_failed", false, time.Since(phaseStart))
		return nil, nil, err
	}
	metrics.RecordSandboxConnectPhase("worker_dial", workspaceId, container.StubId, string(container.Status), "", true, time.Since(phaseStart))

	phaseStart = time.Now()
	client, err := common.NewContainerClient(hostname, token, conn)
	if err != nil {
		metrics.RecordSandboxConnectPhase("client_create", workspaceId, container.StubId, string(container.Status), "client_create_failed", false, time.Since(phaseStart))
		return nil, nil, err
	}
	metrics.RecordSandboxConnectPhase("client_create", workspaceId, container.StubId, string(container.Status), "", true, time.Since(phaseStart))

	s.clientCache.Store(cacheKey, client)
	return client, container, nil
}

func sandboxClientCacheKey(containerId, token string) string {
	return containerId + ":" + token
}

func (s *GenericPodService) evictClient(cacheKey string) {
	cached, ok := s.clientCache.LoadAndDelete(cacheKey)
	if !ok {
		return
	}

	if client, ok := cached.(*common.ContainerClient); ok {
		_ = client.Close()
	}
}

func sandboxConnectErrorMessage(_ error) string {
	return "Failed to connect to sandbox"
}

func logSandboxConnectFailure(err error, containerId string) {
	event := log.Warn()
	if isTransientSandboxConnectFailure(err) {
		event = log.Trace()
	}

	event.Err(err).Str("container_id", containerId).Msg("failed to connect to sandbox for exec")
}

func isTransientSandboxConnectFailure(err error) bool {
	if err == nil {
		return false
	}

	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
		return true
	}

	var stateNotFound *types.ErrContainerStateNotFound
	if errors.As(err, &stateNotFound) {
		return true
	}

	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "failed to schedule container") ||
		strings.Contains(msg, "context cancelled while trying to get worker addr") ||
		strings.Contains(msg, "context deadline exceeded") ||
		strings.Contains(msg, "deadline exceeded") ||
		strings.Contains(msg, "transport") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "eof") ||
		strings.Contains(msg, "unavailable")
}

func (s *GenericPodService) SandboxUpdateTTL(ctx context.Context, in *pb.PodSandboxUpdateTTLRequest) (*pb.PodSandboxUpdateTTLResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	_, container, err := s.getClient(ctx, in.ContainerId, authInfo.Token.Key, authInfo.Workspace.ExternalId)
	if err != nil {
		return &pb.PodSandboxUpdateTTLResponse{
			Ok:       false,
			ErrorMsg: "Failed to connect to sandbox",
		}, nil
	}

	instance, err := s.getOrCreatePodInstance(container.StubId)
	if err != nil {
		return nil, err
	}

	key := Keys.podKeepWarmLock(authInfo.Workspace.Name, instance.Stub.ExternalId, in.ContainerId)
	if in.Ttl <= 0 {
		s.rdb.Set(context.Background(), key, 1, 0) // Never expire
	} else {
		s.rdb.SetEx(context.Background(), key, 1, time.Duration(in.Ttl)*time.Second)
	}

	return &pb.PodSandboxUpdateTTLResponse{
		Ok: true,
	}, nil
}

func (s *GenericPodService) SandboxCreateImageFromFilesystem(ctx context.Context, in *pb.PodSandboxCreateImageFromFilesystemRequest) (*pb.PodSandboxCreateImageFromFilesystemResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	client, _, err := s.getClient(ctx, in.ContainerId, authInfo.Token.Key, authInfo.Workspace.ExternalId)
	if err != nil {
		return &pb.PodSandboxCreateImageFromFilesystemResponse{
			Ok:       false,
			ErrorMsg: "Failed to connect to sandbox",
		}, nil
	}

	imageId := fmt.Sprintf("%s-%d", in.StubId, time.Now().Unix())
	progressChan := make(chan common.OutputMsg, 1000)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-progressChan:
				log.Info().Str("stub_id", in.StubId).Str("container_id", in.ContainerId).Str("image_id", imageId).Msg(msg.Msg)
				if msg.Done {
					return
				}
			}
		}
	}()

	err = client.Archive(ctx, in.ContainerId, imageId, progressChan)
	if err != nil {
		return &pb.PodSandboxCreateImageFromFilesystemResponse{
			Ok:       false,
			ErrorMsg: err.Error(),
		}, nil
	}

	return &pb.PodSandboxCreateImageFromFilesystemResponse{
		Ok:      true,
		ImageId: imageId,
	}, nil
}

func (s *GenericPodService) SandboxSnapshotMemory(ctx context.Context, in *pb.PodSandboxSnapshotMemoryRequest) (*pb.PodSandboxSnapshotMemoryResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	client, _, err := s.getClient(ctx, in.ContainerId, authInfo.Token.Key, authInfo.Workspace.ExternalId)
	if err != nil {
		return &pb.PodSandboxSnapshotMemoryResponse{
			Ok:       false,
			ErrorMsg: "Failed to connect to sandbox",
		}, nil
	}

	resp, err := client.Checkpoint(ctx, in.ContainerId)
	if err != nil {
		return &pb.PodSandboxSnapshotMemoryResponse{
			Ok:       false,
			ErrorMsg: err.Error(),
		}, nil
	}
	if !resp.Ok {
		return &pb.PodSandboxSnapshotMemoryResponse{
			Ok:       false,
			ErrorMsg: resp.ErrorMsg,
		}, nil
	}
	if resp.CheckpointId == "" {
		return &pb.PodSandboxSnapshotMemoryResponse{
			Ok:       false,
			ErrorMsg: "checkpoint response missing checkpoint ID",
		}, nil
	}

	return &pb.PodSandboxSnapshotMemoryResponse{
		Ok:           true,
		CheckpointId: resp.CheckpointId,
	}, nil
}

func (s *GenericPodService) SandboxListUrls(ctx context.Context, in *pb.PodSandboxListUrlsRequest) (*pb.PodSandboxListUrlsResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	client, _, err := s.getClient(ctx, in.ContainerId, authInfo.Token.Key, authInfo.Workspace.ExternalId)
	if err != nil {
		return &pb.PodSandboxListUrlsResponse{
			Ok:       false,
			ErrorMsg: "Failed to connect to sandbox",
		}, nil
	}

	stubId, err := common.ExtractStubIdFromContainerId(in.ContainerId)
	if err != nil {
		return &pb.PodSandboxListUrlsResponse{
			Ok:       false,
			ErrorMsg: "Invalid container id",
		}, nil
	}

	instance, err := s.getOrCreatePodInstance(stubId)
	if err != nil {
		return &pb.PodSandboxListUrlsResponse{
			Ok:       false,
			ErrorMsg: "Failed to get pod instance",
		}, nil
	}

	resp, err := client.SandboxListExposedPorts(in.ContainerId)
	if err != nil {
		return &pb.PodSandboxListUrlsResponse{
			Ok:       false,
			ErrorMsg: "Failed to list urls",
		}, nil
	}

	urls := make(map[int32]string)
	for _, port := range resp.ExposedPorts {
		urls[port] = common.BuildSandboxURL(s.config.GatewayService.HTTP.GetExternalURL(), s.config.GatewayService.InvokeURLType, instance.Stub, port)
	}

	return &pb.PodSandboxListUrlsResponse{
		Ok:   true,
		Urls: urls,
	}, nil
}

func (s *GenericPodService) SandboxListProcesses(ctx context.Context, in *pb.PodSandboxListProcessesRequest) (*pb.PodSandboxListProcessesResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	client, _, err := s.getClient(ctx, in.ContainerId, authInfo.Token.Key, authInfo.Workspace.ExternalId)
	if err != nil {
		return &pb.PodSandboxListProcessesResponse{
			Ok:       false,
			ErrorMsg: "Failed to connect to sandbox",
		}, nil
	}

	resp, err := client.SandboxListProcesses(in.ContainerId)
	if err != nil {
		return &pb.PodSandboxListProcessesResponse{
			Ok:       false,
			ErrorMsg: "Failed to list processes",
		}, nil
	}

	return &pb.PodSandboxListProcessesResponse{
		Ok:        true,
		Processes: resp.Processes,
	}, nil
}
