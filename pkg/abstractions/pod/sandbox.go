package pod

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/rs/zerolog/log"
)

func (s *GenericPodService) SandboxExec(ctx context.Context, in *pb.PodSandboxExecRequest) (*pb.PodSandboxExecResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	client, _, err := s.getClient(ctx, in.ContainerId, authInfo.Token.Key, authInfo.Workspace.ExternalId)
	if err != nil {
		return &pb.PodSandboxExecResponse{
			Ok:       false,
			ErrorMsg: "Failed to connect to sandbox",
		}, nil
	}

	resp, err := client.SandboxExec(in.ContainerId, in.Command, in.Env, in.Cwd)
	if err != nil {
		return &pb.PodSandboxExecResponse{
			Ok:       false,
			ErrorMsg: "Failed to execute command",
		}, nil
	}

	return &pb.PodSandboxExecResponse{
		Ok:  true,
		Pid: resp.Pid,
	}, nil
}

func (s *GenericPodService) SandboxStatus(ctx context.Context, in *pb.PodSandboxStatusRequest) (*pb.PodSandboxStatusResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	client, _, err := s.getClient(ctx, in.ContainerId, authInfo.Token.Key, authInfo.Workspace.ExternalId)
	if err != nil {
		return &pb.PodSandboxStatusResponse{
			Ok:       false,
			ErrorMsg: "Failed to connect to sandbox",
		}, nil
	}

	resp, err := client.SandboxStatus(in.ContainerId, in.Pid)
	if err != nil {
		return &pb.PodSandboxStatusResponse{
			Ok:       false,
			ErrorMsg: "Failed to get sandbox status",
		}, nil
	}

	return &pb.PodSandboxStatusResponse{
		Ok:       true,
		Status:   resp.Status,
		ExitCode: resp.ExitCode,
	}, nil
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
			ErrorMsg: "Failed to get sandbox stdout",
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
			ErrorMsg: "Failed to kill sandbox process",
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
			ErrorMsg: "Failed to upload file to sandbox",
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
			ErrorMsg: "Failed to download file from sandbox",
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
			ErrorMsg: "Failed to delete file in sandbox",
		}, nil
	}

	return &pb.PodSandboxDeleteFileResponse{
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
			ErrorMsg: "Failed to get file info",
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
			ErrorMsg: "Failed to list files",
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
			ErrorMsg: "Failed to replace in file",
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
			ErrorMsg: "Failed to find files",
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

func (s *GenericPodService) getClient(ctx context.Context, containerId, token string, workspaceId string) (*common.RunCClient, *types.ContainerState, error) {
	container, err := s.containerRepo.GetContainerState(containerId)
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
	if cached, ok := s.clientCache.Load(cacheKey); ok {
		if client, ok := cached.(*common.RunCClient); ok {
			return client, container, nil
		}
	}

	hostname, err := s.containerRepo.GetWorkerAddress(ctx, containerId)
	if err != nil {
		return nil, nil, err
	}

	conn, err := network.ConnectToHost(ctx, hostname, time.Second*30, s.tailscale, s.config.Tailscale)
	if err != nil {
		return nil, nil, err
	}

	client, err := common.NewRunCClient(hostname, token, conn)
	if err != nil {
		return nil, nil, err
	}

	s.clientCache.Store(cacheKey, client)
	return client, container, nil
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

func (s *GenericPodService) SandboxSnapshot(ctx context.Context, in *pb.PodSandboxSnapshotRequest) (*pb.PodSandboxSnapshotResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	client, _, err := s.getClient(ctx, in.ContainerId, authInfo.Token.Key, authInfo.Workspace.ExternalId)
	if err != nil {
		return &pb.PodSandboxSnapshotResponse{
			Ok:       false,
			ErrorMsg: "Failed to connect to sandbox",
		}, nil
	}

	snapshotId := fmt.Sprintf("snapshot-%s-%d", in.StubId, time.Now().Unix())
	progressChan := make(chan common.OutputMsg, 1000)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				msg := <-progressChan
				log.Info().Str("stub_id", in.StubId).Str("container_id", in.ContainerId).Str("snapshot_id", snapshotId).Msg(msg.Msg)
				if msg.Done {
					break
				}
			}
		}
	}()

	err = client.Archive(ctx, in.ContainerId, snapshotId, progressChan)
	if err != nil {
		return &pb.PodSandboxSnapshotResponse{
			Ok:       false,
			ErrorMsg: err.Error(),
		}, nil
	}

	return &pb.PodSandboxSnapshotResponse{
		Ok:         true,
		SnapshotId: snapshotId,
	}, nil
}
