package worker

import (
	"context"
	"errors"
	"strings"

	"github.com/beam-cloud/beta9/pkg/storage"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

func (s *Worker) hydrateRuntimeCredentials(ctx context.Context, request *types.ContainerRequest) error {
	if !s.agentWorker() || request.IsBuildRequest() {
		return nil
	}

	credentialRequest := runtimeCredentialsRequest(request)
	if !hasRuntimeCredentialRequest(credentialRequest) {
		return nil
	}

	resp, err := handleGRPCResponse(s.workerRepoClient.GetContainerRuntimeCredentials(ctx, credentialRequest))
	if err != nil {
		return err
	}
	if !resp.Ok {
		return errors.New(resp.ErrorMsg)
	}

	applyRuntimeCredentials(request, resp)
	return nil
}

func runtimeCredentialsRequest(request *types.ContainerRequest) *pb.GetContainerRuntimeCredentialsRequest {
	return &pb.GetContainerRuntimeCredentialsRequest{
		WorkspaceId:      request.WorkspaceId,
		StubId:           request.StubId,
		ContainerId:      request.ContainerId,
		SecretNames:      request.RuntimeSecretNames,
		RuntimeToken:     request.RuntimeTokenRequired,
		WorkspaceStorage: request.Workspace.StorageAvailable() && !workspaceStorageDownloadAvailable(request.Workspace.Storage),
		MountCredentials: runtimeMountCredentialRequests(request),
	}
}

func hasRuntimeCredentialRequest(request *pb.GetContainerRuntimeCredentialsRequest) bool {
	return request.RuntimeToken ||
		request.WorkspaceStorage ||
		len(request.SecretNames) > 0 ||
		len(request.MountCredentials) > 0
}

func runtimeMountCredentialRequests(request *types.ContainerRequest) []*pb.RuntimeMountCredentialRequest {
	credentials := make([]*pb.RuntimeMountCredentialRequest, 0, len(request.Mounts))
	seen := map[string]struct{}{}
	for _, mount := range request.Mounts {
		if mount.MountType != storage.StorageModeMountPoint || mount.MountPointConfig == nil {
			continue
		}
		if mount.MountPointConfig.AccessKey != "" && mount.MountPointConfig.SecretKey != "" {
			continue
		}
		key := types.MountPointCredentialKey(mount.MountPath, mount.MountPointConfig.BucketName)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		credentials = append(credentials, &pb.RuntimeMountCredentialRequest{
			MountPath:  mount.MountPath,
			BucketName: mount.MountPointConfig.BucketName,
		})
	}
	return credentials
}

func mergeRuntimeEnv(existing, vended []string) []string {
	if len(vended) == 0 {
		return existing
	}

	vendedKeys := make(map[string]struct{}, len(vended))
	for _, item := range vended {
		if key := envKey(item); key != "" {
			vendedKeys[key] = struct{}{}
		}
	}

	env := make([]string, 0, len(existing)+len(vended))
	for _, item := range existing {
		if _, ok := vendedKeys[envKey(item)]; ok {
			continue
		}
		env = append(env, item)
	}
	return append(env, vended...)
}

func applyRuntimeCredentials(request *types.ContainerRequest, resp *pb.GetContainerRuntimeCredentialsResponse) {
	request.Env = mergeRuntimeEnv(request.Env, resp.Env)
	if resp.WorkspaceStorage != nil {
		request.Workspace.Storage = mergeWorkspaceStorageCredentials(request.Workspace.Storage, resp.WorkspaceStorage)
	}
	applyMountCredentials(request, resp.MountCredentials)
}

func envKey(item string) string {
	key, _, ok := strings.Cut(item, "=")
	if !ok {
		return ""
	}
	return key
}

func mergeWorkspaceStorageCredentials(existing *types.WorkspaceStorage, creds *pb.CacheWorkspaceStorageCredentials) *types.WorkspaceStorage {
	storage := &types.WorkspaceStorage{}
	if existing != nil {
		copied := *existing
		storage = &copied
	}
	storage.EndpointUrl = stringPtr(creds.EndpointUrl)
	storage.Region = stringPtr(creds.Region)
	storage.BucketName = stringPtr(creds.BucketName)
	storage.AccessKey = stringPtr(creds.AccessKey)
	storage.SecretKey = stringPtr(creds.SecretKey)
	return storage
}

func applyMountCredentials(request *types.ContainerRequest, credentials []*pb.RuntimeMountCredentials) {
	if len(credentials) == 0 {
		return
	}
	byMountPath := make(map[string]*types.MountPointConfig, len(credentials))
	for _, item := range credentials {
		if item == nil || item.Config == nil {
			continue
		}
		byMountPath[types.MountPointCredentialKey(item.MountPath, item.Config.BucketName)] = types.NewMountPointConfigFromProto(item.Config)
	}
	for i := range request.Mounts {
		if request.Mounts[i].MountPointConfig == nil {
			continue
		}
		config := byMountPath[types.MountPointCredentialKey(request.Mounts[i].MountPath, request.Mounts[i].MountPointConfig.BucketName)]
		if config != nil {
			request.Mounts[i].MountPointConfig = config
		}
	}
}

func stringPtr(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
