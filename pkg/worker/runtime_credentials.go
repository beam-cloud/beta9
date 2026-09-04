package worker

import (
	"context"
	"errors"
	"strings"

	"github.com/beam-cloud/beta9/pkg/storage"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// claimContainer is the worker's single pre-start round trip: it acknowledges
// the delivery, refreshes the pending lease, and hydrates the runtime
// credentials the sanitized request omitted. Credentials belong to the request,
// so every worker that accepts one hydrates them, regardless of how it is
// hosted.
//
// claimed reports whether the gateway acknowledged the delivery. A rejected
// claim (claimed=false) means the container is owned elsewhere or was
// cancelled, and the worker must only release its reserved capacity. Any
// failure after the claim is the worker's to report like a startup failure.
func (s *Worker) claimContainer(ctx context.Context, request *types.ContainerRequest) (claimed bool, err error) {
	claim := &pb.ClaimContainerRequest{
		WorkerId:      s.workerId,
		ContainerId:   request.ContainerId,
		PoolName:      s.poolName,
		PodHostname:   s.podHostName,
		DeliveryToken: request.DeliveryToken,
		Credentials:   runtimeCredentialsRequest(request),
	}

	var resp *pb.ClaimContainerResponse
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, containerRequestAckTimeout)
		resp, err = s.workerRepoClient.ClaimContainer(attemptCtx, claim)
		cancel()
		if err == nil {
			break
		}
		// A transport failure is ambiguous: the gateway may already have
		// recorded the claim. Retrying with the same delivery token is safe
		// because the acknowledgement is idempotent for a token it accepted.
		// The gateway also answers transient repository failures after the
		// claim with UNAVAILABLE so the worker retries instead of failing the
		// container.
		if !isRetryableClaimError(err) {
			return false, err
		}
		if !waitForReconnect(ctx, containerRequestStreamInterval) {
			return false, ctx.Err()
		}
	}
	if !resp.Ok {
		return resp.Claimed, errors.New(resp.ErrorMsg)
	}

	if resp.Credentials != nil {
		applyRuntimeCredentials(request, resp.Credentials)
	}
	if claim.Credentials != nil && claim.Credentials.WorkspaceStorage && request.IsBuildRequest() && !workspaceStorageDownloadAvailable(request.Workspace.Storage) {
		return true, errors.New("workspace storage credentials are required to download build context directly")
	}
	if resp.State != nil && types.ContainerStatus(resp.State.Status) == types.ContainerStatusStopping {
		// A stop raced the claim. The observed-stop path cancels startup.
		s.handleObservedStoppingContainer(request.ContainerId, types.EventSourceWorkerStatusHeartbeat)
	}
	return true, nil
}

func isRetryableClaimError(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted, codes.ResourceExhausted:
		return true
	}
	return errors.Is(err, context.DeadlineExceeded)
}

// runtimeCredentialsRequest describes the credentials a request needs vended
// before it can start, or nil when the sanitized request is already complete.
func runtimeCredentialsRequest(request *types.ContainerRequest) *pb.GetContainerRuntimeCredentialsRequest {
	credentials := &pb.GetContainerRuntimeCredentialsRequest{
		WorkspaceId: request.WorkspaceId,
		StubId:      request.StubId,
		ContainerId: request.ContainerId,
	}
	workspaceStorage := request.Workspace.StorageAvailable() && !workspaceStorageDownloadAvailable(request.Workspace.Storage)
	if request.IsBuildRequest() {
		// A build only needs workspace storage, and only to download its
		// build context directly.
		if !workspaceStorage || request.BuildOptions.Dockerfile == nil || request.BuildOptions.BuildCtxObject == nil {
			return nil
		}
		credentials.WorkspaceStorage = true
		return credentials
	}

	credentials.SecretNames = request.RuntimeSecretNames
	credentials.RuntimeToken = request.RuntimeTokenRequired
	credentials.WorkspaceStorage = workspaceStorage
	credentials.MountCredentials = runtimeMountCredentialRequests(request)
	if !credentials.RuntimeToken && !credentials.WorkspaceStorage && len(credentials.SecretNames) == 0 && len(credentials.MountCredentials) == 0 {
		return nil
	}
	return credentials
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
