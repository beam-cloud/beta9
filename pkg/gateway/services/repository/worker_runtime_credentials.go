package repository_services

import (
	"context"
	"database/sql"
	"fmt"
	"path"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

const (
	runtimeCredentialStateRetries       = 50
	runtimeCredentialStateRetryInterval = 100 * time.Millisecond
)

// GetContainerRuntimeCredentials vends per-container credentials over the worker
// repository service. It intentionally uses the existing backend/container repos
// instead of introducing a persistence repository because this path does not own
// durable state.
func (s *WorkerRepositoryService) GetContainerRuntimeCredentials(ctx context.Context, req *pb.GetContainerRuntimeCredentialsRequest) (*pb.GetContainerRuntimeCredentialsResponse, error) {
	workspace, err := s.authorizeWorkerRuntimeCredentialRequest(ctx, req)
	if err != nil {
		return &pb.GetContainerRuntimeCredentialsResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	resp := &pb.GetContainerRuntimeCredentialsResponse{Ok: true}

	if req.RuntimeToken || len(req.SecretNames) > 0 {
		resp.Env, err = s.workerRuntimeEnv(ctx, workspace, req)
		if err != nil {
			return &pb.GetContainerRuntimeCredentialsResponse{Ok: false, ErrorMsg: err.Error()}, nil
		}
	}

	if req.WorkspaceStorage {
		resp.WorkspaceStorage = s.workspaceStorageCredentials(ctx, req.WorkspaceId)
	}

	if len(req.MountCredentials) > 0 {
		resp.MountCredentials, err = s.workerRuntimeMountCredentials(ctx, workspace, req)
		if err != nil {
			return &pb.GetContainerRuntimeCredentialsResponse{Ok: false, ErrorMsg: err.Error()}, nil
		}
	}

	return resp, nil
}

func (s *WorkerRepositoryService) authorizeWorkerRuntimeCredentialRequest(ctx context.Context, req *pb.GetContainerRuntimeCredentialsRequest) (*types.Workspace, error) {
	if req == nil {
		return nil, fmt.Errorf("request is required")
	}
	workspaceID, err := s.workerTokenWorkspaceID(ctx, req.WorkspaceId)
	if err != nil {
		return nil, err
	}

	if s.backendRepo == nil {
		return nil, fmt.Errorf("backend repository is unavailable")
	}

	if runtimeCredentialsWorkspaceStorageOnly(req) {
		return s.runtimeCredentialsWorkspace(ctx, workspaceID, req)
	}

	if s.containerRepo == nil {
		return nil, fmt.Errorf("container repository is unavailable")
	}

	state, err := s.runtimeCredentialsContainerState(ctx, req.ContainerId)
	if err != nil {
		return nil, err
	}
	if state.WorkspaceId != req.WorkspaceId || state.StubId != req.StubId {
		return nil, fmt.Errorf("container %q is not assigned to workspace/stub", req.ContainerId)
	}

	workspace, err := s.runtimeCredentialsWorkspace(ctx, workspaceID, req)
	if err != nil {
		return nil, err
	}
	return workspace, nil
}

func (s *WorkerRepositoryService) runtimeCredentialsContainerState(ctx context.Context, containerID string) (*types.ContainerState, error) {
	var lastErr error
	for attempt := 0; attempt <= runtimeCredentialStateRetries; attempt++ {
		state, err := s.containerRepo.GetContainerState(containerID)
		if err == nil {
			return state, nil
		}
		if !common.IsRedisLockNotObtained(err) {
			return nil, err
		}
		lastErr = err

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(runtimeCredentialStateRetryInterval):
		}
	}

	return nil, lastErr
}

func (s *WorkerRepositoryService) runtimeCredentialsWorkspace(ctx context.Context, workspaceID uint, req *pb.GetContainerRuntimeCredentialsRequest) (*types.Workspace, error) {
	if runtimeCredentialsNeedsSigningKey(req) {
		workspace, err := s.backendRepo.GetWorkspaceByExternalIdWithSigningKey(ctx, req.WorkspaceId)
		if err != nil {
			return nil, err
		}
		if workspace.Id != workspaceID {
			return nil, fmt.Errorf("worker token cannot request credentials for workspace %q", req.WorkspaceId)
		}
		if workspace.SigningKey == nil || *workspace.SigningKey == "" {
			return nil, fmt.Errorf("workspace signing key is unavailable")
		}
		return &workspace, nil
	}

	return s.backendRepo.GetWorkspace(ctx, workspaceID)
}

func runtimeCredentialsNeedsSigningKey(req *pb.GetContainerRuntimeCredentialsRequest) bool {
	return req != nil && (len(req.SecretNames) > 0 || len(req.MountCredentials) > 0)
}

func runtimeCredentialsWorkspaceStorageOnly(req *pb.GetContainerRuntimeCredentialsRequest) bool {
	return req != nil &&
		req.WorkspaceStorage &&
		!req.RuntimeToken &&
		len(req.SecretNames) == 0 &&
		len(req.MountCredentials) == 0
}

func (s *WorkerRepositoryService) workerTokenWorkspaceID(ctx context.Context, workspaceID string) (uint, error) {
	authInfo, ok := auth.AuthInfoFromContext(ctx)
	if !ok || authInfo == nil || authInfo.Token == nil || !types.IsWorkerTokenType(authInfo.Token.TokenType) {
		return 0, fmt.Errorf("worker token is required")
	}
	if authInfo.Workspace == nil || authInfo.Workspace.ExternalId == "" {
		return 0, fmt.Errorf("worker token is not scoped to a workspace")
	}
	if workspaceID == "" || workspaceID != authInfo.Workspace.ExternalId {
		return 0, fmt.Errorf("worker token cannot request credentials for workspace %q", workspaceID)
	}
	if authInfo.Workspace.Id > 0 {
		return authInfo.Workspace.Id, nil
	}
	if authInfo.Token.WorkspaceId != nil && *authInfo.Token.WorkspaceId > 0 {
		return *authInfo.Token.WorkspaceId, nil
	}
	if s.backendRepo == nil {
		return 0, fmt.Errorf("backend repository is unavailable")
	}

	workspace, err := s.backendRepo.GetWorkspaceByExternalId(ctx, workspaceID)
	if err != nil {
		return 0, err
	}
	return workspace.Id, nil
}

func (s *WorkerRepositoryService) workerRuntimeEnv(ctx context.Context, workspace *types.Workspace, req *pb.GetContainerRuntimeCredentialsRequest) ([]string, error) {
	env := make([]string, 0, len(req.SecretNames)+1)
	if len(req.SecretNames) > 0 {
		secrets, err := s.workerRuntimeSecrets(ctx, workspace, req)
		if err != nil {
			return nil, err
		}
		for _, secret := range secrets {
			env = append(env, fmt.Sprintf("%s=%s", secret.Name, secret.Value))
		}
	}

	if req.RuntimeToken {
		token, err := s.workspaceRuntimeToken(ctx, workspace.Id)
		if err != nil {
			return nil, err
		}
		env = append(env, fmt.Sprintf("BETA9_TOKEN=%s", token))
	}
	return env, nil
}

func (s *WorkerRepositoryService) workerRuntimeSecrets(ctx context.Context, workspace *types.Workspace, req *pb.GetContainerRuntimeCredentialsRequest) ([]types.Secret, error) {
	requested := uniqueRuntimeSecretNames(req.SecretNames)
	byName := make(map[string]types.Secret, len(requested))
	missing := make([]string, 0, len(requested))

	stubConfig, err := s.workerRuntimeStubConfig(ctx, req.StubId, req.WorkspaceId)
	if err != nil {
		return nil, err
	}

	secretKey, err := common.ParseSecretKey(*workspace.SigningKey)
	if err != nil {
		return nil, err
	}
	wanted := make(map[string]struct{}, len(requested))
	for _, name := range requested {
		wanted[name] = struct{}{}
	}
	for _, secret := range stubConfig.Secrets {
		if _, ok := wanted[secret.Name]; !ok {
			continue
		}
		value, err := common.Decrypt(secretKey, secret.Value)
		if err != nil {
			return nil, err
		}
		secret.Value = value
		byName[secret.Name] = secret
	}

	for _, name := range requested {
		if _, ok := byName[name]; !ok {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		secrets, err := s.backendRepo.GetSecretsByNameDecrypted(ctx, workspace, missing)
		if err != nil {
			return nil, err
		}
		for _, secret := range secrets {
			byName[secret.Name] = secret
		}
	}

	secrets := make([]types.Secret, 0, len(requested))
	for _, name := range requested {
		secret, ok := byName[name]
		if !ok {
			return nil, fmt.Errorf("secret %q is unavailable", name)
		}
		secrets = append(secrets, secret)
	}
	return secrets, nil
}

func uniqueRuntimeSecretNames(names []string) []string {
	out := make([]string, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	return out
}

func (s *WorkerRepositoryService) workspaceRuntimeToken(ctx context.Context, workspaceID uint) (string, error) {
	tokens, err := s.backendRepo.ListTokens(ctx, workspaceID)
	if err != nil && err != sql.ErrNoRows {
		return "", err
	}
	for _, token := range tokens {
		if token.Active && !token.DisabledByClusterAdmin && token.TokenType == types.TokenTypeWorkspaceRestricted {
			return token.Key, nil
		}
	}

	token, err := s.backendRepo.CreateToken(ctx, workspaceID, types.TokenTypeWorkspaceRestricted, true)
	if err != nil {
		return "", err
	}
	return token.Key, nil
}

func (s *WorkerRepositoryService) workerRuntimeMountCredentials(ctx context.Context, workspace *types.Workspace, req *pb.GetContainerRuntimeCredentialsRequest) ([]*pb.RuntimeMountCredentials, error) {
	stubConfig, err := s.workerRuntimeStubConfig(ctx, req.StubId, req.WorkspaceId)
	if err != nil {
		return nil, err
	}

	secretKey, err := common.ParseSecretKey(*workspace.SigningKey)
	if err != nil {
		return nil, err
	}

	byKey := make(map[string]*types.MountPointConfig, len(stubConfig.Volumes)*2)
	for _, volume := range stubConfig.Volumes {
		if volume == nil || volume.Config == nil {
			continue
		}
		config, err := decryptMountPointConfig(secretKey, volume.Config)
		if err != nil {
			return nil, err
		}
		for _, mountPath := range mountPathsForVolume(volume.MountPath) {
			byKey[types.MountPointCredentialKey(mountPath, config.BucketName)] = config
		}
	}

	out := make([]*pb.RuntimeMountCredentials, 0, len(req.MountCredentials))
	for _, wanted := range req.MountCredentials {
		if wanted == nil {
			continue
		}
		config := byKey[types.MountPointCredentialKey(wanted.MountPath, wanted.BucketName)]
		if config == nil {
			return nil, fmt.Errorf("mount credentials are unavailable for %s", wanted.MountPath)
		}
		out = append(out, &pb.RuntimeMountCredentials{
			MountPath: wanted.MountPath,
			Config:    config.ToProto(),
		})
	}
	return out, nil
}

func (s *WorkerRepositoryService) workerRuntimeStubConfig(ctx context.Context, stubID, workspaceID string) (*types.StubConfigV1, error) {
	stub, err := s.backendRepo.GetStubByExternalId(ctx, stubID, types.QueryFilter{Field: "workspace_id", Value: workspaceID})
	if err != nil {
		return nil, err
	}
	return stub.UnmarshalConfig()
}

func decryptMountPointConfig(secretKey []byte, config *pb.MountPointConfig) (*types.MountPointConfig, error) {
	accessKey, err := common.Decrypt(secretKey, config.AccessKey)
	if err != nil {
		return nil, err
	}
	secret, err := common.Decrypt(secretKey, config.SecretKey)
	if err != nil {
		return nil, err
	}
	return &types.MountPointConfig{
		BucketName:     config.BucketName,
		AccessKey:      string(accessKey),
		SecretKey:      string(secret),
		EndpointURL:    config.EndpointUrl,
		Region:         config.Region,
		ReadOnly:       config.ReadOnly,
		ForcePathStyle: config.ForcePathStyle,
	}, nil
}

func mountPathsForVolume(volumePath string) []string {
	paths := []string{path.Join(types.WorkerContainerVolumePath, volumePath)}
	if path.IsAbs(volumePath) {
		paths = append(paths, volumePath)
	}
	return paths
}
