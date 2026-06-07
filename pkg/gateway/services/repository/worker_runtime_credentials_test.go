package repository_services

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

type workerRuntimeCredentialsBackendRepo struct {
	repository.BackendRepository
	workspace                  *types.Workspace
	stub                       *types.StubWithRelated
	tokens                     []types.Token
	secrets                    []types.Secret
	workspaceByExternalIDCalls int
}

func (r *workerRuntimeCredentialsBackendRepo) GetWorkspaceByExternalId(ctx context.Context, externalID string) (types.Workspace, error) {
	r.workspaceByExternalIDCalls++
	workspace := *r.workspace
	workspace.ExternalId = externalID
	return workspace, nil
}

func (r *workerRuntimeCredentialsBackendRepo) GetWorkspace(ctx context.Context, workspaceID uint) (*types.Workspace, error) {
	workspace := *r.workspace
	workspace.Id = workspaceID
	return &workspace, nil
}

func (r *workerRuntimeCredentialsBackendRepo) GetSecretsByNameDecrypted(ctx context.Context, workspace *types.Workspace, names []string) ([]types.Secret, error) {
	return r.secrets, nil
}

func (r *workerRuntimeCredentialsBackendRepo) ListTokens(ctx context.Context, workspaceID uint) ([]types.Token, error) {
	return r.tokens, nil
}

func (r *workerRuntimeCredentialsBackendRepo) CreateToken(ctx context.Context, workspaceID uint, tokenType string, reusable bool) (types.Token, error) {
	token := types.Token{Key: "created-runtime-token", Active: true, TokenType: tokenType, Reusable: reusable}
	r.tokens = append(r.tokens, token)
	return token, nil
}

func (r *workerRuntimeCredentialsBackendRepo) GetStubByExternalId(ctx context.Context, externalID string, queryFilters ...types.QueryFilter) (*types.StubWithRelated, error) {
	return r.stub, nil
}

type workerRuntimeCredentialsContainerRepo struct {
	repository.ContainerRepository
	state *types.ContainerState
}

func (r *workerRuntimeCredentialsContainerRepo) GetContainerState(containerID string) (*types.ContainerState, error) {
	return r.state, nil
}

func TestGetContainerRuntimeCredentialsVendsPrivateRuntimeBundle(t *testing.T) {
	signingKey, signingKeyBytes := testSigningKey(t)
	accessKey, err := common.Encrypt(signingKeyBytes, "mount-access")
	require.NoError(t, err)
	secretKey, err := common.Encrypt(signingKeyBytes, "mount-secret")
	require.NoError(t, err)

	storageID := uint(1)
	storageBucket := "workspace-bucket"
	storageAccess := "storage-access"
	storageSecret := "storage-secret"
	storageEndpoint := "https://storage.example.com"
	storageRegion := "us-east-1"

	stubConfig := types.StubConfigV1{
		Volumes: []*pb.Volume{{
			Id:        "volume-id",
			MountPath: "data",
			Config: &pb.MountPointConfig{
				BucketName:     "mount-bucket",
				AccessKey:      accessKey,
				SecretKey:      secretKey,
				EndpointUrl:    "https://mount.example.com",
				Region:         "us-west-2",
				ForcePathStyle: true,
			},
		}},
	}
	configJSON, err := json.Marshal(stubConfig)
	require.NoError(t, err)

	service := &WorkerRepositoryService{
		backendRepo: &workerRuntimeCredentialsBackendRepo{
			workspace: &types.Workspace{
				Id:         7,
				ExternalId: "workspace-id",
				Name:       "workspace",
				SigningKey: &signingKey,
				Storage: &types.WorkspaceStorage{
					Id:          &storageID,
					BucketName:  &storageBucket,
					AccessKey:   &storageAccess,
					SecretKey:   &storageSecret,
					EndpointUrl: &storageEndpoint,
					Region:      &storageRegion,
				},
			},
			stub: &types.StubWithRelated{
				Stub: types.Stub{ExternalId: "stub-id", Config: string(configJSON)},
			},
			tokens: []types.Token{{
				Key:       "restricted-runtime-token",
				Active:    true,
				TokenType: types.TokenTypeWorkspaceRestricted,
			}},
			secrets: []types.Secret{{Name: "SECRET", Value: "secret-value"}},
		},
		containerRepo: &workerRuntimeCredentialsContainerRepo{
			state: &types.ContainerState{
				ContainerId: "container-id",
				WorkspaceId: "workspace-id",
				StubId:      "stub-id",
			},
		},
	}

	resp, err := service.GetContainerRuntimeCredentials(
		cacheRepositoryWorkspaceAuthContext("workspace-id"),
		&pb.GetContainerRuntimeCredentialsRequest{
			WorkspaceId:      "workspace-id",
			StubId:           "stub-id",
			ContainerId:      "container-id",
			SecretNames:      []string{"SECRET"},
			RuntimeToken:     true,
			WorkspaceStorage: true,
			MountCredentials: []*pb.RuntimeMountCredentialRequest{{
				MountPath:  "/volumes/data",
				BucketName: "mount-bucket",
			}},
		},
	)

	require.NoError(t, err)
	require.True(t, resp.Ok)
	require.ElementsMatch(t, []string{"SECRET=secret-value", "BETA9_TOKEN=restricted-runtime-token"}, resp.Env)
	require.NotNil(t, resp.WorkspaceStorage)
	require.Equal(t, "storage-access", resp.WorkspaceStorage.AccessKey)
	require.Equal(t, "storage-secret", resp.WorkspaceStorage.SecretKey)
	require.Len(t, resp.MountCredentials, 1)
	require.Equal(t, "mount-access", resp.MountCredentials[0].Config.AccessKey)
	require.Equal(t, "mount-secret", resp.MountCredentials[0].Config.SecretKey)
}

func TestGetContainerRuntimeCredentialsUsesWorkerTokenWorkspaceID(t *testing.T) {
	signingKey, _ := testSigningKey(t)
	workspaceID := uint(7)
	backendRepo := &workerRuntimeCredentialsBackendRepo{
		workspace: &types.Workspace{
			Id:         workspaceID,
			ExternalId: "workspace-id",
			SigningKey: &signingKey,
		},
	}
	service := &WorkerRepositoryService{
		backendRepo: backendRepo,
		containerRepo: &workerRuntimeCredentialsContainerRepo{
			state: &types.ContainerState{ContainerId: "container-id", WorkspaceId: "workspace-id", StubId: "stub-id"},
		},
	}
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Workspace: &types.Workspace{Id: workspaceID, ExternalId: "workspace-id"},
		Token:     &types.Token{TokenType: types.TokenTypeWorker, WorkspaceId: &workspaceID},
	})

	resp, err := service.GetContainerRuntimeCredentials(
		ctx,
		&pb.GetContainerRuntimeCredentialsRequest{WorkspaceId: "workspace-id", StubId: "stub-id", ContainerId: "container-id"},
	)

	require.NoError(t, err)
	require.True(t, resp.Ok)
	require.Zero(t, backendRepo.workspaceByExternalIDCalls)
}

func TestGetContainerRuntimeCredentialsRejectsMismatchedContainerState(t *testing.T) {
	signingKey, _ := testSigningKey(t)
	service := &WorkerRepositoryService{
		backendRepo: &workerRuntimeCredentialsBackendRepo{
			workspace: &types.Workspace{Id: 7, ExternalId: "workspace-id", SigningKey: &signingKey},
		},
		containerRepo: &workerRuntimeCredentialsContainerRepo{
			state: &types.ContainerState{ContainerId: "container-id", WorkspaceId: "other-workspace", StubId: "stub-id"},
		},
	}

	resp, err := service.GetContainerRuntimeCredentials(
		cacheRepositoryWorkspaceAuthContext("workspace-id"),
		&pb.GetContainerRuntimeCredentialsRequest{WorkspaceId: "workspace-id", StubId: "stub-id", ContainerId: "container-id"},
	)

	require.NoError(t, err)
	require.False(t, resp.Ok)
	require.Contains(t, resp.ErrorMsg, "not assigned")
}

func TestGetContainerRuntimeCredentialsRequiresWorkerToken(t *testing.T) {
	service := &WorkerRepositoryService{}

	resp, err := service.GetContainerRuntimeCredentials(
		cacheRepositoryAuthContext(types.TokenTypeWorkspace),
		&pb.GetContainerRuntimeCredentialsRequest{WorkspaceId: "workspace-id", StubId: "stub-id", ContainerId: "container-id"},
	)

	require.NoError(t, err)
	require.False(t, resp.Ok)
	require.Contains(t, resp.ErrorMsg, "worker token")
}

func testSigningKey(t *testing.T) (string, []byte) {
	t.Helper()
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	return "sk_" + base64.StdEncoding.EncodeToString(key), key
}
