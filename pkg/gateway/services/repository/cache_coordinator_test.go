package repository_services

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/cache"
	reg "github.com/beam-cloud/beta9/pkg/registry"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

type pruneCheckpointBackendRepo struct {
	repository.BackendRepository
	activeKeys  []string
	checkpoints []types.Checkpoint
	pruneIDs    []string
	workspaces  map[uint]*types.Workspace
}

func (r *pruneCheckpointBackendRepo) ListStaleCheckpoints(ctx context.Context, activeRecentStubKeys []string) ([]types.Checkpoint, error) {
	r.activeKeys = append([]string(nil), activeRecentStubKeys...)
	return r.checkpoints, nil
}

func (r *pruneCheckpointBackendRepo) PruneCheckpoints(ctx context.Context, checkpointIDs []string) ([]types.Checkpoint, error) {
	r.pruneIDs = append([]string(nil), checkpointIDs...)
	pruned := make([]types.Checkpoint, 0, len(checkpointIDs))
	for _, checkpointID := range checkpointIDs {
		pruned = append(pruned, types.Checkpoint{CheckpointId: checkpointID})
	}
	return pruned, nil
}

func (r *pruneCheckpointBackendRepo) GetWorkspace(ctx context.Context, workspaceID uint) (*types.Workspace, error) {
	if r.workspaces != nil {
		if workspace, ok := r.workspaces[workspaceID]; ok {
			return workspace, nil
		}
	}
	return &types.Workspace{}, nil
}

type originCredentialsBackendRepo struct {
	repository.BackendRepository
	workspace  *types.Workspace
	secretName string
	secret     *types.Secret
}

func (r *originCredentialsBackendRepo) GetImageCredentialSecret(ctx context.Context, imageID string) (string, string, error) {
	return r.secretName, "", nil
}

func (r *originCredentialsBackendRepo) GetWorkspaceByExternalId(ctx context.Context, externalID string) (types.Workspace, error) {
	if r.workspace == nil {
		return types.Workspace{}, nil
	}
	workspace := *r.workspace
	workspace.ExternalId = externalID
	return workspace, nil
}

func (r *originCredentialsBackendRepo) GetWorkspace(ctx context.Context, workspaceID uint) (*types.Workspace, error) {
	if r.workspace == nil {
		return &types.Workspace{Id: workspaceID}, nil
	}
	workspace := *r.workspace
	workspace.Id = workspaceID
	return &workspace, nil
}

func (r *originCredentialsBackendRepo) GetSecretByNameDecrypted(ctx context.Context, workspace *types.Workspace, name string) (*types.Secret, error) {
	if name != r.secretName {
		return nil, nil
	}
	return r.secret, nil
}

func TestAuthorizeCacheRepositoryRequestWithWorkerToken(t *testing.T) {
	ctx := cacheRepositoryAuthContext(types.TokenTypeWorker)

	if err := (&WorkerRepositoryService{}).authorizeCacheRepositoryRequest(ctx); err != nil {
		t.Fatalf("authorizeCacheRepositoryRequest failed: %v", err)
	}
}

func TestAuthorizeCacheRepositoryRequestWithCoordinatorToken(t *testing.T) {
	service := &WorkerRepositoryService{cacheCoordinatorToken: "coordinator-token"}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer coordinator-token"))

	if err := service.authorizeCacheRepositoryRequest(ctx); err != nil {
		t.Fatalf("authorizeCacheRepositoryRequest failed: %v", err)
	}
}

func TestAuthorizeCacheRepositoryRequestRejectsMissingOrNonWorkerToken(t *testing.T) {
	for _, ctx := range []context.Context{
		context.Background(),
		cacheRepositoryAuthContext(types.TokenTypeWorkspace),
		cacheRepositoryAuthContext(types.TokenTypeClusterAdmin),
		auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{}),
	} {
		if err := (&WorkerRepositoryService{}).authorizeCacheRepositoryRequest(ctx); err == nil {
			t.Fatal("authorizeCacheRepositoryRequest succeeded, want error")
		}
	}
}

func TestAuthorizeCacheRepositoryRequestRejectsWrongCoordinatorToken(t *testing.T) {
	service := &WorkerRepositoryService{cacheCoordinatorToken: "coordinator-token"}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer wrong-token"))

	if err := service.authorizeCacheRepositoryRequest(ctx); err == nil {
		t.Fatal("authorizeCacheRepositoryRequest succeeded, want error")
	}
}

func TestConfiguredCacheCoordinatorTokenUsesEnvOverride(t *testing.T) {
	t.Setenv(types.CacheCoordinatorTokenEnv, "env-coordinator-token")

	if got := configuredCacheCoordinatorToken("config-coordinator-token"); got != "env-coordinator-token" {
		t.Fatalf("configuredCacheCoordinatorToken() = %q, want env-coordinator-token", got)
	}
}

func TestPruneStaleCacheCheckpointsUsesRecentStubsAcrossLocalities(t *testing.T) {
	server, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(server.Close)

	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	metadataStore := cache.NewRedisCacheMetadataStoreWithClient(cache.GlobalConfig{}, cache.ServerConfig{}, rdb)
	require.NoError(t, metadataStore.AddRecentStub(context.Background(), "locality-a", "workspace", "stub-a", time.Hour))
	require.NoError(t, metadataStore.AddRecentStub(context.Background(), "locality-b", "workspace", "stub-b", time.Hour))

	backendRepo := &pruneCheckpointBackendRepo{}
	service := &WorkerRepositoryService{
		cacheMetadata: metadataStore,
		backendRepo:   backendRepo,
		appConfig: types.AppConfig{
			Cache: cache.Config{
				Reconciliation: cache.ReconciliationConfig{RecentStubTTLSeconds: 3600},
			},
		},
	}

	resp, err := service.PruneStaleCacheCheckpoints(
		cacheRepositoryAuthContext(types.TokenTypeWorker),
		&pb.PruneStaleCacheCheckpointsRequest{},
	)

	require.NoError(t, err)
	require.True(t, resp.Ok)
	require.ElementsMatch(t, []string{
		cache.RecentStubKey("workspace", "stub-a"),
		cache.RecentStubKey("workspace", "stub-b"),
	}, backendRepo.activeKeys)
}

func TestPruneStaleCacheCheckpointsDefersDbPruneWhenOriginDeleteCannotRun(t *testing.T) {
	server, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(server.Close)

	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	metadataStore := cache.NewRedisCacheMetadataStoreWithClient(cache.GlobalConfig{}, cache.ServerConfig{}, rdb)
	backendRepo := &pruneCheckpointBackendRepo{
		checkpoints: []types.Checkpoint{{
			CheckpointId: "checkpoint-a",
			WorkspaceId:  7,
			OriginKey:    "checkpoints/checkpoint-a.tar",
		}},
		workspaces: map[uint]*types.Workspace{7: {Name: "workspace"}},
	}
	service := &WorkerRepositoryService{
		cacheMetadata: metadataStore,
		backendRepo:   backendRepo,
		appConfig: types.AppConfig{
			Cache: cache.Config{
				Reconciliation: cache.ReconciliationConfig{RecentStubTTLSeconds: 3600},
			},
		},
	}

	resp, err := service.PruneStaleCacheCheckpoints(
		cacheRepositoryAuthContext(types.TokenTypeWorker),
		&pb.PruneStaleCacheCheckpointsRequest{},
	)

	require.NoError(t, err)
	require.False(t, resp.Ok)
	require.Contains(t, resp.ErrorMsg, "workspace storage is unavailable")
	require.Empty(t, backendRepo.pruneIDs)
}

func TestGetCacheOriginCredentialsVendsImageRegistrySecret(t *testing.T) {
	service := &WorkerRepositoryService{
		backendRepo: &originCredentialsBackendRepo{
			workspace:  &types.Workspace{Id: 7, ExternalId: "workspace-id"},
			secretName: "registry-secret",
			secret:     &types.Secret{Name: "registry-secret", Value: "registry-user:registry-pass"},
		},
	}

	resp, err := service.GetCacheOriginCredentials(
		cacheRepositoryWorkspaceAuthContext("workspace-id"),
		&pb.GetCacheOriginCredentialsRequest{
			WorkspaceId: "workspace-id",
			StubId:      "stub-id",
			ImageId:     "image-id",
			Registry:    "registry.example.com",
		},
	)

	require.NoError(t, err)
	require.True(t, resp.Ok)
	require.Equal(t, "registry-user:registry-pass", resp.RegistryCredentials)
}

func TestGetCacheOriginCredentialsVendsImageArchiveStorage(t *testing.T) {
	service := &WorkerRepositoryService{
		backendRepo: &originCredentialsBackendRepo{
			workspace: &types.Workspace{Id: 7, ExternalId: "workspace-id"},
		},
		appConfig: types.AppConfig{
			ImageService: types.ImageServiceConfig{
				RegistryStore: reg.S3ImageRegistryStore,
				Registries: types.ImageRegistriesConfig{
					S3: types.S3ImageRegistryConfig{
						BucketName:     "image-bucket",
						Region:         "us-east-1",
						Endpoint:       "https://objects.example.com",
						AccessKey:      "access",
						SecretKey:      "secret",
						ForcePathStyle: true,
					},
				},
			},
		},
	}

	resp, err := service.GetCacheOriginCredentials(
		cacheRepositoryWorkspaceAuthContext("workspace-id"),
		&pb.GetCacheOriginCredentialsRequest{
			WorkspaceId: "workspace-id",
			StubId:      "stub-id",
			ImageId:     "image-id",
		},
	)

	require.NoError(t, err)
	require.True(t, resp.Ok)
	require.NotNil(t, resp.ImageArchiveStorage)
	require.Equal(t, "image-bucket", resp.ImageArchiveStorage.BucketName)
	require.Equal(t, "image-id."+reg.RemoteImageFileExtension, resp.ImageArchiveObjectKey)
}

func TestGetCacheOriginCredentialsRejectsWrongWorkerWorkspace(t *testing.T) {
	service := &WorkerRepositoryService{}

	resp, err := service.GetCacheOriginCredentials(
		cacheRepositoryWorkspaceAuthContext("workspace-a"),
		&pb.GetCacheOriginCredentialsRequest{WorkspaceId: "workspace-b"},
	)

	require.NoError(t, err)
	require.False(t, resp.Ok)
	require.Contains(t, resp.ErrorMsg, "workspace")
}

func cacheRepositoryAuthContext(tokenType string) context.Context {
	return auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Token: &types.Token{TokenType: tokenType},
	})
}

func cacheRepositoryWorkspaceAuthContext(workspaceID string) context.Context {
	return auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Workspace: &types.Workspace{ExternalId: workspaceID},
		Token:     &types.Token{TokenType: types.TokenTypeWorker},
	})
}
