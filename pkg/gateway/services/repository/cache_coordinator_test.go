package repository_services

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/common"
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
	activeKeys               []string
	checkpoints              []types.Checkpoint
	recentKeysByCheckpointID map[string]string
	pruneIDs                 []string
	workspaces               map[uint]*types.Workspace
}

func (r *pruneCheckpointBackendRepo) ListStaleCheckpoints(ctx context.Context, activeRecentStubKeys []string) ([]types.Checkpoint, error) {
	r.activeKeys = append([]string(nil), activeRecentStubKeys...)
	active := map[string]struct{}{}
	for _, key := range activeRecentStubKeys {
		active[key] = struct{}{}
	}
	checkpoints := make([]types.Checkpoint, 0, len(r.checkpoints))
	for _, checkpoint := range r.checkpoints {
		if key := r.recentKeysByCheckpointID[checkpoint.CheckpointId]; key != "" {
			if _, ok := active[key]; ok {
				continue
			}
		}
		checkpoints = append(checkpoints, checkpoint)
	}
	return checkpoints, nil
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
	workspace                 *types.Workspace
	secretName                string
	secret                    *types.Secret
	workspaceByExternalCalls  int
	workspaceWithSigningCalls int
	workspaceCalls            int
}

func (r *originCredentialsBackendRepo) GetImageCredentialSecret(ctx context.Context, imageID string) (string, string, error) {
	return r.secretName, "", nil
}

func (r *originCredentialsBackendRepo) GetWorkspaceByExternalId(ctx context.Context, externalID string) (types.Workspace, error) {
	r.workspaceByExternalCalls++
	if r.workspace == nil {
		return types.Workspace{}, nil
	}
	workspace := *r.workspace
	workspace.ExternalId = externalID
	return workspace, nil
}

func (r *originCredentialsBackendRepo) GetWorkspaceByExternalIdWithSigningKey(ctx context.Context, externalID string) (types.Workspace, error) {
	r.workspaceWithSigningCalls++
	if r.workspace == nil {
		return types.Workspace{}, nil
	}
	workspace := *r.workspace
	workspace.ExternalId = externalID
	return workspace, nil
}

func (r *originCredentialsBackendRepo) GetWorkspace(ctx context.Context, workspaceID uint) (*types.Workspace, error) {
	r.workspaceCalls++
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
	if workspace == nil || workspace.SigningKey == nil || *workspace.SigningKey == "" {
		return nil, nil
	}
	return r.secret, nil
}

func TestAuthorizeCacheRepositoryRequestWithWorkerToken(t *testing.T) {
	for _, tokenType := range []string{types.TokenTypeWorker, types.TokenTypeWorkerPrivate} {
		ctx := cacheRepositoryAuthContext(tokenType)

		if err := (&WorkerRepositoryService{}).authorizeCacheRepositoryRequest(ctx); err != nil {
			t.Fatalf("authorizeCacheRepositoryRequest failed for %s: %v", tokenType, err)
		}
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

func TestCacheCoordinatorScopesWorkerLocalityByWorkspaceAndPool(t *testing.T) {
	server, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(server.Close)

	rdb, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{server.Addr()},
		Mode:  types.RedisModeSingle,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })

	service := &WorkerRepositoryService{
		cacheCoordinator: cache.NewCoordinator(repository.NewCacheRedisRepository(rdb)),
	}
	registerCacheHost := func(ctx context.Context, logicalHostID, addr string) {
		resp, err := service.RegisterCacheHost(ctx, &pb.RegisterCacheHostRequest{
			Host: &pb.CacheCoordinatorHost{
				LogicalHostId:  logicalHostID,
				RegistrationId: "registration-" + logicalHostID,
				PoolName:       "shared-pool",
				Locality:       "shared-pool",
				Addr:           addr,
			},
			TtlSeconds: 30,
		})
		require.NoError(t, err)
		require.True(t, resp.Ok, resp.ErrorMsg)
	}

	ctxA := cacheRepositoryWorkspaceAuthContext("workspace-a")
	ctxB := cacheRepositoryWorkspaceAuthContext("workspace-b")
	registerCacheHost(ctxA, "host-a", "10.0.0.1:2050")
	registerCacheHost(ctxB, "host-b", "10.0.0.2:2050")

	respA, err := service.ListCacheHosts(ctxA, &pb.ListCacheHostsRequest{Locality: "shared-pool"})
	require.NoError(t, err)
	require.True(t, respA.Ok, respA.ErrorMsg)
	require.Len(t, respA.Hosts, 1)
	require.Equal(t, "host-a", respA.Hosts[0].LogicalHostId)
	require.Equal(t, "shared-pool", respA.Hosts[0].PoolName)
	require.Equal(t, "workspace-a/shared-pool", respA.Hosts[0].Locality)

	pooledRespA, err := service.ListCacheHosts(ctxA, &pb.ListCacheHostsRequest{PoolName: "shared-pool", Locality: "shared-pool"})
	require.NoError(t, err)
	require.True(t, pooledRespA.Ok, pooledRespA.ErrorMsg)
	require.Len(t, pooledRespA.Hosts, 1)
	require.Equal(t, "host-a", pooledRespA.Hosts[0].LogicalHostId)

	respB, err := service.ListCacheHosts(ctxB, &pb.ListCacheHostsRequest{Locality: "shared-pool"})
	require.NoError(t, err)
	require.True(t, respB.Ok, respB.ErrorMsg)
	require.Len(t, respB.Hosts, 1)
	require.Equal(t, "host-b", respB.Hosts[0].LogicalHostId)
	require.Equal(t, "workspace-b/shared-pool", respB.Hosts[0].Locality)
}

// Cluster workers (plain worker tokens) share the unscoped cache namespace
// with the cache-server daemonset, which registers via the coordinator token.
// Their listings must also fall back across pool names so daemonset hosts
// registered under a different pool stay discoverable. Only private-pool
// worker tokens (TokenTypeWorkerPrivate) are workspace-scoped.
func TestCacheCoordinatorClusterWorkersShareUnscopedLocalityWithDaemonSet(t *testing.T) {
	server, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(server.Close)

	rdb, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{server.Addr()},
		Mode:  types.RedisModeSingle,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })

	service := &WorkerRepositoryService{
		cacheCoordinator:      cache.NewCoordinator(repository.NewCacheRedisRepository(rdb)),
		cacheCoordinatorToken: "coordinator-token",
	}

	// The cache-server daemonset registers with the coordinator token under its
	// own pool name and an unscoped locality.
	daemonCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer coordinator-token"))
	registerResp, err := service.RegisterCacheHost(daemonCtx, &pb.RegisterCacheHostRequest{
		Host: &pb.CacheCoordinatorHost{
			LogicalHostId:  "daemonset-host",
			RegistrationId: "registration-daemonset-host",
			PoolName:       "cache-server",
			Locality:       "default",
			Addr:           "10.0.0.9:2050",
		},
		TtlSeconds: 30,
	})
	require.NoError(t, err)
	require.True(t, registerResp.Ok, registerResp.ErrorMsg)

	// A cluster worker keeps an unscoped locality and discovers the daemonset
	// host even though it lists under a different pool name.
	clusterCtx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Workspace: &types.Workspace{ExternalId: "admin-workspace"},
		Token:     &types.Token{TokenType: types.TokenTypeWorker},
	})
	clusterResp, err := service.ListCacheHosts(clusterCtx, &pb.ListCacheHostsRequest{PoolName: "gpu-pool", Locality: "default"})
	require.NoError(t, err)
	require.True(t, clusterResp.Ok, clusterResp.ErrorMsg)
	require.Len(t, clusterResp.Hosts, 1)
	require.Equal(t, "daemonset-host", clusterResp.Hosts[0].LogicalHostId)
	require.Equal(t, "default", clusterResp.Hosts[0].Locality)

	// A private-pool worker in a customer workspace stays scoped and must not
	// see the unscoped daemonset host.
	privateResp, err := service.ListCacheHosts(cacheRepositoryWorkspaceAuthContext("workspace-a"), &pb.ListCacheHostsRequest{PoolName: "gpu-pool", Locality: "default"})
	require.NoError(t, err)
	require.True(t, privateResp.Ok, privateResp.ErrorMsg)
	require.Empty(t, privateResp.Hosts)
}

func TestCacheMetadataScopesWorkerLocalityByWorkspace(t *testing.T) {
	server, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(server.Close)

	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	service := &WorkerRepositoryService{
		cacheMetadata: cache.NewRedisCacheMetadataStoreWithClient(cache.GlobalConfig{}, cache.ServerConfig{}, rdb),
	}

	resp, err := service.AddRecentCacheStub(cacheRepositoryWorkspaceAuthContext("workspace-a"), &pb.AddRecentCacheStubRequest{
		Locality:    "shared-pool",
		WorkspaceId: "workspace-a",
		StubId:      "stub-a",
		TtlSeconds:  3600,
	})
	require.NoError(t, err)
	require.True(t, resp.Ok, resp.ErrorMsg)

	resp, err = service.AddRecentCacheStub(cacheRepositoryWorkspaceAuthContext("workspace-b"), &pb.AddRecentCacheStubRequest{
		Locality:    "shared-pool",
		WorkspaceId: "workspace-b",
		StubId:      "stub-b",
		TtlSeconds:  3600,
	})
	require.NoError(t, err)
	require.True(t, resp.Ok, resp.ErrorMsg)

	listA, err := service.ListRecentCacheStubs(cacheRepositoryWorkspaceAuthContext("workspace-a"), &pb.ListRecentCacheStubsRequest{
		Locality:   "shared-pool",
		TtlSeconds: 3600,
		Limit:      10,
	})
	require.NoError(t, err)
	require.True(t, listA.Ok, listA.ErrorMsg)
	require.Len(t, listA.Stubs, 1)
	require.Equal(t, "workspace-a", listA.Stubs[0].WorkspaceId)
	require.Equal(t, "stub-a", listA.Stubs[0].StubId)
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

func TestPruneStaleCacheCheckpointsPrunesStaleSandboxAndEndpointCheckpoints(t *testing.T) {
	server, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(server.Close)

	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	now := time.Now()
	metadataStore := cache.NewRedisCacheMetadataStoreWithClient(cache.GlobalConfig{}, cache.ServerConfig{}, rdb)
	require.NoError(t, metadataStore.AddRecentStub(context.Background(), "locality-a", "workspace", "recent-sandbox-stub", time.Hour))
	backendRepo := &pruneCheckpointBackendRepo{
		checkpoints: []types.Checkpoint{
			{
				CheckpointId: "sandbox-checkpoint",
				StubType:     types.StubTypeSandbox,
				CreatedAt:    types.Time{Time: now.Add(-2 * time.Hour)},
			},
			{
				CheckpointId: "recent-sandbox-checkpoint",
				StubType:     types.StubTypeSandbox,
				CreatedAt:    types.Time{Time: now.Add(-2 * time.Hour)},
			},
			{
				CheckpointId:   "recently-restored-sandbox-checkpoint",
				StubType:       types.StubTypeSandbox,
				CreatedAt:      types.Time{Time: now.Add(-2 * time.Hour)},
				LastRestoredAt: types.Time{Time: now.Add(-5 * time.Minute)},
			},
			{
				CheckpointId: "fresh-endpoint-checkpoint",
				StubType:     types.StubTypeEndpointDeployment,
				CreatedAt:    types.Time{Time: now.Add(-5 * time.Minute)},
			},
			{
				CheckpointId: "old-endpoint-checkpoint",
				StubType:     types.StubTypeEndpointDeployment,
				CreatedAt:    types.Time{Time: now.Add(-2 * time.Hour)},
			},
		},
		recentKeysByCheckpointID: map[string]string{
			"recent-sandbox-checkpoint": cache.RecentStubKey("workspace", "recent-sandbox-stub"),
		},
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
	require.True(t, resp.Ok, resp.ErrorMsg)
	require.EqualValues(t, 2, resp.Pruned)
	require.Equal(t, []string{"sandbox-checkpoint", "old-endpoint-checkpoint"}, backendRepo.pruneIDs)
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
			StubType:     types.StubTypeEndpointDeployment,
			CreatedAt:    types.Time{Time: time.Now().Add(-2 * time.Hour)},
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
	signingKey := "workspace-signing-key"
	backendRepo := &originCredentialsBackendRepo{
		workspace:  &types.Workspace{Id: 7, ExternalId: "workspace-id", SigningKey: &signingKey},
		secretName: "registry-secret",
		secret:     &types.Secret{Name: "registry-secret", Value: "registry-user:registry-pass"},
	}
	service := &WorkerRepositoryService{
		backendRepo: backendRepo,
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
	require.Equal(t, 1, backendRepo.workspaceWithSigningCalls)
	require.Equal(t, 1, backendRepo.workspaceByExternalCalls)
	require.Equal(t, 1, backendRepo.workspaceCalls)
}

func TestGetCacheOriginCredentialsDoesNotDecryptImageRegistrySecretWithoutSigningKey(t *testing.T) {
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
	require.Empty(t, resp.RegistryCredentials)
}

func TestGetCacheOriginCredentialsVendsImageArchiveURL(t *testing.T) {
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
	require.Nil(t, resp.ImageArchiveStorage)
	require.Equal(t, "image-id."+reg.RemoteImageFileExtension, resp.ImageArchiveObjectKey)
	require.Contains(t, resp.ImageArchiveUrl, "image-id."+reg.RemoteImageFileExtension)
	require.Contains(t, resp.ImageArchiveUrl, "X-Amz-Credential=")
	require.Contains(t, resp.ImageArchiveDataUrl, "image-id."+reg.LocalImageFileExtension)
	require.Contains(t, resp.ImageArchiveDataUrl, "X-Amz-Credential=")
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

func TestGetCacheOriginCredentialsRejectsNilRequest(t *testing.T) {
	service := &WorkerRepositoryService{}

	resp, err := service.GetCacheOriginCredentials(cacheRepositoryAuthContext(types.TokenTypeWorker), nil)

	require.NoError(t, err)
	require.False(t, resp.Ok)
	require.Contains(t, resp.ErrorMsg, "request")
}

func cacheRepositoryAuthContext(tokenType string) context.Context {
	return auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Token: &types.Token{TokenType: tokenType},
	})
}

// cacheRepositoryWorkspaceAuthContext simulates a private-pool worker token,
// which carries the pool owner's workspace and is subject to workspace scoping.
func cacheRepositoryWorkspaceAuthContext(workspaceID string) context.Context {
	return auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Workspace: &types.Workspace{ExternalId: workspaceID},
		Token:     &types.Token{TokenType: types.TokenTypeWorkerPrivate},
	})
}
