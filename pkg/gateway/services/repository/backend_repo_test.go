package repository_services

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

func TestBuildRegistryCredentialsUsesFallbackWithoutLeakingToOtherHosts(t *testing.T) {
	service := &WorkerRepositoryService{appConfig: types.AppConfig{ImageService: types.ImageServiceConfig{
		Runner:              types.RunnerConfig{BaseImageRegistry: "registry.example.com"},
		BuildRepositoryName: "stage/vms",
		BuildRegistryCredentials: types.BuildRegistryCredentialsConfig{
			Type: "basic", Credentials: map[string]string{"USERNAME": "user", "PASSWORD": "secret"},
		},
	}}}
	for _, tt := range []struct{ registry, want string }{
		{"registry.example.com", "user:secret"},
		{"https://registry.example.com/stage/vms", "user:secret"},
		{"registry.example.com.evil.test", ""},
		{"other.example.com", ""},
		{"", ""},
	} {
		t.Run(tt.registry, func(t *testing.T) {
			require.Equal(t, tt.want, service.buildRegistryCredentials(context.Background(), tt.registry))
		})
	}
}

type checkpointLookupBackendRepo struct {
	repository.BackendRepository
	checkpoint *types.Checkpoint
	key        string
}

func (r *checkpointLookupBackendRepo) GetLatestCheckpointByStubId(_ context.Context, _ string, keys ...string) (*types.Checkpoint, error) {
	r.key = keys[0]
	if r.checkpoint == nil {
		return nil, &types.ErrCheckpointNotFound{}
	}
	return r.checkpoint, nil
}

func TestGetLatestCheckpointReturnsStoredCompatibilityKey(t *testing.T) {
	for _, key := range []string{"v2:profile", "v1:profile"} {
		t.Run(key, func(t *testing.T) {
			repo := &checkpointLookupBackendRepo{checkpoint: &types.Checkpoint{CheckpointId: "saved", CompatibilityKey: key}}
			service := &BackendRepositoryService{backendRepo: repo}
			request := &pb.GetLatestCheckpointByStubIdRequest{StubId: "stub", CompatibilityKey: "v2:profile"}
			response, err := service.GetLatestCheckpointByStubId(context.Background(), request)
			require.NoError(t, err)
			require.True(t, response.Ok)
			require.Equal(t, "saved", response.Checkpoint.CheckpointId)
			require.Equal(t, key, response.CompatibilityKey)
			require.Equal(t, request.CompatibilityKey, repo.key)

			repo.checkpoint = nil
			response, err = service.GetLatestCheckpointByStubId(context.Background(), request)
			require.NoError(t, err)
			require.True(t, response.Ok)
			require.Nil(t, response.Checkpoint)
			require.Equal(t, request.CompatibilityKey, response.CompatibilityKey)
		})
	}
}

type diskSnapshotDownloadBackendRepo struct {
	repository.BackendRepository
	workspaceLookups int
}

func (r *diskSnapshotDownloadBackendRepo) GetWorkspaceByExternalId(context.Context, string) (types.Workspace, error) {
	r.workspaceLookups++
	return types.Workspace{}, nil
}

type failDiskSnapshotBackendRepo struct {
	repository.BackendRepository
	snapshot *types.DiskSnapshot
	updated  *types.DiskSnapshot
}

func (r *failDiskSnapshotBackendRepo) GetWorkspaceByExternalId(_ context.Context, externalID string) (types.Workspace, error) {
	return types.Workspace{Id: 1, ExternalId: externalID}, nil
}

func (r *failDiskSnapshotBackendRepo) GetDiskSnapshot(context.Context, uint, string) (*types.DiskSnapshot, error) {
	return r.snapshot, nil
}

func (r *failDiskSnapshotBackendRepo) UpdateDiskSnapshot(_ context.Context, snapshot *types.DiskSnapshot) (*types.DiskSnapshot, error) {
	r.updated = snapshot
	return snapshot, nil
}

func TestFailDiskSnapshotWithdrawsOnlyOwnSnapshots(t *testing.T) {
	for _, tt := range []struct {
		name      string
		token     types.Token
		owner     uint
		withdrawn bool
	}{
		{name: "worker", token: types.Token{TokenType: types.TokenTypeWorker, WorkerID: "worker"}, owner: 1, withdrawn: true},
		{name: "public snapshot of another workspace", token: types.Token{TokenType: types.TokenTypeWorker, WorkerID: "worker"}, owner: 2},
		{name: "private worker of another workspace", token: types.Token{TokenType: types.TokenTypeWorkerPrivate, WorkerID: "worker"}, owner: 1},
		{name: "workspace token", token: types.Token{TokenType: types.TokenTypeWorkspace}, owner: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			repo := &failDiskSnapshotBackendRepo{snapshot: &types.DiskSnapshot{ExternalId: "snap", WorkspaceId: tt.owner, Status: types.DiskSnapshotStatusAvailable}}
			service := &BackendRepositoryService{backendRepo: repo}
			ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
				Token:     &tt.token,
				Workspace: &types.Workspace{ExternalId: "other"},
			})

			req := &pb.FailDiskSnapshotRequest{WorkspaceId: "own", SnapshotId: "snap", Reason: "not checkpointed"}
			if err := service.AuthorizeRequest(ctx, req); err != nil {
				require.False(t, tt.withdrawn, err)
				require.Nil(t, repo.updated)
				return
			}
			response, err := service.FailDiskSnapshot(ctx, req)
			require.NoError(t, err)
			if !tt.withdrawn {
				require.False(t, response.Ok)
				require.Nil(t, repo.updated)
				return
			}
			require.True(t, response.Ok, response.ErrorMsg)
			require.Equal(t, &types.DiskSnapshot{ExternalId: "snap", Status: types.DiskSnapshotStatusFailed, Reason: "not checkpointed"}, repo.updated)
		})
	}
}

func TestDiskSnapshotPublicProtoRoundTrip(t *testing.T) {
	snapshot := diskSnapshotFromProto(diskSnapshotToProto(&types.DiskSnapshot{Public: true}))
	require.True(t, snapshot.Public)
}

func TestDiskSnapshotManifestChunkKeys(t *testing.T) {
	data, err := json.Marshal(types.DiskSnapshotManifest{Files: []types.DiskSnapshotFile{{
		Chunks: []types.DiskSnapshotChunk{{ObjectKey: "durable-disks/source/chunks/allowed"}},
	}}})
	require.NoError(t, err)
	digest := sha256.Sum256(data)

	keys, err := diskSnapshotManifestChunkKeys(data, "sha256:"+hex.EncodeToString(digest[:]))
	require.NoError(t, err)
	snapshot := &types.DiskSnapshot{ManifestKey: "durable-disks/source/snapshots/1/manifest.json"}
	require.True(t, diskSnapshotObjectKeyAllowed(snapshot, snapshot.ManifestKey, keys))
	require.True(t, diskSnapshotObjectKeyAllowed(snapshot, "durable-disks/source/chunks/allowed", keys))
	require.False(t, diskSnapshotObjectKeyAllowed(snapshot, "durable-disks/source/chunks/private", keys))

	_, err = diskSnapshotManifestChunkKeys(data, "sha256:wrong")
	require.ErrorContains(t, err, "digest mismatch")
}

func TestAuthorizeRequestLimitsDiskSnapshotsToTheWorkersWorkspace(t *testing.T) {
	for _, tt := range []struct {
		name      string
		tokenType string
		requested string
		allowed   bool
	}{
		{name: "private worker", tokenType: types.TokenTypeWorkerPrivate, requested: "own", allowed: true},
		{name: "private worker cross tenant", tokenType: types.TokenTypeWorkerPrivate, requested: "other"},
		{name: "private worker request without a workspace", tokenType: types.TokenTypeWorkerPrivate},
		{name: "trusted worker cross tenant", tokenType: types.TokenTypeWorker, requested: "other", allowed: true},
		{name: "workspace token", tokenType: types.TokenTypeWorkspacePrimary, requested: "own"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			repo := &diskSnapshotDownloadBackendRepo{}
			service := &BackendRepositoryService{backendRepo: repo}
			ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
				Token:     &types.Token{TokenType: tt.tokenType, WorkerID: "worker"},
				Workspace: &types.Workspace{ExternalId: "own"},
			})
			for _, req := range []interface{}{
				&pb.CreateDiskSnapshotRequest{WorkspaceId: tt.requested},
				&pb.FailDiskSnapshotRequest{WorkspaceId: tt.requested},
				&pb.GetLatestDiskSnapshotRequest{WorkspaceId: tt.requested},
				&pb.GetDiskSnapshotRequest{WorkspaceId: tt.requested},
				&pb.GetDiskSnapshotDownloadURLRequest{WorkspaceId: tt.requested},
			} {
				err := service.AuthorizeRequest(ctx, req)
				if tt.allowed {
					require.NoError(t, err, "%T", req)
				} else {
					require.Error(t, err, "%T", req)
				}
			}
			require.Zero(t, repo.workspaceLookups)
		})
	}
}

func TestGetCachedDiskSnapshotChunkKeys(t *testing.T) {
	now := time.Now()
	keys := map[string]struct{}{"chunk": {}}
	service := &BackendRepositoryService{diskSnapshotObjectCache: map[string]diskSnapshotObjectCacheEntry{
		"snapshot": {digest: "digest", expiresAt: now.Add(time.Minute), keys: keys},
	}}

	got, ok := service.getCachedDiskSnapshotChunkKeys(&types.DiskSnapshot{ExternalId: "snapshot", ManifestDigest: "digest"}, now)
	require.True(t, ok)
	require.Equal(t, keys, got)

	_, ok = service.getCachedDiskSnapshotChunkKeys(&types.DiskSnapshot{ExternalId: "snapshot", ManifestDigest: "other"}, now)
	require.False(t, ok)

	_, ok = service.getCachedDiskSnapshotChunkKeys(&types.DiskSnapshot{ExternalId: "snapshot", ManifestDigest: "digest"}, now.Add(time.Minute))
	require.False(t, ok)
}

type checkpointAuthBackendRepo struct{ repository.BackendRepository }

var checkpointAuthWorkspaces = map[string]uint{"own": 1, "other": 2}

func (checkpointAuthBackendRepo) GetStubByExternalId(_ context.Context, id string, _ ...types.QueryFilter) (*types.StubWithRelated, error) {
	return &types.StubWithRelated{Stub: types.Stub{ExternalId: id, WorkspaceId: checkpointAuthWorkspaces[id]}}, nil
}

func (checkpointAuthBackendRepo) GetCheckpointById(_ context.Context, id string) (*types.Checkpoint, error) {
	return &types.Checkpoint{CheckpointId: id, WorkspaceId: checkpointAuthWorkspaces[id]}, nil
}

func TestCheckpointAuthorizationScopesUntrustedWorkersToTheirWorkspace(t *testing.T) {
	service := &BackendRepositoryService{backendRepo: checkpointAuthBackendRepo{}}
	requests := func(workspace string) []interface{} {
		return []interface{}{
			&pb.CreateCheckpointRequest{StubId: workspace},
			&pb.GetLatestCheckpointByStubIdRequest{StubId: workspace},
			&pb.UpdateCheckpointRequest{CheckpointId: workspace},
		}
	}
	for _, tokenType := range []string{types.TokenTypeWorker, types.TokenTypeWorkerPrivate} {
		ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
			Token:     &types.Token{TokenType: tokenType, WorkerID: "worker"},
			Workspace: &types.Workspace{Id: 1, ExternalId: "own"},
		})
		for _, req := range requests("own") {
			require.NoError(t, service.AuthorizeRequest(ctx, req), "%s %T", tokenType, req)
		}
		for _, req := range requests("other") {
			err := service.AuthorizeRequest(ctx, req)
			if tokenType == types.TokenTypeWorker {
				require.NoError(t, err, "%T", req)
			} else {
				require.ErrorIs(t, err, errCheckpointNotPermitted, "%T", req)
			}
		}
		require.ErrorIs(t, service.AuthorizeRequest(ctx, &pb.GetCheckpointByIdRequest{CheckpointId: "own"}), errCheckpointNotPermitted)
		require.ErrorIs(t, service.AuthorizeRequest(ctx, &pb.ListCheckpointsRequest{WorkspaceId: "own"}), errCheckpointNotPermitted)
	}
}
