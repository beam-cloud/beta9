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
		{name: "worker", token: types.Token{TokenType: types.TokenTypeWorker}, owner: 1, withdrawn: true},
		{name: "public snapshot of another workspace", token: types.Token{TokenType: types.TokenTypeWorker}, owner: 2},
		{name: "token of another workspace", token: types.Token{TokenType: types.TokenTypeWorkspace}, owner: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			repo := &failDiskSnapshotBackendRepo{snapshot: &types.DiskSnapshot{ExternalId: "snap", WorkspaceId: tt.owner, Status: types.DiskSnapshotStatusAvailable}}
			service := &BackendRepositoryService{backendRepo: repo}
			ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
				Token:     &tt.token,
				Workspace: &types.Workspace{ExternalId: "other"},
			})

			response, err := service.FailDiskSnapshot(ctx, &pb.FailDiskSnapshotRequest{WorkspaceId: "own", SnapshotId: "snap", Reason: "not checkpointed"})
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

func TestAuthorizeDiskSnapshotWorkspace(t *testing.T) {
	for _, tt := range []struct {
		name          string
		tokenType     string
		authWorkspace string
		requested     string
		allowed       bool
	}{
		{name: "primary", tokenType: types.TokenTypeWorkspacePrimary, authWorkspace: "own", requested: "own", allowed: true},
		{name: "workspace", tokenType: types.TokenTypeWorkspace, authWorkspace: "own", requested: "own", allowed: true},
		{name: "workspace cross tenant", tokenType: types.TokenTypeWorkspace, authWorkspace: "own", requested: "other"},
		{name: "restricted cross tenant", tokenType: types.TokenTypeWorkspaceRestricted, authWorkspace: "own", requested: "other"},
		{name: "private worker", tokenType: types.TokenTypeWorkerPrivate, authWorkspace: "own", requested: "own", allowed: true},
		{name: "private worker cross tenant", tokenType: types.TokenTypeWorkerPrivate, authWorkspace: "own", requested: "other"},
		{name: "machine cross tenant", tokenType: types.TokenTypeMachine, authWorkspace: "own", requested: "other"},
		{name: "trusted worker cross tenant", tokenType: types.TokenTypeWorker, authWorkspace: "own", requested: "other", allowed: true},
		{name: "admin cross tenant", tokenType: types.TokenTypeClusterAdmin, authWorkspace: "own", requested: "other", allowed: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
				Token:     &types.Token{TokenType: tt.tokenType},
				Workspace: &types.Workspace{ExternalId: tt.authWorkspace},
			})
			err := authorizeDiskSnapshotWorkspace(ctx, tt.requested)
			if tt.allowed {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}

	require.Error(t, authorizeDiskSnapshotWorkspace(context.Background(), "workspace"))
}

func TestGetDiskSnapshotDownloadURLRejectsCrossWorkspaceToken(t *testing.T) {
	repo := &diskSnapshotDownloadBackendRepo{}
	service := &BackendRepositoryService{backendRepo: repo}
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Token:     &types.Token{TokenType: types.TokenTypeWorkspacePrimary},
		Workspace: &types.Workspace{ExternalId: "own"},
	})

	response, err := service.GetDiskSnapshotDownloadURL(ctx, &pb.GetDiskSnapshotDownloadURLRequest{WorkspaceId: "other"})
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Contains(t, response.ErrorMsg, "cannot access workspace")
	require.Zero(t, repo.workspaceLookups)
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
