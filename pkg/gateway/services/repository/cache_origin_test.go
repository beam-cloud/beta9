package repository_services

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

type cacheOriginEventRepo struct {
	repository.EventRepository
	items []types.CacheRequiredContentItem
}

func (r *cacheOriginEventRepo) ReadStubCacheRequiredContent(context.Context, string, string) ([]types.CacheRequiredContentItem, error) {
	return r.items, nil
}

type cacheOriginRecentStubStore struct {
	cache.CacheMetadataStore
	byLocality map[string][]cache.RecentStub
}

func (s cacheOriginRecentStubStore) ListRecentStubs(_ context.Context, locality string, _ time.Duration, _ int) ([]cache.RecentStub, error) {
	return s.byLocality[locality], nil
}

func TestCacheOriginWorkspaceObjectRequiresRecordedContent(t *testing.T) {
	base := context.Background()
	coordinator := metadata.NewIncomingContext(base, metadata.Pairs("authorization", "Bearer cache-token"))
	worker := cacheRepositoryAuthContext(types.TokenTypeWorkerPrivate, "workspace-id")
	recent := []cache.RecentStub{{WorkspaceID: "workspace-id", StubID: "stub-id"}}
	metadataStore := cacheOriginRecentStubStore{byLocality: map[string][]cache.RecentStub{"local": recent, "workspace-id/local": recent}}

	storageID := uint(1)
	bucket := "workspace-bucket"
	endpoint := "https://storage.example.com"
	region := "us-east-1"
	access := "storage-access"
	secret := "storage-secret"
	service := &WorkerRepositoryService{
		cacheCoordinatorToken: "cache-token",
		cacheMetadata:         metadataStore,
		backendRepo: &originCredentialsBackendRepo{stub: &types.StubWithRelated{
			Stub: types.Stub{ExternalId: "stub-id"},
			Workspace: types.Workspace{
				Id: 7, Name: "test-workspace", ExternalId: "workspace-id",
				Storage: &types.WorkspaceStorage{
					Id: &storageID, BucketName: &bucket, EndpointUrl: &endpoint, Region: &region,
					AccessKey: &access, SecretKey: &secret,
				},
			},
		}},
		eventRepo: &cacheOriginEventRepo{items: []types.CacheRequiredContentItem{
			{Kind: types.CacheContentKindVolume, Hash: "volume-hash", Source: "volumes/example/object"},
			{Kind: types.CacheContentKindDiskSnapshot, Hash: "disk-hash", Source: "durable-disks/disk/chunks/key", SourceBucket: bucket},
			{Kind: types.CacheContentKindVolume, Hash: "other-bucket-hash", Source: "volumes/other/object", SourceBucket: "unrelated-bucket"},
			{Kind: types.CacheContentKindVolume, Hash: "other-prefix-hash", Source: "objects/example"},
			{Kind: types.CacheContentKindCheckpoint, Hash: "retired-hash", Source: "checkpoints/old", Retired: true},
		}},
	}

	for _, ctx := range []context.Context{coordinator, worker} {
		req := &pb.GetCacheOriginCredentialsRequest{WorkspaceId: "workspace-id", StubId: "stub-id", CacheLocality: "local", Source: "volumes/example/object", SourceHash: "volume-hash"}
		resp, err := service.GetCacheOriginCredentials(ctx, req)
		require.NoError(t, err)
		require.True(t, resp.Ok, resp.ErrorMsg)
		presigned, err := url.Parse(resp.WorkspaceObjectUrl)
		require.NoError(t, err)
		require.Equal(t, "storage.example.com", presigned.Host)
		require.Equal(t, "/workspace-bucket/volumes/example/object", presigned.Path)
		require.Equal(t, "300", presigned.Query().Get("X-Amz-Expires"))
		require.Nil(t, resp.WorkspaceStorage)
		require.Empty(t, resp.RegistryCredentials)

		req.Source, req.SourceHash, req.SourceBucket = "durable-disks/disk/chunks/key", "disk-hash", bucket
		resp, err = service.GetCacheOriginCredentials(ctx, req)
		require.NoError(t, err)
		require.True(t, resp.Ok, resp.ErrorMsg)
		presigned, err = url.Parse(resp.WorkspaceObjectUrl)
		require.NoError(t, err)
		require.Equal(t, "/workspace-bucket/durable-disks/disk/chunks/key", presigned.Path)
	}

	for _, testcase := range []struct{ workspace, locality, source, hash, bucket string }{
		{"different-workspace", "local", "volumes/example/object", "volume-hash", ""},
		{"workspace-id", "different-locality", "volumes/example/object", "volume-hash", ""},
		{"workspace-id", "workspace-id/local", "volumes/example/object", "volume-hash", ""},
		{"workspace-id", "local", "volumes/example/unrecorded", "volume-hash", ""},
		{"workspace-id", "local", "volumes/example/object", "different-hash", ""},
		{"workspace-id", "local", "volumes/other/object", "other-bucket-hash", "unrelated-bucket"},
		{"workspace-id", "local", "objects/example", "other-prefix-hash", ""},
		{"workspace-id", "local", "checkpoints/old", "retired-hash", ""},
	} {
		resp, err := service.GetCacheOriginCredentials(coordinator, &pb.GetCacheOriginCredentialsRequest{
			WorkspaceId: testcase.workspace, StubId: "stub-id", CacheLocality: testcase.locality,
			Source: testcase.source, SourceHash: testcase.hash, SourceBucket: testcase.bucket,
		})
		require.NoError(t, err)
		require.False(t, resp.Ok)
		require.Empty(t, resp.WorkspaceObjectUrl)
		require.Nil(t, resp.WorkspaceStorage)
	}

	resp, err := service.AddRecentCacheStub(coordinator, &pb.AddRecentCacheStubRequest{WorkspaceId: "workspace-id", StubId: "stub-id", Locality: "local"})
	require.NoError(t, err)
	require.False(t, resp.Ok)
}
