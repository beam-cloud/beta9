package worker

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/common"
	reg "github.com/beam-cloud/beta9/pkg/registry"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	clipCommon "github.com/beam-cloud/clip/pkg/common"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestGatewayCredentialProviderForImageFetchesScopedCredentials(t *testing.T) {
	repo := &fakeImageCredentialWorkerRepo{
		resp: &pb.GetCacheOriginCredentialsResponse{
			Ok:                  true,
			RegistryCredentials: "user:pass",
		},
	}
	client := &ImageClient{
		workerRepoClient: repo,
		originCredsCache: make(map[string]*originCredentials),
	}
	request := &types.ContainerRequest{
		WorkspaceId: "workspace-id",
		StubId:      "stub-id",
		ImageId:     "image-a",
	}

	provider := client.gatewayCredentialProviderForImage(context.Background(), "image-a", "registry.example.com", request)
	if provider == nil {
		t.Fatal("expected gateway-vended credential provider")
	}
	if len(repo.requests) != 1 {
		t.Fatalf("credential requests = %d, want 1", len(repo.requests))
	}
	req := repo.requests[0]
	if req.WorkspaceId != "workspace-id" || req.StubId != "stub-id" || req.ImageId != "image-a" || req.Registry != "registry.example.com" {
		t.Fatalf("unexpected credential request: %+v", req)
	}

	if provider := client.gatewayCredentialProviderForImage(context.Background(), "image-a", "registry.example.com", request); provider == nil {
		t.Fatal("expected cached gateway-vended credential provider")
	}
	if len(repo.requests) != 1 {
		t.Fatalf("credential requests after cache hit = %d, want 1", len(repo.requests))
	}
}

func TestLazyMountOptionsForClipV1UsesBrokeredS3Storage(t *testing.T) {
	client := &ImageClient{
		imageCachePath: "/images/cache",
		config: types.AppConfig{
			ImageService: types.ImageServiceConfig{RegistryStore: reg.S3ImageRegistryStore},
		},
	}
	request := &types.ContainerRequest{ImageId: "image-a"}
	sourceRegistry := &types.S3ImageRegistryConfig{
		BucketName:     "image-bucket",
		Region:         "us-east-1",
		Endpoint:       "https://objects.example.com",
		AccessKey:      "access",
		SecretKey:      "secret",
		ForcePathStyle: true,
	}

	opts := client.lazyMountOptions(context.Background(), request, lazyImageArchive{
		path:           "/images/cache/image-a.rclip",
		sourceRegistry: sourceRegistry,
		storageMode:    "s3",
	})

	require.Equal(t, "/images/cache/image-a.rclip", opts.ArchivePath)
	require.Equal(t, "/images/cache/image-a.clip", opts.CachePath)
	require.Nil(t, opts.RegistryCredProvider)
	require.NotNil(t, opts.Credentials.S3)
	require.Equal(t, "access", opts.Credentials.S3.AccessKey)
	require.Equal(t, "secret", opts.Credentials.S3.SecretKey)

	storageInfo, ok := opts.StorageInfo.(*clipCommon.S3StorageInfo)
	require.True(t, ok)
	require.Equal(t, "image-bucket", storageInfo.Bucket)
	require.Equal(t, "us-east-1", storageInfo.Region)
	require.Equal(t, "https://objects.example.com", storageInfo.Endpoint)
	require.Equal(t, "image-a."+reg.LocalImageFileExtension, storageInfo.Key)
	require.True(t, storageInfo.ForcePathStyle)
}

func TestLazyMountOptionsForClipV2UsesGatewayRegistryCredentials(t *testing.T) {
	repo := &fakeImageCredentialWorkerRepo{
		resp: &pb.GetCacheOriginCredentialsResponse{
			Ok:                  true,
			RegistryCredentials: "registry-user:registry-pass",
		},
	}
	client := &ImageClient{
		imageCachePath:    "/images/cache",
		workerRepoClient:  repo,
		originCredsCache:  make(map[string]*originCredentials),
		v2ImageRefs:       common.NewSafeMap[string](),
		v2ArchiveMetadata: common.NewSafeMap[*clipCommon.ClipArchiveMetadata](),
	}
	client.v2ImageRefs.Set("image-a", "registry.example.com/team/image:tag")
	request := &types.ContainerRequest{
		WorkspaceId: "workspace-id",
		StubId:      "stub-id",
		ImageId:     "image-a",
	}

	opts := client.lazyMountOptions(context.Background(), request, lazyImageArchive{
		path:        "/images/cache/image-a.rclip",
		storageMode: "oci",
	})

	require.Equal(t, "/images/cache/image-a.rclip", opts.ArchivePath)
	require.Equal(t, "/images/cache", opts.CachePath)
	require.Nil(t, opts.StorageInfo)
	require.NotNil(t, opts.RegistryCredProvider)
	require.Len(t, repo.requests, 1)
	require.Equal(t, "workspace-id", repo.requests[0].WorkspaceId)
	require.Equal(t, "stub-id", repo.requests[0].StubId)
	require.Equal(t, "image-a", repo.requests[0].ImageId)
	require.Equal(t, "registry.example.com", repo.requests[0].Registry)
}

type fakeImageCredentialWorkerRepo struct {
	pb.WorkerRepositoryServiceClient
	resp     *pb.GetCacheOriginCredentialsResponse
	requests []*pb.GetCacheOriginCredentialsRequest
}

func (f *fakeImageCredentialWorkerRepo) GetCacheOriginCredentials(ctx context.Context, req *pb.GetCacheOriginCredentialsRequest, opts ...grpc.CallOption) (*pb.GetCacheOriginCredentialsResponse, error) {
	f.requests = append(f.requests, req)
	return f.resp, nil
}
