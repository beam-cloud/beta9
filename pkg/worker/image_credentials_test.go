package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	reg "github.com/beam-cloud/beta9/pkg/registry"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	clipCommon "github.com/beam-cloud/clip/pkg/common"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestLockImageArchiveFileRespectsContextCancellation(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "image.clip")
	unlock, err := lockImageArchiveFile(context.Background(), archivePath)
	require.NoError(t, err)
	defer unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err = lockImageArchiveFile(ctx, archivePath)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), 500*time.Millisecond)
}

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

// A source registry without a bucket (e.g. after a brokered URL pull, or an
// unconfigured private-pool worker) must not override the archive's own storage
// info with an empty S3 source that would fail every lazy data read.
func TestLazyMountOptionsForClipV1IgnoresEmptySourceRegistry(t *testing.T) {
	client := &ImageClient{
		imageCachePath: "/images/cache",
		config: types.AppConfig{
			ImageService: types.ImageServiceConfig{RegistryStore: reg.S3ImageRegistryStore},
		},
	}
	request := &types.ContainerRequest{ImageId: "image-a"}

	opts := client.lazyMountOptions(context.Background(), request, lazyImageArchive{
		path:           "/images/cache/image-a.rclip",
		sourceRegistry: &types.S3ImageRegistryConfig{},
		storageMode:    "s3",
	})

	require.Nil(t, opts.StorageInfo)
	require.Nil(t, opts.Credentials.S3)
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
	require.True(t, opts.UseCheckpoints)
}

func TestLazyMountOptionsForPrivateClipV2UsesCheckpoints(t *testing.T) {
	repo := &fakeImageCredentialWorkerRepo{
		resp: &pb.GetCacheOriginCredentialsResponse{
			Ok:                  true,
			RegistryCredentials: "registry-user:registry-pass",
		},
	}
	client := &ImageClient{
		imageCachePath:   "/images/cache",
		workerRepoClient: repo,
		originCredsCache: make(map[string]*originCredentials),
		v2ImageRefs:      common.NewSafeMap[string](),
		config: types.AppConfig{
			Worker: types.WorkerConfig{
				Pools: map[string]types.WorkerPoolConfig{
					"aws-cpu": {Mode: types.PoolModePrivate},
				},
			},
		},
	}
	client.v2ImageRefs.Set("image-a", "registry.example.com/team/image:tag")
	request := &types.ContainerRequest{
		WorkspaceId:  "workspace-id",
		StubId:       "stub-id",
		ImageId:      "image-a",
		PoolSelector: "aws-cpu",
	}

	opts := client.lazyMountOptions(context.Background(), request, lazyImageArchive{
		path:        "/images/cache/image-a.rclip",
		storageMode: "oci",
	})

	require.NotNil(t, opts.RegistryCredProvider)
	require.True(t, opts.UseCheckpoints)
}

func TestGetCredentialProviderForAgentPoolImageUsesGatewayCredentialsOnly(t *testing.T) {
	for _, mode := range []types.PoolMode{types.PoolModePrivate, types.PoolModeMarketplace} {
		t.Run(string(mode), func(t *testing.T) {
			repo := &fakeImageCredentialWorkerRepo{
				resp: &pb.GetCacheOriginCredentialsResponse{
					Ok:                  true,
					RegistryCredentials: "gateway-user:gateway-pass",
				},
			}
			client := agentPoolImageClient(mode, repo)
			client.v2ImageRefs.Set("image-a", "registry.example.com/team/image:tag")
			request := &types.ContainerRequest{
				WorkspaceId: "workspace-id",
				StubId:      "stub-id",
				ImageId:     "image-a",
				// Agent-pool requests should never trust credentials embedded in
				// the scheduler payload, even if a malformed caller supplies them.
				ImageCredentials: "embedded-user:embedded-pass",
				BuildOptions: types.BuildOptions{
					SourceImageCreds: "source-user:source-pass",
				},
				PoolSelector: "agent-pool",
			}

			provider := client.getCredentialProviderForImage(context.Background(), "image-a", request)
			require.NotNil(t, provider)
			cfg, err := provider.GetCredentials(context.Background(), "registry.example.com", "team/image")
			require.NoError(t, err)
			require.NotNil(t, cfg)
			require.Equal(t, "gateway-user", cfg.Username)
			require.Equal(t, "gateway-pass", cfg.Password)
			require.Len(t, repo.requests, 1)
			require.Equal(t, "image-a", repo.requests[0].ImageId)
		})
	}
}

func TestGetCredentialProviderForAgentPoolImageAvoidsAmbientKeychainWithoutGatewayCredentials(t *testing.T) {
	for _, mode := range []types.PoolMode{types.PoolModePrivate, types.PoolModeMarketplace} {
		t.Run(string(mode), func(t *testing.T) {
			repo := &fakeImageCredentialWorkerRepo{
				resp: &pb.GetCacheOriginCredentialsResponse{Ok: true},
			}
			client := agentPoolImageClient(mode, repo)
			client.v2ImageRefs.Set("image-a", "registry.example.com/team/image:tag")
			request := &types.ContainerRequest{
				WorkspaceId:  "workspace-id",
				StubId:       "stub-id",
				ImageId:      "image-a",
				PoolSelector: "agent-pool",
			}

			provider := client.getCredentialProviderForImage(context.Background(), "image-a", request)
			require.NotNil(t, provider)
			require.Equal(t, "private-worker-anonymous", provider.Name())
			cfg, err := provider.GetCredentials(context.Background(), "registry.example.com", "team/image")
			require.NoError(t, err)
			require.NotNil(t, cfg)
			require.Empty(t, cfg.Username)
			require.Empty(t, cfg.Password)
			require.Len(t, repo.requests, 1)
		})
	}
}

func TestPullImageArchiveFromBrokeredOriginUsesURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		_, _ = w.Write([]byte("archive"))
	}))
	defer server.Close()

	repo := &fakeImageCredentialWorkerRepo{
		resp: &pb.GetCacheOriginCredentialsResponse{
			Ok:              true,
			ImageArchiveUrl: server.URL + "/image.rclip",
		},
	}
	client := &ImageClient{
		workerRepoClient: repo,
		originCredsCache: make(map[string]*originCredentials),
	}
	path := filepath.Join(t.TempDir(), "image.rclip.tmp")
	request := &types.ContainerRequest{
		WorkspaceId: "workspace-id",
		StubId:      "stub-id",
		ImageId:     "image-a",
	}

	pulled, sourceRegistry, err := client.pullImageArchiveFromBrokeredOrigin(context.Background(), path, request)

	require.NoError(t, err)
	require.True(t, pulled)
	// URL pulls vend no S3 credentials, so no source registry is returned;
	// returning an empty config here would poison v1 lazy mounts.
	require.Nil(t, sourceRegistry)
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte("archive"), got)
	require.Len(t, repo.requests, 1)
	require.Equal(t, "workspace-id", repo.requests[0].WorkspaceId)
	require.Equal(t, "image-a", repo.requests[0].ImageId)
}

func TestPullImageFromRegistryAgentPoolsRequireBrokeredOrigin(t *testing.T) {
	for _, mode := range []types.PoolMode{types.PoolModePrivate, types.PoolModeMarketplace} {
		t.Run(string(mode), func(t *testing.T) {
			repo := &fakeImageCredentialWorkerRepo{
				resp: &pb.GetCacheOriginCredentialsResponse{Ok: true},
			}
			client := agentPoolImageClient(mode, repo)
			archivePath := filepath.Join(t.TempDir(), "image-a.rclip")
			request := &types.ContainerRequest{
				WorkspaceId:  "workspace-id",
				StubId:       "stub-id",
				ImageId:      "image-a",
				PoolSelector: "agent-pool",
			}

			_, err := client.pullImageFromRegistry(context.Background(), archivePath, request)

			require.Error(t, err)
			require.Contains(t, err.Error(), "gateway-brokered image archive origin is unavailable")
			require.Len(t, repo.requests, 1)
			require.Equal(t, "image-a", repo.requests[0].ImageId)
		})
	}
}

func TestPullImageFromRegistryAgentPoolsUseWorkerPoolWhenRequestSelectorIsEmpty(t *testing.T) {
	repo := &fakeImageCredentialWorkerRepo{
		resp: &pb.GetCacheOriginCredentialsResponse{Ok: true},
	}
	client := agentPoolImageClient(types.PoolModeMarketplace, repo)
	archivePath := filepath.Join(t.TempDir(), "image-a.rclip")
	request := &types.ContainerRequest{
		WorkspaceId: "workspace-id",
		StubId:      "stub-id",
		ImageId:     "image-a",
	}

	_, err := client.pullImageFromRegistry(context.Background(), archivePath, request)

	require.Error(t, err)
	require.Contains(t, err.Error(), "gateway-brokered image archive origin is unavailable")
	require.Len(t, repo.requests, 1)
	require.Equal(t, "image-a", repo.requests[0].ImageId)
}

func agentPoolImageClient(mode types.PoolMode, repo *fakeImageCredentialWorkerRepo) *ImageClient {
	return &ImageClient{
		workerRepoClient: repo,
		workerPoolName:   "agent-pool",
		originCredsCache: make(map[string]*originCredentials),
		v2ImageRefs:      common.NewSafeMap[string](),
		config: types.AppConfig{
			Worker: types.WorkerConfig{
				Pools: map[string]types.WorkerPoolConfig{
					"agent-pool": {Mode: mode},
				},
			},
		},
	}
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
