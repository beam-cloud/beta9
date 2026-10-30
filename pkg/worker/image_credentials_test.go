package worker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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

func TestBuildahAuthArgsParsesEveryCredentialForm(t *testing.T) {
	client := &ImageClient{}
	for name, creds := range map[string]string{
		"plain":           "user:pa:ss",
		"json":            `{"USERNAME":"user","PASSWORD":"pa:ss"}`,
		"json-whitespace": "  \n" + `{"USERNAME":"user","PASSWORD":"pa:ss"}` + "\n",
		"dockerhub-json":  `{"registry":"docker.io","type":"basic","credentials":{"DOCKERHUB_USERNAME":"user","DOCKERHUB_PASSWORD":"pa:ss"}}`,
	} {
		request := &types.ContainerRequest{BuildOptions: types.BuildOptions{SourceImageCreds: creds}}
		require.Equal(t, []string{"--creds", "user:pa:ss"}, client.buildahAuthArgs(context.Background(), request, "registry.example.com/img"), name)
	}
	require.Nil(t, client.getBuildahCredentials("registry.example.com/img", ""))
	require.Nil(t, client.getBuildahCredentials("registry.example.com/img", `{"AWS_ACCESS_KEY_ID":"a","AWS_SECRET_ACCESS_KEY":"b"}`))
}

func TestBuildahAuthFileScopesCredentialsToSourceRegistry(t *testing.T) {
	for _, sourceImage := range []string{"docker.io/library/node:24", "node:24", "team/node:24"} {
		t.Run(sourceImage, func(t *testing.T) {
			root := t.TempDir()
			runroot := filepath.Join(root, "run")
			buildContext := filepath.Join(root, "context")
			require.NoError(t, os.Mkdir(runroot, 0700))
			require.NoError(t, os.Mkdir(buildContext, 0700))
			dockerfile := "FROM " + sourceImage + " AS node\nFROM public.ecr.aws/n4e0e1y0/beta9-microvm:gpu\nCOPY --from=node /usr/local/bin/node /usr/local/bin/node\n"
			require.NoError(t, os.WriteFile(filepath.Join(buildContext, "Dockerfile"), []byte(dockerfile), 0600))
			request := &types.ContainerRequest{BuildOptions: types.BuildOptions{
				SourceImage: &sourceImage, SourceImageCreds: "user:pa:ss", Dockerfile: &dockerfile,
			}}

			args, err := (&ImageClient{}).buildahAuthFileArgs(context.Background(), request, sourceImage, runroot)
			require.NoError(t, err)
			require.Equal(t, []string{"--authfile", filepath.Join(runroot, "auth.json")}, args)
			require.NotContains(t, args, "--creds")
			contents, err := os.ReadFile(args[1])
			require.NoError(t, err)
			var config struct {
				Auths map[string]struct {
					Auth string `json:"auth"`
				} `json:"auths"`
			}
			require.NoError(t, json.Unmarshal(contents, &config))
			require.Len(t, config.Auths, 1)
			decoded, err := base64.StdEncoding.DecodeString(config.Auths["docker.io"].Auth)
			require.NoError(t, err)
			require.Equal(t, "user:pa:ss", string(decoded))
			require.NotContains(t, config.Auths, "public.ecr.aws")
			info, err := os.Stat(args[1])
			require.NoError(t, err)
			require.Equal(t, os.FileMode(0600), info.Mode().Perm())
			entries, err := os.ReadDir(buildContext)
			require.NoError(t, err)
			require.Len(t, entries, 1)
			require.Equal(t, "Dockerfile", entries[0].Name())
		})
	}
}

func TestBuildahAuthFileOutsideDefaultContext(t *testing.T) {
	client := &ImageClient{}
	request := &types.ContainerRequest{BuildOptions: types.BuildOptions{SourceImageCreds: "user:secret"}}
	runroot := mustMkdirTempBuildahDir("buildah-test-run-")
	defer os.RemoveAll(runroot)
	buildContext, err := client.getBuildContext(context.Background(), t.TempDir(), request)
	require.NoError(t, err)
	require.Equal(t, ".", buildContext)
	args, err := client.buildahAuthFileArgs(context.Background(), request, "node:24", runroot)
	require.NoError(t, err)
	contextPath, err := filepath.Abs(buildContext)
	require.NoError(t, err)
	relative, err := filepath.Rel(contextPath, args[1])
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(relative, ".."+string(filepath.Separator)), "authfile must be outside COPY . context: %s", relative)
}

func TestBuildahAuthFileWithoutSourceCredentials(t *testing.T) {
	for _, sourceImage := range []string{"", "docker.io/library/node:24"} {
		args, err := (&ImageClient{}).buildahAuthFileArgs(context.Background(), &types.ContainerRequest{}, sourceImage, t.TempDir())
		require.NoError(t, err)
		contents, err := os.ReadFile(args[1])
		require.NoError(t, err)
		require.JSONEq(t, `{"auths":{}}`, string(contents))
	}
}

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

func TestGatewayCredentialProviderForImageFetchesScopedCredentialsOnFirstUse(t *testing.T) {
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

	provider := client.gatewayCredentialProviderForImage("image-a", "registry.example.com", request, nil)
	require.NotNil(t, provider)
	require.Empty(t, repo.requests, "building the provider must not call the gateway")

	cfg, err := provider.GetCredentials(context.Background(), "registry.example.com", "team/image")
	require.NoError(t, err)
	require.Equal(t, "user", cfg.Username)
	require.Len(t, repo.requests, 1)
	req := repo.requests[0]
	require.Equal(t, "workspace-id", req.WorkspaceId)
	require.Equal(t, "stub-id", req.StubId)
	require.Equal(t, "image-a", req.ImageId)
	require.Equal(t, "registry.example.com", req.Registry)

	_, err = provider.GetCredentials(context.Background(), "registry.example.com", "team/image")
	require.NoError(t, err)
	require.Len(t, repo.requests, 1, "credentials are cached for their TTL")

	client.originCredsMu.Lock()
	for _, cached := range client.originCredsCache {
		cached.fetchedAt = time.Now().Add(-originCredentialsTTL - time.Second)
	}
	client.originCredsMu.Unlock()
	repo.resp = &pb.GetCacheOriginCredentialsResponse{
		Ok:                  true,
		RegistryCredentials: "second-user:second-pass",
	}

	cfg, err = provider.GetCredentials(context.Background(), "registry.example.com", "team/image")
	require.NoError(t, err)
	require.Equal(t, "second-user", cfg.Username)
	require.Equal(t, "second-pass", cfg.Password)
	require.Len(t, repo.requests, 2)
}

func TestGatewayCredentialProviderFallsBackWhenGatewayHasNone(t *testing.T) {
	repo := &fakeImageCredentialWorkerRepo{resp: &pb.GetCacheOriginCredentialsResponse{Ok: true}}
	client := &ImageClient{workerRepoClient: repo, originCredsCache: make(map[string]*originCredentials)}
	request := &types.ContainerRequest{WorkspaceId: "workspace-id", StubId: "stub-id", ImageId: "image-a"}

	provider := client.gatewayCredentialProviderForImage("image-a", "registry.example.com", request, nil)
	_, err := provider.GetCredentials(context.Background(), "registry.example.com", "team/image")
	require.ErrorIs(t, err, clipCommon.ErrNoCredentials)

	provider = client.gatewayCredentialProviderForImage("image-a", "registry.example.com", request, privateWorkerAnonymousRegistryProvider{})
	cfg, err := provider.GetCredentials(context.Background(), "registry.example.com", "team/image")
	require.NoError(t, err)
	require.Empty(t, cfg.Username)
}

// Once the gateway stops vending credentials (revoked or expired), a refresh
// must drop the cached provider so the fallback answers instead of the stale
// credentials being reused.
func TestGatewayCredentialProviderDropsStaleCredentialsWhenRefreshHasNone(t *testing.T) {
	repo := &fakeImageCredentialWorkerRepo{resp: &pb.GetCacheOriginCredentialsResponse{Ok: true, RegistryCredentials: "user:pass"}}
	client := &ImageClient{workerRepoClient: repo, originCredsCache: make(map[string]*originCredentials)}
	request := &types.ContainerRequest{WorkspaceId: "workspace-id", StubId: "stub-id", ImageId: "image-a"}

	provider := client.gatewayCredentialProviderForImage("image-a", "registry.example.com", request, privateWorkerAnonymousRegistryProvider{})
	cfg, err := provider.GetCredentials(context.Background(), "registry.example.com", "team/image")
	require.NoError(t, err)
	require.Equal(t, "user", cfg.Username)

	expireCachedCredentials := func() {
		client.originCredsMu.Lock()
		for _, cached := range client.originCredsCache {
			cached.fetchedAt = time.Now().Add(-originCredentialsTTL - time.Second)
		}
		client.originCredsMu.Unlock()
	}

	expireCachedCredentials()
	repo.resp = &pb.GetCacheOriginCredentialsResponse{Ok: true}
	cfg, err = provider.GetCredentials(context.Background(), "registry.example.com", "team/image")
	require.NoError(t, err)
	require.Empty(t, cfg.Username, "the fallback answers once the gateway has no credentials")
	require.Len(t, repo.requests, 2)

	// A failed refresh is not an answer: the last known credentials stay.
	expireCachedCredentials()
	repo.resp = &pb.GetCacheOriginCredentialsResponse{Ok: true, RegistryCredentials: "user2:pass2"}
	cfg, err = provider.GetCredentials(context.Background(), "registry.example.com", "team/image")
	require.NoError(t, err)
	require.Equal(t, "user2", cfg.Username)
	expireCachedCredentials()
	repo.err = errFakeGatewayUnavailable
	cfg, err = provider.GetCredentials(context.Background(), "registry.example.com", "team/image")
	require.NoError(t, err)
	require.Equal(t, "user2", cfg.Username)

	// Without a fallback, revoked credentials surface as no credentials.
	repo.err = nil
	repo.resp = &pb.GetCacheOriginCredentialsResponse{Ok: true, RegistryCredentials: "user:pass"}
	bare := client.gatewayCredentialProviderForImage("image-b", "registry.example.com", &types.ContainerRequest{WorkspaceId: "workspace-id", StubId: "stub-id", ImageId: "image-b"}, nil)
	_, err = bare.GetCredentials(context.Background(), "registry.example.com", "team/image")
	require.NoError(t, err)
	expireCachedCredentials()
	repo.resp = &pb.GetCacheOriginCredentialsResponse{Ok: true}
	_, err = bare.GetCredentials(context.Background(), "registry.example.com", "team/image")
	require.ErrorIs(t, err, clipCommon.ErrNoCredentials)
}

func TestGatewayCredentialProviderPreventsAgentKeychainFallback(t *testing.T) {
	provider := &gatewayRegistryCredentialProvider{
		client:         &ImageClient{},
		provider:       clipCommon.NewPublicOnlyProvider(),
		preventAmbient: true,
	}

	cfg, err := provider.GetCredentials(context.Background(), "registry.example.com", "team/image")

	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.Empty(t, cfg.Username)
	require.Empty(t, cfg.Password)
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
		imageCachePath:   "/images/cache",
		workerRepoClient: repo,
		originCredsCache: make(map[string]*originCredentials),
		v2ImageRefs:      common.NewSafeMap[string](),
	}
	client.v2ImageRefs.Set("image-a", "registry.example.com/team/image:tag")
	request := &types.ContainerRequest{
		WorkspaceId: "workspace-id",
		StubId:      "stub-id",
		ImageId:     "image-a",
	}
	metadata := &clipCommon.ClipArchiveMetadata{}

	opts := client.lazyMountOptions(context.Background(), request, lazyImageArchive{
		path:        "/images/cache/image-a.rclip",
		storageMode: "oci",
		metadata:    metadata,
	})

	require.Equal(t, "/images/cache/image-a.rclip", opts.ArchivePath)
	require.Same(t, metadata, opts.Metadata)
	require.Equal(t, "/images/cache", opts.CachePath)
	require.Nil(t, opts.StorageInfo)
	require.NotNil(t, opts.RegistryCredProvider)
	require.Empty(t, repo.requests, "mounting must not wait on a credential round trip")
	require.True(t, opts.UseCheckpoints)

	provider, ok := opts.RegistryCredProvider.(clipCommon.RegistryCredentialProvider)
	require.True(t, ok)
	cfg, err := provider.GetCredentials(context.Background(), "registry.example.com", "team/image")
	require.NoError(t, err)
	require.Equal(t, "registry-user", cfg.Username)
	require.Len(t, repo.requests, 1)
	require.Equal(t, "workspace-id", repo.requests[0].WorkspaceId)
	require.Equal(t, "stub-id", repo.requests[0].StubId)
	require.Equal(t, "image-a", repo.requests[0].ImageId)
	require.Equal(t, "registry.example.com", repo.requests[0].Registry)
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
	for _, mode := range []types.PoolMode{types.PoolModePrivate, types.PoolModeProvider, types.PoolModeExternal} {
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
	for _, mode := range []types.PoolMode{types.PoolModePrivate, types.PoolModeProvider, types.PoolModeExternal} {
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
			require.Empty(t, repo.requests)
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
	for _, mode := range []types.PoolMode{types.PoolModePrivate, types.PoolModeProvider, types.PoolModeExternal} {
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
	client := agentPoolImageClient(types.PoolModeProvider, repo)
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

var errFakeGatewayUnavailable = errors.New("gateway unavailable")

type fakeImageCredentialWorkerRepo struct {
	pb.WorkerRepositoryServiceClient
	resp     *pb.GetCacheOriginCredentialsResponse
	err      error
	requests []*pb.GetCacheOriginCredentialsRequest
}

func (f *fakeImageCredentialWorkerRepo) GetCacheOriginCredentials(ctx context.Context, req *pb.GetCacheOriginCredentialsRequest, opts ...grpc.CallOption) (*pb.GetCacheOriginCredentialsResponse, error) {
	f.requests = append(f.requests, req)
	if f.err != nil {
		return nil, f.err
	}
	return f.resp, nil
}
