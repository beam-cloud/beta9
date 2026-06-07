package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	reg "github.com/beam-cloud/beta9/pkg/registry"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc"
)

func TestPullImageArchiveFromGateway(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/agent/images/image-a/image-a.clip" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer worker-token" {
			t.Fatalf("authorization = %q, want worker token", got)
		}
		_, _ = w.Write([]byte("clip-archive"))
	}))
	t.Cleanup(server.Close)
	t.Setenv(types.AgentGatewayURLEnv, server.URL)
	t.Setenv(types.WorkerTokenEnv, "worker-token")

	client := &ImageClient{
		registry: &reg.ImageRegistry{ImageFileExtension: reg.LocalImageFileExtension},
	}
	path := filepath.Join(t.TempDir(), "cache", "image-a.clip.tmp")

	if err := client.pullImageArchiveFromGateway(context.Background(), path, "image-a"); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "clip-archive" {
		t.Fatalf("archive contents = %q", data)
	}
}

func TestPullImageArchiveFromGatewayFallsBackToRemoteArchive(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/agent/images/image-a/image-a.clip":
			http.NotFound(w, r)
		case "/api/v1/agent/images/image-a/image-a.rclip":
			_, _ = w.Write([]byte("remote-clip-archive"))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)
	t.Setenv(types.AgentGatewayURLEnv, server.URL)
	t.Setenv(types.WorkerTokenEnv, "worker-token")

	client := &ImageClient{
		registry: &reg.ImageRegistry{ImageFileExtension: reg.LocalImageFileExtension},
	}
	path := filepath.Join(t.TempDir(), "cache", "image-a.clip.tmp")

	if err := client.pullImageArchiveFromGateway(context.Background(), path, "image-a"); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "remote-clip-archive" {
		t.Fatalf("archive contents = %q", data)
	}
}

func TestPullImageFromRegistryFailsClosedForAgentGatewayError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "gateway unavailable", http.StatusBadGateway)
	}))
	t.Cleanup(server.Close)
	t.Setenv(types.AgentGatewayURLEnv, server.URL)
	t.Setenv(types.WorkerTokenEnv, "worker-token")

	registry, err := reg.NewImageRegistry(types.AppConfig{}, types.S3ImageRegistryConfig{})
	if err != nil {
		t.Fatal(err)
	}
	client := &ImageClient{registry: registry}
	path := filepath.Join(t.TempDir(), "cache", "image-a.clip")

	_, err = client.pullImageFromRegistry(context.Background(), path, &types.ContainerRequest{ImageId: "image-a"})
	if err == nil {
		t.Fatal("expected gateway archive error")
	}
	if !strings.Contains(err.Error(), "502 Bad Gateway") {
		t.Fatalf("err = %v, want gateway status", err)
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("archive path exists after failed gateway pull: %v", statErr)
	}
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

type fakeImageCredentialWorkerRepo struct {
	pb.WorkerRepositoryServiceClient
	resp     *pb.GetCacheOriginCredentialsResponse
	requests []*pb.GetCacheOriginCredentialsRequest
}

func (f *fakeImageCredentialWorkerRepo) GetCacheOriginCredentials(ctx context.Context, req *pb.GetCacheOriginCredentialsRequest, opts ...grpc.CallOption) (*pb.GetCacheOriginCredentialsResponse, error) {
	f.requests = append(f.requests, req)
	return f.resp, nil
}
