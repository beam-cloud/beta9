package worker

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
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

type fakeImageCredentialWorkerRepo struct {
	pb.WorkerRepositoryServiceClient
	resp     *pb.GetCacheOriginCredentialsResponse
	requests []*pb.GetCacheOriginCredentialsRequest
}

func (f *fakeImageCredentialWorkerRepo) GetCacheOriginCredentials(ctx context.Context, req *pb.GetCacheOriginCredentialsRequest, opts ...grpc.CallOption) (*pb.GetCacheOriginCredentialsResponse, error) {
	f.requests = append(f.requests, req)
	return f.resp, nil
}
