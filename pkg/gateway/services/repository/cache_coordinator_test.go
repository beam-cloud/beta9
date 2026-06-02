package repository_services

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc/metadata"
)

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
	t.Setenv(cacheCoordinatorTokenEnv, "env-coordinator-token")

	if got := configuredCacheCoordinatorToken("config-coordinator-token"); got != "env-coordinator-token" {
		t.Fatalf("configuredCacheCoordinatorToken() = %q, want env-coordinator-token", got)
	}
}

func TestResolveRequiredContentOriginLoadsWorkspaceStorage(t *testing.T) {
	storageID := uint(2)
	storage := &types.WorkspaceStorage{
		Id:          &storageID,
		BucketName:  ptr("workspace-bucket"),
		Region:      ptr("us-east-1"),
		EndpointUrl: ptr("http://localstack:4566"),
		AccessKey:   ptr("access-key"),
		SecretKey:   ptr("secret-key"),
	}
	service := &WorkerRepositoryService{
		backendRepo:           &requiredContentOriginBackendRepo{storageID: storageID, storage: storage},
		cacheCoordinatorToken: "coordinator-token",
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer coordinator-token"))

	resp, err := service.ResolveRequiredContentOrigin(ctx, &pb.ResolveRequiredContentOriginRequest{
		Item: &pb.RequiredContentItem{
			Locality:     "default",
			WorkspaceId:  "workspace-external-id",
			StubId:       "stub-id",
			Kind:         string(cache.RequiredContentKindVolume),
			Hash:         "hash-a",
			RoutingKey:   "volumes/path/file.bin",
			ExpectedHash: "hash-a",
			Source: &pb.RequiredContentSource{
				Type:       string(cache.RequiredContentSourceS3),
				ObjectPath: "volumes/path/file.bin",
			},
		},
	})
	if err != nil {
		t.Fatalf("ResolveRequiredContentOrigin returned error: %v", err)
	}
	if !resp.Ok || !resp.OriginAvailable {
		t.Fatalf("ResolveRequiredContentOrigin unavailable: ok=%t available=%t error=%q", resp.Ok, resp.OriginAvailable, resp.ErrorMsg)
	}
	if resp.Origin.BucketName != "workspace-bucket" || resp.Origin.AccessKey != "access-key" || resp.Origin.SecretKey != "secret-key" {
		t.Fatalf("origin storage fields were not resolved: %+v", resp.Origin)
	}
	if resp.Origin.Path != "volumes/path/file.bin" || resp.Origin.CachePath != "volumes/path/file.bin" || resp.Origin.ExpectedHash != "hash-a" {
		t.Fatalf("origin content fields were not resolved: %+v", resp.Origin)
	}
	if !resp.Origin.ForcePathStyle {
		t.Fatalf("origin should use path-style addressing for explicit endpoint: %+v", resp.Origin)
	}
}

func TestResolveRequiredContentOriginReturnsOCIOrigin(t *testing.T) {
	service := &WorkerRepositoryService{cacheCoordinatorToken: "coordinator-token"}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer coordinator-token"))

	resp, err := service.ResolveRequiredContentOrigin(ctx, &pb.ResolveRequiredContentOriginRequest{
		Item: &pb.RequiredContentItem{
			Locality:     "default",
			WorkspaceId:  "workspace-external-id",
			StubId:       "stub-id",
			Kind:         string(cache.RequiredContentKindClipOCI),
			Hash:         "decompressed-hash",
			RoutingKey:   "decompressed-hash",
			ExpectedHash: "decompressed-hash",
			Source: &pb.RequiredContentSource{
				Type:        string(cache.RequiredContentSourceOCIRegistry),
				Registry:    "registry.localhost:5000",
				Repository:  "stage/beta9-users",
				Reference:   "sha256:manifest",
				LayerDigest: "sha256:layer",
			},
		},
	})
	if err != nil {
		t.Fatalf("ResolveRequiredContentOrigin returned error: %v", err)
	}
	if !resp.Ok || !resp.OriginAvailable {
		t.Fatalf("ResolveRequiredContentOrigin unavailable: ok=%t available=%t error=%q", resp.Ok, resp.OriginAvailable, resp.ErrorMsg)
	}
	source, ok := cache.ParseOCIRequiredContentOriginPath(resp.Origin.Path)
	if !ok {
		t.Fatalf("origin path is not an OCI origin: %s", resp.Origin.Path)
	}
	if source.Registry != "registry.localhost:5000" || source.Repository != "stage/beta9-users" || source.Reference != "sha256:manifest" || source.LayerDigest != "sha256:layer" {
		t.Fatalf("unexpected OCI origin source: %+v", source)
	}
	if resp.Origin.ExpectedHash != "decompressed-hash" || resp.Origin.CachePath != "decompressed-hash" {
		t.Fatalf("origin content fields were not resolved: %+v", resp.Origin)
	}
}

func cacheRepositoryAuthContext(tokenType string) context.Context {
	return auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Token: &types.Token{TokenType: tokenType},
	})
}

type requiredContentOriginBackendRepo struct {
	repository.BackendRepository
	storageID uint
	storage   *types.WorkspaceStorage
}

func (r *requiredContentOriginBackendRepo) GetWorkspaceByExternalId(ctx context.Context, externalID string) (types.Workspace, error) {
	return types.Workspace{
		Id:         3,
		ExternalId: externalID,
		StorageId:  &r.storageID,
	}, nil
}

func (r *requiredContentOriginBackendRepo) GetWorkspace(ctx context.Context, workspaceID uint) (*types.Workspace, error) {
	return &types.Workspace{
		Id:      workspaceID,
		Storage: r.storage,
	}, nil
}

func ptr(value string) *string {
	return &value
}
