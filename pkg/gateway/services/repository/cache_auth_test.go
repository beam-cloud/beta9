package repository_services

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestAuthorizeCacheRepositoryRequestWithConfiguredToken(t *testing.T) {
	service := &WorkerRepositoryService{cacheCoordinatorToken: "cache-token"}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer cache-token"))

	if err := service.authorizeCacheRepositoryRequest(ctx); err != nil {
		t.Fatalf("authorizeCacheRepositoryRequest failed: %v", err)
	}
}

func TestAuthorizeCacheRepositoryRequestRejectsMissingOrWrongToken(t *testing.T) {
	service := &WorkerRepositoryService{cacheCoordinatorToken: "cache-token"}

	for _, ctx := range []context.Context{
		context.Background(),
		metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer wrong-token")),
	} {
		if err := service.authorizeCacheRepositoryRequest(ctx); err == nil {
			t.Fatal("authorizeCacheRepositoryRequest succeeded, want error")
		}
	}
}

func TestConfiguredCacheCoordinatorTokenUsesEnvOverride(t *testing.T) {
	t.Setenv(cacheCoordinatorTokenEnv, "env-cache-token")

	if got := configuredCacheCoordinatorToken("config-cache-token"); got != "env-cache-token" {
		t.Fatalf("configuredCacheCoordinatorToken() = %q, want env-cache-token", got)
	}
}
