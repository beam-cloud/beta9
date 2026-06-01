package repository_services

import (
	"context"
	"crypto/subtle"
	"errors"
	"os"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	"google.golang.org/grpc/metadata"
)

const (
	cacheCoordinatorTokenEnv       = "CACHE_COORDINATOR_TOKEN"
	legacyCacheCoordinatorTokenEnv = "CACHE_SERVER_TOKEN"
)

var errCacheCoordinatorUnauthorized = errors.New("unauthorized cache coordinator request")

func configuredCacheCoordinatorToken(configured string) string {
	if token := os.Getenv(cacheCoordinatorTokenEnv); token != "" {
		return token
	}
	if token := os.Getenv(legacyCacheCoordinatorTokenEnv); token != "" {
		return token
	}
	return configured
}

func (s *WorkerRepositoryService) authorizeCacheRepositoryRequest(ctx context.Context) error {
	if _, ok := auth.AuthInfoFromContext(ctx); ok {
		return nil
	}
	if s == nil || s.cacheCoordinatorToken == "" {
		return errCacheCoordinatorUnauthorized
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok || len(md["authorization"]) == 0 {
		return errCacheCoordinatorUnauthorized
	}

	token := strings.TrimPrefix(md["authorization"][0], "Bearer ")
	if subtle.ConstantTimeCompare([]byte(token), []byte(s.cacheCoordinatorToken)) != 1 {
		return errCacheCoordinatorUnauthorized
	}

	return nil
}
