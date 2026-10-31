package auth

import (
	"context"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"strings"
	"time"
)

func (ai *AuthInterceptor) guardStream(ctx context.Context, credentials ...*types.Token) (context.Context, func(), func() error) {
	md, _ := metadata.FromIncomingContext(ctx)
	key := ""
	if v := md.Get("authorization"); len(v) > 0 {
		key = strings.TrimPrefix(v[0], "Bearer ")
	}
	credential := &types.Token{Key: key}
	if len(credentials) > 0 {
		credential = credentials[0]
	}
	return guardCredential(ctx, ai.backendRepo, credential)
}

func guardCredential(ctx context.Context, backend repository.BackendRepository, credential *types.Token) (context.Context, func(), func() error) {
	ctx, cancel := context.WithCancel(ctx)
	check := func() error {
		if backend == nil || credential == nil || credential.Key == "" {
			return nil
		}
		if validator, ok := backend.(interface {
			TokenIsActive(context.Context, string, string) (bool, error)
		}); ok && credential.ExternalId != "" {
			active, err := validator.TokenIsActive(ctx, credential.ExternalId, credential.Key)
			if err != nil || !active {
				cancel()
				return status.Error(codes.Unauthenticated, "credential is no longer active")
			}
			return nil
		}
		token, _, err := backend.AuthorizeToken(ctx, credential.Key)
		if err != nil || token == nil || !token.ValidAt(time.Now()) {
			cancel()
			return status.Error(codes.Unauthenticated, "credential is no longer active")
		}
		return nil
	}
	if backend != nil && credential != nil && credential.Key != "" {
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if check() != nil {
						return
					}
				}
			}
		}()
	}
	return ctx, cancel, check
}

// LiveCredentialContext cancels long-running transfers after revocation.
func LiveCredentialContext(ctx context.Context, backend repository.BackendRepository, credential *types.Token) (context.Context, func()) {
	live, cancel, _ := guardCredential(ctx, backend, credential)
	return live, cancel
}
