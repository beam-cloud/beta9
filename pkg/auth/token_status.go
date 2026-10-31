package auth

import (
	"context"
	"fmt"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

// Cached identity data never substitutes for the current credential status.
func validateCachedToken(ctx context.Context, backend repository.BackendRepository, token *types.Token) error {
	status, ok := backend.(interface {
		TokenIsActive(context.Context, string, string) (bool, error)
	})
	if !ok {
		return nil
	}
	active, err := status.TokenIsActive(ctx, token.ExternalId, token.Key)
	if err != nil {
		return err
	}
	if !active {
		return fmt.Errorf("inactive credential")
	}
	return nil
}
