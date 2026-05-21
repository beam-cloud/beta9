package cache

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWithStoreFromContentLockReturnsUnlockErrorAndRetriesDeferredRelease(t *testing.T) {
	registry := &failFirstUnlockRegistry{MockRegistry: NewMockRegistry()}
	client := &Client{
		ctx:         context.Background(),
		locality:    "test",
		coordinator: registry,
	}

	hash, err := client.withStoreFromContentLock(context.Background(), "/source", true, func() (string, error) {
		return "hash", nil
	})

	require.Equal(t, "hash", hash)
	require.ErrorContains(t, err, "unlock failed")
	require.Equal(t, 2, registry.removeCalls)
	require.False(t, registry.locks["store-lock:test:/source"])
}

type failFirstUnlockRegistry struct {
	*MockRegistry
	removeCalls int
}

func (r *failFirstUnlockRegistry) RemoveStoreFromContentLock(ctx context.Context, locality string, sourcePath string) error {
	r.removeCalls++
	if r.removeCalls == 1 {
		return errors.New("unlock failed")
	}
	return r.MockRegistry.RemoveStoreFromContentLock(ctx, locality, sourcePath)
}
