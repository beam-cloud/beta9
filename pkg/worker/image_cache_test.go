package worker

import (
	"errors"
	"testing"

	"github.com/beam-cloud/beta9/pkg/cache"
	clipStorage "github.com/beam-cloud/clip/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestImageContentCacheErrorClassifiesMissAndUnavailable(t *testing.T) {
	require.ErrorIs(t, imageContentCacheError(cache.ErrContentNotFound), clipStorage.ErrContentCacheMiss)
	require.ErrorIs(t, imageContentCacheError(cache.ErrSelectedHostUnavailable), clipStorage.ErrContentCacheUnavailable)
	require.ErrorIs(t, imageContentCacheError(cache.ErrUnableToReachHost), clipStorage.ErrContentCacheUnavailable)

	other := errors.New("other")
	require.Same(t, other, imageContentCacheError(other))
}
