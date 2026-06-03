package worker

import (
	"errors"
	"testing"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/types"
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

func TestClipV2DoesNotUseEmbeddedImageArchiveCache(t *testing.T) {
	client := &ImageClient{}
	client.config.ImageService.ClipVersion = uint32(types.ClipVersion2)
	require.False(t, client.shouldUseEmbeddedImageArchiveCache())

	client.config.ImageService.ClipVersion = uint32(types.ClipVersion1)
	require.True(t, client.shouldUseEmbeddedImageArchiveCache())
}
