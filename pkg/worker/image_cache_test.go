package worker

import (
	"errors"
	"testing"
	"time"

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

func TestImageContentCacheFinishStoreClassifiesResults(t *testing.T) {
	traces := []imageContentCacheTrace{}
	c := &imageContentCache{
		imageID: "image",
		kind:    "kind",
		observe: func(trace imageContentCacheTrace) {
			traces = append(traces, trace)
		},
	}

	c.finishStore(imageContentCacheStoreTrace{
		operation:  imageContentCacheOperationStoreStream,
		result:     imageContentCacheResultStoredOrPresent,
		hash:       "hash",
		actualHash: "hash",
		routingKey: "hash",
		startedAt:  time.Now(),
	})
	c.finishStore(imageContentCacheStoreTrace{
		operation:  imageContentCacheOperationStoreStream,
		result:     imageContentCacheResultSkippedUnavailable,
		hash:       "hash",
		actualHash: "hash",
		routingKey: "hash",
		startedAt:  time.Now(),
	})
	err := errors.New("store failed")
	c.finishStore(imageContentCacheStoreTrace{
		operation:  imageContentCacheOperationStoreStream,
		hash:       "hash",
		actualHash: "hash",
		routingKey: "hash",
		startedAt:  time.Now(),
		err:        err,
	})

	require.EqualValues(t, 1, c.storeSuccesses.Load())
	require.EqualValues(t, 1, c.storeSkipped.Load())
	require.EqualValues(t, 1, c.storeErrors.Load())
	require.Len(t, traces, 3)
	require.Equal(t, imageContentCacheResultStoredOrPresent, traces[0].Result)
	require.Equal(t, imageContentCacheResultSkippedUnavailable, traces[1].Result)
	require.Equal(t, imageContentCacheResultError, traces[2].Result)
	require.Contains(t, traces[2].Error, "store failed")
}
