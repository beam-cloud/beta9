package worker

import (
	"errors"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/types"
	clipCommon "github.com/beam-cloud/clip/pkg/common"
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

func TestClipReadAggregateTreatsContentCacheMissAsExpected(t *testing.T) {
	tests := []struct {
		name             string
		result           string
		wantSuccess      bool
		wantErrorCount   int64
		wantFirstError   string
		wantRollupErrors int64
	}{
		{name: "miss", result: imageContentCacheResultMiss, wantSuccess: true},
		{name: "unavailable", result: imageContentCacheResultUnavailable, wantErrorCount: 1, wantFirstError: "cache unavailable", wantRollupErrors: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			aggregate := newClipReadAggregate(&types.ContainerRequest{})
			aggregate.add(clipCommon.ReadTraceEvent{
				Operation: string(types.ContainerLifecycleClipContentCacheRead),
				Success:   false,
				Error:     "cache unavailable",
				Attrs:     map[string]string{"cache_result": tt.result},
			})

			require.Equal(t, tt.wantSuccess, aggregate.success)
			require.Equal(t, tt.wantErrorCount, aggregate.errorCount)
			require.Equal(t, tt.wantFirstError, aggregate.firstError)
			require.Equal(t, tt.wantRollupErrors, aggregate.byOperation[string(types.ContainerLifecycleClipContentCacheRead)].ErrorCount)
		})
	}
}

func TestClipReadAggregateDoesNotCountContentCacheMissAsError(t *testing.T) {
	aggregate := newClipReadAggregate(&types.ContainerRequest{})
	aggregate.addContentCache(imageContentCacheTrace{
		Operation: imageContentCacheOperationReadInto,
		Result:    imageContentCacheResultMiss,
		Error:     "content cache miss: content not found",
		Trace: cache.OperationTrace{Attempts: []cache.OperationTraceAttempt{{
			HostID: "cache-host",
			Source: "remote",
			Result: imageContentCacheResultMiss,
			Error:  "content cache miss: content not found",
		}}},
	})

	require.EqualValues(t, 1, aggregate.cacheMissCount)
	require.Zero(t, aggregate.cacheErrorCount)
	require.Empty(t, aggregate.firstError)
	require.Zero(t, aggregate.byCacheOperation[clipReadRollupKey(imageContentCacheOperationReadInto, imageContentCacheResultMiss)].ErrorCount)
	require.Zero(t, aggregate.byCacheSource[clipReadRollupKey(imageContentCacheOperationReadInto, "remote", imageContentCacheResultMiss, "")].ErrorCount)
	require.Zero(t, aggregate.byCacheHost[clipReadRollupKey("cache-host", "remote", imageContentCacheResultMiss, "")].ErrorCount)
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
