package scheduler

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/tj/assert"
)

func TestSchedulerCredentialCacheSingleflightsConcurrentLoads(t *testing.T) {
	cache := newSchedulerCredentialCache()

	var loads int32
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			value, _, err := cache.getOrLoad("credential", time.Hour, func() (cachedSchedulerCredential, error) {
				atomic.AddInt32(&loads, 1)
				time.Sleep(20 * time.Millisecond)
				return cachedSchedulerCredential{value: "token", exists: true}, nil
			})
			assert.Nil(t, err)
			assert.True(t, value.exists)
			assert.Equal(t, "token", value.value)
		}()
	}
	wg.Wait()

	assert.Equal(t, int32(1), atomic.LoadInt32(&loads))
}

func TestSchedulerCredentialCacheDoesNotCacheErrors(t *testing.T) {
	cache := newSchedulerCredentialCache()

	var loads int32
	_, _, err := cache.getOrLoad("credential", time.Hour, func() (cachedSchedulerCredential, error) {
		atomic.AddInt32(&loads, 1)
		return cachedSchedulerCredential{}, errors.New("temporary failure")
	})
	assert.NotNil(t, err)

	value, cacheHit, err := cache.getOrLoad("credential", time.Hour, func() (cachedSchedulerCredential, error) {
		atomic.AddInt32(&loads, 1)
		return cachedSchedulerCredential{value: "token", exists: true}, nil
	})
	assert.Nil(t, err)
	assert.False(t, cacheHit)
	assert.True(t, value.exists)
	assert.Equal(t, "token", value.value)
	assert.Equal(t, int32(2), atomic.LoadInt32(&loads))
}

func TestBuildRegistryCredentialCacheKeyIncludesCredentialDigest(t *testing.T) {
	first := types.BuildRegistryCredentialsConfig{
		Type: "basic",
		Credentials: map[string]string{
			"USERNAME": "user",
			"PASSWORD": "one",
		},
	}
	second := types.BuildRegistryCredentialsConfig{
		Type: "basic",
		Credentials: map[string]string{
			"USERNAME": "user",
			"PASSWORD": "two",
		},
	}

	firstKey := buildRegistryCredentialCacheKey("example.com", "repo", first)
	secondKey := buildRegistryCredentialCacheKey("example.com", "repo", second)
	assert.NotEqual(t, firstKey, secondKey)
}
