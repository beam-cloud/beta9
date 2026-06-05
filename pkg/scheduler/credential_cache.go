package scheduler

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"golang.org/x/sync/singleflight"
)

const (
	schedulerBuildRegistryCredentialTTL = 10 * time.Hour
	schedulerImageCredentialTTL         = 5 * time.Minute
)

type schedulerCredentialCache struct {
	mu      sync.RWMutex
	entries map[string]cachedSchedulerCredential
	group   singleflight.Group
}

type cachedSchedulerCredential struct {
	value     string
	source    string
	exists    bool
	expiresAt time.Time
}

func newSchedulerCredentialCache() *schedulerCredentialCache {
	return &schedulerCredentialCache{
		entries: map[string]cachedSchedulerCredential{},
	}
}

func (c *schedulerCredentialCache) getOrLoad(key string, ttl time.Duration, load func() (cachedSchedulerCredential, error)) (cachedSchedulerCredential, bool, error) {
	if c == nil {
		value, err := load()
		return value, false, err
	}

	if value, ok := c.get(key); ok {
		return value, true, nil
	}

	loaded, err, shared := c.group.Do(key, func() (interface{}, error) {
		if value, ok := c.get(key); ok {
			return value, nil
		}

		value, err := load()
		if err != nil {
			return cachedSchedulerCredential{}, err
		}

		value.expiresAt = time.Now().Add(ttl)
		c.set(key, value)
		return value, nil
	})
	if err != nil {
		return cachedSchedulerCredential{}, false, err
	}

	value, ok := loaded.(cachedSchedulerCredential)
	if !ok {
		return cachedSchedulerCredential{}, false, fmt.Errorf("unexpected scheduler credential cache value: %T", loaded)
	}

	return value, shared, nil
}

func (c *schedulerCredentialCache) get(key string) (cachedSchedulerCredential, bool) {
	c.mu.RLock()
	value, ok := c.entries[key]
	c.mu.RUnlock()

	if !ok {
		return cachedSchedulerCredential{}, false
	}
	if time.Now().After(value.expiresAt) {
		c.mu.Lock()
		if current, exists := c.entries[key]; exists && time.Now().After(current.expiresAt) {
			delete(c.entries, key)
		}
		c.mu.Unlock()
		return cachedSchedulerCredential{}, false
	}

	return value, true
}

func (c *schedulerCredentialCache) set(key string, value cachedSchedulerCredential) {
	c.mu.Lock()
	c.entries[key] = value
	c.mu.Unlock()
}

func imageCredentialCacheKey(workspaceID, imageID string) string {
	return fmt.Sprintf("image:%s:%s", workspaceID, imageID)
}

func buildRegistryCredentialCacheKey(registry, repository string, credentials types.BuildRegistryCredentialsConfig) string {
	return fmt.Sprintf("build:%s:%s:%s:%s", registry, repository, credentials.Type, stableCredentialDigest(credentials.Credentials))
}

func stableCredentialDigest(credentials map[string]string) string {
	if len(credentials) == 0 {
		return "empty"
	}

	bytes, err := json.Marshal(credentials)
	if err != nil {
		return "invalid"
	}

	sum := sha256.Sum256(bytes)
	return hex.EncodeToString(sum[:])
}
