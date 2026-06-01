package cache

import (
	"context"
	"errors"
	"strings"

	"github.com/bsm/redislock"
)

func removeStoreFromContentLock(ctx context.Context, metadataStore CacheMetadataStore, locality, sourcePath, logPrefix string) error {
	err := metadataStore.RemoveStoreFromContentLock(ctx, locality, sourcePath)
	if err == nil {
		return nil
	}

	if isStoreFromContentLockNotHeld(err) {
		Logger.Debugf("%s[LOCK_RELEASED] - lock already gone [source=%s err=%v]", logPrefix, sourcePath, err)
		return nil
	}

	Logger.Errorf("%s[ERR] - error removing lock: %v", logPrefix, err)
	return err
}

func refreshStoreFromContentLock(ctx context.Context, metadataStore CacheMetadataStore, locality, sourcePath, logPrefix string) {
	err := metadataStore.RefreshStoreFromContentLock(ctx, locality, sourcePath)
	if err == nil || ctx.Err() != nil {
		return
	}

	if isStoreFromContentLockNotHeld(err) {
		Logger.Debugf("%s[LOCK_REFRESH_MISS] - lock no longer held [source=%s err=%v]", logPrefix, sourcePath, err)
		return
	}

	Logger.Warnf("%s[ERR] - error refreshing lock: %v", logPrefix, err)
}

func isStoreFromContentLockNotHeld(err error) bool {
	return errors.Is(err, redislock.ErrLockNotHeld) ||
		errors.Is(err, redislock.ErrNotObtained) ||
		strings.Contains(err.Error(), "redislock: lock not held") ||
		strings.Contains(err.Error(), "redislock: not obtained")
}

func storeContentHashLockKey(hash string) string {
	return "hash:" + hash
}

func storeContentLockKey(cachePath string, routingKey string) string {
	if isContentHash(routingKey) {
		return storeContentHashLockKey(routingKey)
	}
	if cachePath != "" {
		return cachePath
	}
	return routingKey
}
