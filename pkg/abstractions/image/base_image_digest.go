package image

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"golang.org/x/sync/singleflight"
)

const baseImageDigestCacheTTL = 5 * time.Minute

type baseImageDigestCacheEntry struct {
	digest    string
	expiresAt time.Time
}

type baseImageDigestCache struct {
	mu      sync.Mutex
	entries map[string]baseImageDigestCacheEntry
	group   singleflight.Group
}

func newBaseImageDigestCache() baseImageDigestCache {
	return baseImageDigestCache{
		entries: make(map[string]baseImageDigestCacheEntry),
	}
}

func (is *ContainerImageService) resolveBaseImageDigest(ctx context.Context, opts *BuildOpts, cacheable bool) {
	if opts == nil || opts.BaseImageDigest != "" || opts.BaseImageRegistry == "" || opts.BaseImageName == "" || opts.BaseImageTag == "" {
		return
	}

	sourceImage := getSourceImage(opts)
	if !cacheable {
		opts.BaseImageDigest = is.inspectBaseImageDigest(ctx, sourceImage, opts.BaseImageCreds)
		return
	}

	digest, shared := is.baseImageDigests.resolve(ctx, sourceImage, opts.BaseImageCreds, is.inspectBaseImageDigest)
	if digest == "" {
		return
	}

	opts.BaseImageDigest = digest
	if shared {
		log.Debug().Str("source_image", sourceImage).Msg("resolved base image digest from shared lookup")
	}
}

func (c *baseImageDigestCache) resolve(
	ctx context.Context,
	sourceImage string,
	creds string,
	inspect func(context.Context, string, string) string,
) (string, bool) {
	if digest := c.get(sourceImage); digest != "" {
		return digest, false
	}

	digest, _, shared := c.group.Do(sourceImage, func() (interface{}, error) {
		if digest := c.get(sourceImage); digest != "" {
			return digest, nil
		}

		digest := inspect(ctx, sourceImage, creds)
		if digest != "" {
			c.set(sourceImage, digest)
		}
		return digest, nil
	})

	resolvedDigest, _ := digest.(string)
	return resolvedDigest, shared
}

func (is *ContainerImageService) inspectBaseImageDigest(ctx context.Context, sourceImage, creds string) string {
	startedAt := time.Now()
	metadata, err := is.builder.skopeoClient.Inspect(ctx, sourceImage, creds, nil)
	if err != nil {
		log.Warn().Err(err).Str("source_image", sourceImage).Msg("failed to resolve base image digest for image identity")
		return ""
	}
	if metadata.Digest == "" {
		log.Warn().Str("source_image", sourceImage).Msg("base image digest missing from registry inspect")
		return ""
	}

	log.Debug().Str("source_image", sourceImage).Dur("duration", time.Since(startedAt)).Msg("resolved base image digest")
	return metadata.Digest
}

func (c *baseImageDigestCache) get(sourceImage string) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[sourceImage]
	if !ok {
		return ""
	}
	if time.Now().After(entry.expiresAt) {
		delete(c.entries, sourceImage)
		return ""
	}
	return entry.digest
}

func (c *baseImageDigestCache) set(sourceImage, digest string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries[sourceImage] = baseImageDigestCacheEntry{
		digest:    digest,
		expiresAt: time.Now().Add(baseImageDigestCacheTTL),
	}
}
