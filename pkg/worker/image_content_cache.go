package worker

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/rs/zerolog/log"
)

const (
	imageContentCacheReadTimeout = 120 * time.Second
	imageContentCacheSlowRead    = 250 * time.Millisecond
	imageContentCacheSlowStore   = 500 * time.Millisecond
	imageContentCacheSummary     = 5 * time.Second
)

type imageContentCache struct {
	client  *cache.Client
	imageID string

	readRequests  atomic.Int64
	readBytes     atomic.Int64
	readErrors    atomic.Int64
	storeRequests atomic.Int64
	storeBytes    atomic.Int64
	storeErrors   atomic.Int64
	lastSummaryNS atomic.Int64
}

func newImageContentCache(client *cache.Client, imageID string) *imageContentCache {
	if client == nil {
		return nil
	}
	return &imageContentCache{client: client, imageID: imageID}
}

func (c *imageContentCache) GetContent(hash string, offset int64, length int64, opts struct{ RoutingKey string }) ([]byte, error) {
	if length < 0 || length > int64(int(length)) {
		return nil, fmt.Errorf("invalid image content cache read length: %d", length)
	}

	dst := make([]byte, int(length))
	n, err := c.ReadContentInto(hash, offset, dst, opts)
	if err != nil {
		return nil, err
	}
	if n != length {
		return nil, cache.ErrContentNotFound
	}

	return dst[:n], nil
}

func (c *imageContentCache) ReadContentInto(hash string, offset int64, dest []byte, opts struct{ RoutingKey string }) (read int64, err error) {
	if c == nil || c.client == nil {
		return 0, cache.ErrClientNotFound
	}
	if opts.RoutingKey == "" {
		opts.RoutingKey = hash
	}

	started := time.Now()
	length := int64(len(dest))
	c.readRequests.Add(1)
	c.readBytes.Add(length)
	defer func() {
		elapsed := time.Since(started)
		if err != nil || read != length {
			c.readErrors.Add(1)
			log.Warn().
				Err(err).
				Str("image_id", c.imageID).
				Str("hash", shortHash(hash)).
				Str("routing_key", shortHash(opts.RoutingKey)).
				Int64("offset", offset).
				Int64("length", length).
				Int64("read", read).
				Dur("elapsed", elapsed).
				Msg("clip image content cache read result")
		} else if elapsed > imageContentCacheSlowRead {
			log.Info().
				Str("image_id", c.imageID).
				Str("hash", shortHash(hash)).
				Str("routing_key", shortHash(opts.RoutingKey)).
				Int64("offset", offset).
				Int64("length", length).
				Dur("elapsed", elapsed).
				Msg("clip image content cache slow read")
		}
		c.maybeLogSummary()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), imageContentCacheReadTimeout)
	defer cancel()

	return c.client.ReadContentInto(ctx, hash, offset, dest, cache.ClientOptions{RoutingKey: opts.RoutingKey})
}

func (c *imageContentCache) StoreContent(chunks chan []byte, hash string, opts struct{ RoutingKey string }) (string, error) {
	if c == nil || c.client == nil {
		return "", cache.ErrClientNotFound
	}
	if opts.RoutingKey == "" {
		opts.RoutingKey = hash
	}

	started := time.Now()
	c.storeRequests.Add(1)
	countingChunks := make(chan []byte, 2)

	go func() {
		defer close(countingChunks)
		for chunk := range chunks {
			c.storeBytes.Add(int64(len(chunk)))
			countingChunks <- chunk
		}
	}()

	actualHash, err := c.client.StoreContent(countingChunks, hash, opts)
	elapsed := time.Since(started)
	if err != nil {
		c.storeErrors.Add(1)
		log.Warn().
			Err(err).
			Str("image_id", c.imageID).
			Str("hash", shortHash(hash)).
			Str("routing_key", shortHash(opts.RoutingKey)).
			Dur("elapsed", elapsed).
			Msg("clip image content cache store result")
	} else if elapsed > imageContentCacheSlowStore {
		log.Info().
			Str("image_id", c.imageID).
			Str("hash", shortHash(hash)).
			Str("actual_hash", shortHash(actualHash)).
			Str("routing_key", shortHash(opts.RoutingKey)).
			Dur("elapsed", elapsed).
			Msg("clip image content cache store result")
	}
	c.maybeLogSummary()

	return actualHash, err
}

func (c *imageContentCache) maybeLogSummary() {
	now := time.Now()
	last := c.lastSummaryNS.Load()
	if last != 0 && now.Sub(time.Unix(0, last)) < imageContentCacheSummary {
		return
	}
	if !c.lastSummaryNS.CompareAndSwap(last, now.UnixNano()) {
		return
	}

	log.Info().
		Str("image_id", c.imageID).
		Int64("read_requests", c.readRequests.Load()).
		Int64("read_bytes", c.readBytes.Load()).
		Int64("read_errors", c.readErrors.Load()).
		Int64("store_requests", c.storeRequests.Load()).
		Int64("store_bytes", c.storeBytes.Load()).
		Int64("store_errors", c.storeErrors.Load()).
		Msg("clip image content cache summary")
}

func shortHash(hash string) string {
	if len(hash) <= 12 {
		return hash
	}
	return hash[:12]
}
