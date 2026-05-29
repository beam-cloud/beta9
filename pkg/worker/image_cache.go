package worker

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	clipStorage "github.com/beam-cloud/clip/pkg/storage"
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
	kind    string

	readRequests   atomic.Int64
	readBytes      atomic.Int64
	readErrors     atomic.Int64
	existsRequests atomic.Int64
	existsHits     atomic.Int64
	existsErrors   atomic.Int64
	storeRequests  atomic.Int64
	storeBytes     atomic.Int64
	storeErrors    atomic.Int64
	lastSummaryNS  atomic.Int64
}

func newImageContentCache(client *cache.Client, imageID string, kind ...string) *imageContentCache {
	if client == nil {
		return nil
	}
	cacheKind := "content"
	if len(kind) > 0 && kind[0] != "" {
		cacheKind = kind[0]
	}
	return &imageContentCache{client: client, imageID: imageID, kind: cacheKind}
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
				Str("kind", c.kind).
				Str("hash", shortHash(hash)).
				Str("routing_key", shortHash(opts.RoutingKey)).
				Int64("offset", offset).
				Int64("length", length).
				Int64("read", read).
				Dur("elapsed", elapsed).
				Msg("clip image content cache read result")
		} else if elapsed > imageContentCacheSlowRead {
			log.Debug().
				Str("image_id", c.imageID).
				Str("kind", c.kind).
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

func (c *imageContentCache) ContentExists(hash string, opts struct{ RoutingKey string }) (exists bool, err error) {
	if c == nil || c.client == nil {
		return false, cache.ErrClientNotFound
	}
	if opts.RoutingKey == "" {
		opts.RoutingKey = hash
	}

	started := time.Now()
	c.existsRequests.Add(1)
	defer func() {
		elapsed := time.Since(started)
		if err != nil {
			c.existsErrors.Add(1)
			log.Warn().
				Err(err).
				Str("image_id", c.imageID).
				Str("kind", c.kind).
				Str("hash", shortHash(hash)).
				Str("routing_key", shortHash(opts.RoutingKey)).
				Dur("elapsed", elapsed).
				Msg("clip image content cache exists result")
		} else if exists {
			c.existsHits.Add(1)
			log.Debug().
				Str("image_id", c.imageID).
				Str("kind", c.kind).
				Str("hash", shortHash(hash)).
				Str("routing_key", shortHash(opts.RoutingKey)).
				Dur("elapsed", elapsed).
				Msg("clip image content cache exists hit")
		}
		c.maybeLogSummary()
	}()

	return c.client.IsCachedOnSelectedHost(hash, opts.RoutingKey)
}

func (c *imageContentCache) ClientLocalPageFileViews(hash string, offset int64, length int64, opts struct{ RoutingKey string }) (views []clipStorage.ClientLocalPageFileView, err error) {
	if c == nil || c.client == nil {
		return nil, cache.ErrClientNotFound
	}
	if opts.RoutingKey == "" {
		opts.RoutingKey = hash
	}

	started := time.Now()
	defer func() {
		elapsed := time.Since(started)
		if err != nil {
			log.Warn().
				Err(err).
				Str("image_id", c.imageID).
				Str("kind", c.kind).
				Str("hash", shortHash(hash)).
				Str("routing_key", shortHash(opts.RoutingKey)).
				Int64("offset", offset).
				Int64("length", length).
				Int("views", len(views)).
				Dur("elapsed", elapsed).
				Msg("clip image content cache client-local page-file views result")
		} else if len(views) == 0 || elapsed > imageContentCacheSlowRead {
			log.Debug().
				Str("image_id", c.imageID).
				Str("kind", c.kind).
				Str("hash", shortHash(hash)).
				Str("routing_key", shortHash(opts.RoutingKey)).
				Int64("offset", offset).
				Int64("length", length).
				Int("views", len(views)).
				Dur("elapsed", elapsed).
				Msg("clip image content cache client-local page-file views result")
		}
		c.maybeLogSummary()
	}()

	localViews, err := c.client.ClientLocalPageFileViews(hash, offset, length, cache.ClientOptions{RoutingKey: opts.RoutingKey})
	if err != nil {
		return nil, err
	}
	views = make([]clipStorage.ClientLocalPageFileView, 0, len(localViews))
	for _, view := range localViews {
		views = append(views, clipStorage.ClientLocalPageFileView{
			Path:   view.Path,
			Offset: view.Offset,
			Length: view.Length,
		})
	}
	return views, nil
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
	done := make(chan struct{})
	var storeBytes atomic.Int64

	go func() {
		defer close(countingChunks)
		for chunk := range chunks {
			n := int64(len(chunk))
			select {
			case countingChunks <- chunk:
				c.storeBytes.Add(n)
				storeBytes.Add(n)
			case <-done:
				drainImageContentChunks(chunks)
				return
			}
		}
	}()

	actualHash, err := c.client.StoreContent(countingChunks, hash, struct{ RoutingKey string }{RoutingKey: opts.RoutingKey})
	close(done)
	elapsed := time.Since(started)
	if err != nil {
		c.storeErrors.Add(1)
		log.Warn().
			Err(err).
			Str("image_id", c.imageID).
			Str("kind", c.kind).
			Str("hash", shortHash(hash)).
			Str("routing_key", shortHash(opts.RoutingKey)).
			Int64("bytes", storeBytes.Load()).
			Dur("elapsed", elapsed).
			Msg("clip image content cache store result")
	} else if elapsed > imageContentCacheSlowStore {
		log.Debug().
			Str("image_id", c.imageID).
			Str("kind", c.kind).
			Str("hash", shortHash(hash)).
			Str("actual_hash", shortHash(actualHash)).
			Str("routing_key", shortHash(opts.RoutingKey)).
			Int64("bytes", storeBytes.Load()).
			Dur("elapsed", elapsed).
			Msg("clip image content cache store result")
	}
	c.maybeLogSummary()

	return actualHash, err
}

func (c *imageContentCache) StoreContentFromLocalPath(path string, hash string, opts struct{ RoutingKey string }) (actualHash string, err error) {
	if c == nil || c.client == nil {
		return "", cache.ErrClientNotFound
	}
	if opts.RoutingKey == "" {
		opts.RoutingKey = hash
	}

	started := time.Now()
	c.storeRequests.Add(1)
	defer func() {
		elapsed := time.Since(started)
		if err != nil {
			c.storeErrors.Add(1)
			log.Warn().
				Err(err).
				Str("image_id", c.imageID).
				Str("kind", c.kind).
				Str("hash", shortHash(hash)).
				Str("routing_key", shortHash(opts.RoutingKey)).
				Str("path", path).
				Dur("elapsed", elapsed).
				Msg("clip image content cache local-path store result")
		} else if elapsed > imageContentCacheSlowStore {
			log.Debug().
				Str("image_id", c.imageID).
				Str("kind", c.kind).
				Str("hash", shortHash(hash)).
				Str("actual_hash", shortHash(actualHash)).
				Str("routing_key", shortHash(opts.RoutingKey)).
				Str("path", path).
				Dur("elapsed", elapsed).
				Msg("clip image content cache local-path store result")
		}
		c.maybeLogSummary()
	}()

	return c.client.StoreContentFromLocalFile(cache.LocalContentSource{
		Path:      path,
		CachePath: path,
	}, cache.StoreContentOptions{
		RoutingKey: opts.RoutingKey,
		Lock:       true,
	})
}

func drainImageContentChunks(chunks <-chan []byte) {
	for range chunks {
	}
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

	log.Debug().
		Str("image_id", c.imageID).
		Str("kind", c.kind).
		Int64("read_requests", c.readRequests.Load()).
		Int64("read_bytes", c.readBytes.Load()).
		Int64("read_errors", c.readErrors.Load()).
		Int64("exists_requests", c.existsRequests.Load()).
		Int64("exists_hits", c.existsHits.Load()).
		Int64("exists_errors", c.existsErrors.Load()).
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
