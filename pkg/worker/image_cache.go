package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
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

const (
	imageContentCacheOperationReadInto       = "read_into"
	imageContentCacheOperationPageFileViews  = "page_file_views"
	imageContentCacheOperationStoreStream    = "store_stream"
	imageContentCacheOperationStoreLocalPath = "store_local_path"

	imageContentCacheResultHit                = "hit"
	imageContentCacheResultMiss               = "miss"
	imageContentCacheResultUnavailable        = "unavailable"
	imageContentCacheResultShortRead          = "short_read"
	imageContentCacheResultError              = "error"
	imageContentCacheResultStoredOrPresent    = "stored_or_present"
	imageContentCacheResultSkippedUnavailable = "skipped_unavailable"
)

type imageContentCache struct {
	client  *cache.Client
	imageID string
	kind    string
	observe imageContentCacheObserver

	readRequests     atomic.Int64
	readBytes        atomic.Int64
	readHits         atomic.Int64
	readMisses       atomic.Int64
	readUnavailable  atomic.Int64
	readShortReads   atomic.Int64
	readErrors       atomic.Int64
	existsRequests   atomic.Int64
	existsHits       atomic.Int64
	existsErrors     atomic.Int64
	pageViewRequests atomic.Int64
	pageViewHits     atomic.Int64
	pageViewMisses   atomic.Int64
	pageViewErrors   atomic.Int64
	pageViewBytes    atomic.Int64
	storeRequests    atomic.Int64
	storeBytes       atomic.Int64
	storeSuccesses   atomic.Int64
	storeSkipped     atomic.Int64
	storeErrors      atomic.Int64
	lastSummaryNS    atomic.Int64
}

type imageContentCacheObserver func(imageContentCacheTrace)

type imageContentCacheTrace struct {
	Operation  string
	Result     string
	ImageID    string
	Kind       string
	Hash       string
	RoutingKey string
	Offset     int64
	Length     int64
	Read       int64
	Views      int
	Bytes      int64
	StartedAt  time.Time
	Duration   time.Duration
	Error      string
	Trace      cache.OperationTrace
}

type imageContentCacheStoreTrace struct {
	operation  string
	result     string
	hash       string
	actualHash string
	routingKey string
	path       string
	bytes      int64
	startedAt  time.Time
	err        error
	traceErr   error
	trace      cache.OperationTrace
}

func newImageContentCache(client *cache.Client, imageID string, kind string, observers ...imageContentCacheObserver) *imageContentCache {
	if client == nil {
		return nil
	}
	cacheKind := "content"
	if kind != "" {
		cacheKind = kind
	}
	var observer imageContentCacheObserver
	if len(observers) > 0 {
		observer = observers[0]
	}
	return &imageContentCache{client: client, imageID: imageID, kind: cacheKind, observe: observer}
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
		return nil, fmt.Errorf("%w: short read", clipStorage.ErrContentCacheMiss)
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
	var cacheTrace cache.OperationTrace
	c.readRequests.Add(1)
	c.readBytes.Add(length)
	defer func() {
		elapsed := time.Since(started)
		result := imageContentCacheReadResult(err, read, length)
		switch result {
		case imageContentCacheResultHit:
			c.readHits.Add(1)
		case imageContentCacheResultMiss:
			c.readMisses.Add(1)
		case imageContentCacheResultUnavailable:
			c.readUnavailable.Add(1)
		case imageContentCacheResultShortRead:
			c.readShortReads.Add(1)
		}
		if err != nil || read != length {
			c.readErrors.Add(1)
			log.Warn().
				Err(err).
				Str("cache_result", result).
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
				Str("cache_result", result).
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
		c.observeContentCacheTrace(imageContentCacheTrace{
			Operation:  imageContentCacheOperationReadInto,
			Result:     result,
			ImageID:    c.imageID,
			Kind:       c.kind,
			Hash:       hash,
			RoutingKey: opts.RoutingKey,
			Offset:     offset,
			Length:     length,
			Read:       read,
			Bytes:      read,
			StartedAt:  started,
			Duration:   elapsed,
			Error:      imageContentCacheErrorString(err),
			Trace:      cacheTrace,
		})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), imageContentCacheReadTimeout)
	defer cancel()

	read, cacheTrace, err = c.client.ReadContentIntoWithTrace(ctx, hash, offset, dest, cache.ClientOptions{RoutingKey: opts.RoutingKey})
	if err != nil {
		return read, imageContentCacheError(err)
	}
	return read, nil
}

func imageContentCacheError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, cache.ErrContentNotFound) {
		return fmt.Errorf("%w: %w", clipStorage.ErrContentCacheMiss, err)
	}
	if errors.Is(err, cache.ErrSelectedHostUnavailable) ||
		errors.Is(err, cache.ErrUnableToReachHost) ||
		errors.Is(err, cache.ErrHostNotFound) ||
		errors.Is(err, cache.ErrClientNotFound) {
		return fmt.Errorf("%w: %w", clipStorage.ErrContentCacheUnavailable, err)
	}
	return err
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

	// CLIP's ContentExists hook does not include the decompressed layer size, so
	// a positive response cannot distinguish a complete layer from a stale
	// partially-published page directory. Let the following StoreContentFromLocalPath
	// call do a size-aware selected-host completeness check before it decides
	// whether the store can be skipped.
	return false, nil
}

func (c *imageContentCache) ClientLocalPageFileViews(hash string, offset int64, length int64, opts struct{ RoutingKey string }) (views []clipStorage.ClientLocalPageFileView, err error) {
	if c == nil || c.client == nil {
		return nil, cache.ErrClientNotFound
	}
	if opts.RoutingKey == "" {
		opts.RoutingKey = hash
	}

	started := time.Now()
	var cacheTrace cache.OperationTrace
	c.pageViewRequests.Add(1)
	defer func() {
		elapsed := time.Since(started)
		result := imageContentCachePageViewResult(err, len(views))
		switch result {
		case imageContentCacheResultHit:
			c.pageViewHits.Add(1)
			c.pageViewBytes.Add(length)
		case imageContentCacheResultMiss:
			c.pageViewMisses.Add(1)
		case imageContentCacheResultUnavailable, imageContentCacheResultError:
			c.pageViewErrors.Add(1)
		}
		if err != nil {
			log.Warn().
				Err(err).
				Str("cache_result", result).
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
				Str("cache_result", result).
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
		c.observeContentCacheTrace(imageContentCacheTrace{
			Operation:  imageContentCacheOperationPageFileViews,
			Result:     result,
			ImageID:    c.imageID,
			Kind:       c.kind,
			Hash:       hash,
			RoutingKey: opts.RoutingKey,
			Offset:     offset,
			Length:     length,
			Views:      len(views),
			Bytes:      length,
			StartedAt:  started,
			Duration:   elapsed,
			Error:      imageContentCacheErrorString(err),
			Trace:      cacheTrace,
		})
	}()

	localViews, cacheTrace, err := c.client.ClientLocalPageFileViewsWithTrace(hash, offset, length, cache.ClientOptions{RoutingKey: opts.RoutingKey})
	if err != nil {
		if errors.Is(err, cache.ErrContentNotFound) {
			return nil, nil
		}
		return nil, imageContentCacheError(err)
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
	if c.skipRuntimeStoreWhenUnavailable(hash, opts.RoutingKey) {
		go drainImageContentChunks(chunks)
		c.finishStore(imageContentCacheStoreTrace{
			operation:  imageContentCacheOperationStoreStream,
			result:     imageContentCacheResultSkippedUnavailable,
			hash:       hash,
			actualHash: hash,
			routingKey: opts.RoutingKey,
			startedAt:  started,
		})
		return hash, nil
	}

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
	traceErr := err
	result := imageContentCacheResultStoredOrPresent
	if err != nil && c.bestEffortRuntimeStore() && cache.IsStoreHostUnavailable(err) {
		result = imageContentCacheResultSkippedUnavailable
		actualHash = hash
		err = nil
	}
	c.finishStore(imageContentCacheStoreTrace{
		operation:  imageContentCacheOperationStoreStream,
		result:     result,
		hash:       hash,
		actualHash: actualHash,
		routingKey: opts.RoutingKey,
		bytes:      storeBytes.Load(),
		startedAt:  started,
		err:        err,
		traceErr:   traceErr,
	})

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
	var cacheTrace cache.OperationTrace
	c.storeRequests.Add(1)
	if c.skipRuntimeStoreWhenUnavailable(hash, opts.RoutingKey) {
		c.finishStore(imageContentCacheStoreTrace{
			operation:  imageContentCacheOperationStoreLocalPath,
			result:     imageContentCacheResultSkippedUnavailable,
			hash:       hash,
			actualHash: hash,
			routingKey: opts.RoutingKey,
			path:       path,
			bytes:      fileSize(path),
			startedAt:  started,
		})
		return hash, nil
	}

	var traceErr error
	defer func() {
		result := cacheTrace.Result
		c.finishStore(imageContentCacheStoreTrace{
			operation:  imageContentCacheOperationStoreLocalPath,
			result:     result,
			hash:       hash,
			actualHash: actualHash,
			routingKey: opts.RoutingKey,
			path:       path,
			bytes:      fileSize(path),
			startedAt:  started,
			err:        err,
			traceErr:   traceErr,
			trace:      cacheTrace,
		})
	}()

	actualHash, cacheTrace, err = c.client.StoreContentFromLocalFileWithTrace(cache.LocalContentSource{
		Path:      path,
		CachePath: path,
	}, cache.StoreContentOptions{
		RoutingKey: opts.RoutingKey,
		Lock:       true,
	})
	if err != nil && c.bestEffortRuntimeStore() && cache.IsStoreHostUnavailable(err) {
		traceErr = err
		cacheTrace.Result = imageContentCacheResultSkippedUnavailable
		actualHash = hash
		err = nil
	}
	return actualHash, err
}

func (c *imageContentCache) finishStore(trace imageContentCacheStoreTrace) {
	elapsed := time.Since(trace.startedAt)
	if trace.result == "" {
		trace.result = imageContentCacheResultStoredOrPresent
	}
	if trace.actualHash == "" {
		trace.actualHash = trace.hash
	}
	if trace.traceErr == nil {
		trace.traceErr = trace.err
	}

	switch {
	case trace.err != nil:
		trace.result = imageContentCacheResultError
		c.storeErrors.Add(1)
		event := log.Warn().
			Err(trace.err).
			Str("operation", trace.operation).
			Str("cache_result", trace.result).
			Str("image_id", c.imageID).
			Str("kind", c.kind).
			Str("hash", shortHash(trace.hash)).
			Str("routing_key", shortHash(trace.routingKey)).
			Int64("bytes", trace.bytes).
			Dur("elapsed", elapsed)
		if trace.path != "" {
			event.Str("path", trace.path)
		}
		event.Msg("clip image content cache store result")
	case trace.result == imageContentCacheResultSkippedUnavailable:
		c.storeSkipped.Add(1)
	default:
		c.storeSuccesses.Add(1)
		if elapsed > imageContentCacheSlowStore {
			event := log.Debug().
				Str("operation", trace.operation).
				Str("cache_result", trace.result).
				Str("image_id", c.imageID).
				Str("kind", c.kind).
				Str("hash", shortHash(trace.hash)).
				Str("actual_hash", shortHash(trace.actualHash)).
				Str("routing_key", shortHash(trace.routingKey)).
				Int64("bytes", trace.bytes).
				Dur("elapsed", elapsed)
			if trace.path != "" {
				event.Str("path", trace.path)
			}
			event.Msg("clip image content cache store result")
		}
	}

	c.maybeLogSummary()
	c.observeContentCacheTrace(imageContentCacheTrace{
		Operation:  trace.operation,
		Result:     trace.result,
		ImageID:    c.imageID,
		Kind:       c.kind,
		Hash:       trace.hash,
		RoutingKey: trace.routingKey,
		Bytes:      trace.bytes,
		StartedAt:  trace.startedAt,
		Duration:   elapsed,
		Error:      imageContentCacheErrorString(trace.traceErr),
		Trace:      trace.trace,
	})
}

func drainImageContentChunks(chunks <-chan []byte) {
	for range chunks {
	}
}

func (c *imageContentCache) observeContentCacheTrace(event imageContentCacheTrace) {
	if c == nil || c.observe == nil {
		return
	}
	c.observe(event)
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
		Int64("read_hits", c.readHits.Load()).
		Int64("read_misses", c.readMisses.Load()).
		Int64("read_unavailable", c.readUnavailable.Load()).
		Int64("read_short_reads", c.readShortReads.Load()).
		Int64("read_errors", c.readErrors.Load()).
		Int64("exists_requests", c.existsRequests.Load()).
		Int64("exists_hits", c.existsHits.Load()).
		Int64("exists_errors", c.existsErrors.Load()).
		Int64("page_view_requests", c.pageViewRequests.Load()).
		Int64("page_view_hits", c.pageViewHits.Load()).
		Int64("page_view_misses", c.pageViewMisses.Load()).
		Int64("page_view_errors", c.pageViewErrors.Load()).
		Int64("page_view_bytes", c.pageViewBytes.Load()).
		Int64("store_requests", c.storeRequests.Load()).
		Int64("store_bytes", c.storeBytes.Load()).
		Int64("store_successes", c.storeSuccesses.Load()).
		Int64("store_skipped", c.storeSkipped.Load()).
		Int64("store_errors", c.storeErrors.Load()).
		Msg("clip image content cache summary")
}

func (c *imageContentCache) bestEffortRuntimeStore() bool {
	return c != nil && (c.kind == "legacy-file-runtime" || c.kind == "oci-layer-runtime")
}

func (c *imageContentCache) skipRuntimeStoreWhenUnavailable(hash string, routingKey string) bool {
	return c.bestEffortRuntimeStore() && !c.client.SelectedStoreHostAvailable(hash, routingKey)
}

func imageContentCacheErrorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

func imageContentCacheReadResult(err error, read int64, length int64) string {
	switch {
	case err == nil && read == length:
		return imageContentCacheResultHit
	case errors.Is(err, clipStorage.ErrContentCacheMiss):
		return imageContentCacheResultMiss
	case errors.Is(err, clipStorage.ErrContentCacheUnavailable):
		return imageContentCacheResultUnavailable
	case err == nil && read != length:
		return imageContentCacheResultShortRead
	default:
		return imageContentCacheResultError
	}
}

func imageContentCachePageViewResult(err error, viewCount int) string {
	switch {
	case err == nil && viewCount > 0:
		return imageContentCacheResultHit
	case err == nil:
		return imageContentCacheResultMiss
	case errors.Is(err, clipStorage.ErrContentCacheMiss):
		return imageContentCacheResultMiss
	case errors.Is(err, clipStorage.ErrContentCacheUnavailable):
		return imageContentCacheResultUnavailable
	default:
		return imageContentCacheResultError
	}
}

func shortHash(hash string) string {
	if len(hash) <= 12 {
		return hash
	}
	return hash[:12]
}
