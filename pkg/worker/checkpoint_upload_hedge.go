package worker

import (
	"context"
	"errors"
	"io"
	"net/http/httptrace"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/common"
)

// Extra uploads are only for immutable checkpoint objects whose request body
// has already been written. Keep amplification bounded across the whole worker.
var checkpointOriginHedgeSlots = make(chan struct{}, 4)

func checkpointOriginHedgeDelay() time.Duration {
	delay, err := time.ParseDuration(os.Getenv("BETA9_CHECKPOINT_ORIGIN_HEDGE_DELAY"))
	if err != nil || delay < time.Second || delay > 5*time.Second {
		return 0
	}
	return delay
}

func checkpointOriginHedgeEligible(size int64, opts clients.StorageUploadOptions) bool {
	// The default seekable uploader uses one PutObject below its 64 MiB part
	// size. Leave multipart and custom policies on their existing retry path.
	return size > 0 && size < 64<<20 && opts.PartSize == 0 && opts.StreamingConcurrency == 0
}

type checkpointOriginUploadFunc func(context.Context, io.Reader) error

type checkpointOriginUploadResult struct{ err error }

type checkpointOriginSeekableReader interface {
	io.Reader
	io.ReaderAt
	io.Seeker
}

// Both attempts use the same origin key and immutable bytes. An acknowledged
// upload is still required before availability; cancellation joins both uploader
// calls before the caller closes the archive. No mutation RPC is retried.
func uploadCheckpointArchiveWithHedge(ctx context.Context, file *os.File, primary io.Reader, size int64, delay time.Duration, slots chan struct{}, upload checkpointOriginUploadFunc) error {
	if delay <= 0 {
		return upload(ctx, primary)
	}
	// HTTP transports may close request bodies implementing io.ReadCloser.
	// Preserve seek/read-at behavior while retaining ownership of the shared
	// file until both attempts finish.
	if seekable, ok := primary.(checkpointOriginSeekableReader); ok {
		primary = struct{ checkpointOriginSeekableReader }{seekable}
	} else {
		primary = struct{ io.Reader }{primary}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan checkpointOriginUploadResult, 2)
	written := make(chan struct{}, 1)
	var mu sync.Mutex
	var lastWrite time.Time
	primaryCtx := httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
		if info.Err != nil {
			return
		}
		mu.Lock()
		lastWrite = time.Now()
		mu.Unlock()
		select {
		case written <- struct{}{}:
		default:
		}
	}})
	start := func(attemptCtx context.Context, reader io.Reader, role string, release func()) {
		go func() {
			started := time.Now()
			err := upload(attemptCtx, reader)
			common.RecordStartupSpanWithAttributes(ctx, "checkpoint.origin_upload_attempt", started, time.Now(), map[string]string{"role": role, "success": strconv.FormatBool(err == nil)})
			if release != nil {
				release()
			}
			results <- checkpointOriginUploadResult{err: err}
		}()
	}
	start(primaryCtx, primary, "primary", nil)
	waitPrimary := func() error {
		select {
		case result := <-results:
			return result.err
		case <-ctx.Done():
			cancel()
			result := <-results
			return errors.Join(ctx.Err(), result.err)
		}
	}
	select {
	case result := <-results:
		return result.err
	case <-ctx.Done():
		return waitPrimary()
	case <-written:
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	for {
		select {
		case result := <-results:
			return result.err
		case <-ctx.Done():
			return waitPrimary()
		case <-timer.C:
		}
		mu.Lock()
		remaining := delay - time.Since(lastWrite)
		mu.Unlock()
		if remaining > 0 { // A completed SDK retry restarts the post-write window.
			timer.Reset(remaining)
			continue
		}
		select {
		case result := <-results:
			return result.err
		case <-ctx.Done():
			return waitPrimary()
		case slots <- struct{}{}:
		}
		if ctx.Err() != nil {
			<-slots
			return waitPrimary()
		}
		mu.Lock()
		remaining = delay - time.Since(lastWrite)
		mu.Unlock()
		if remaining > 0 {
			<-slots
			timer.Reset(remaining)
			continue
		}
		break
	}
	now := time.Now()
	common.RecordStartupSpan(ctx, "checkpoint.origin_hedge_started", now, now)
	start(ctx, io.NewSectionReader(file, 0, size), "hedge", func() { <-slots })
	var failures []error
	for pending := 2; pending > 0; pending-- {
		result := <-results
		if result.err == nil {
			cancel()
			for remaining := pending - 1; remaining > 0; remaining-- {
				<-results
			}
			return nil
		}
		failures = append(failures, result.err)
	}
	return errors.Join(failures...)
}
