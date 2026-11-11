package clients

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	storageDownloadPartSize     = 16 * 1024 * 1024
	storageDownloadWorkers      = 8
	storageDownloadExtraWorkers = 16
	storageDownloadPartAttempts = 3
)

// storageDownloadSlots bounds the requests made at once beyond one for each
// object, and with them the memory their parts wait in: about 300 MiB.
var storageDownloadSlots = make(chan struct{}, storageDownloadExtraWorkers)

var storageDownloadParts = sync.Pool{New: func() any {
	part := make([]byte, storageDownloadPartSize)
	return &part
}}

// DownloadLargeWithReader reads an object of a known size in order, as
// DownloadWithReader does, from several requests at once, each for a range of
// it: object storage serves them faster together than it serves one response.
// A burst of downloads has the link to share: those that find no request to
// spare read one response.
func (c *StorageClient) DownloadLargeWithReader(ctx context.Context, key, bucket string, size int64) (io.ReadCloser, error) {
	workers := 1
	if size > storageDownloadPartSize {
		workers += acquireStorageDownloadSlots(storageDownloadWorkers - 1)
	}
	if workers == 1 {
		return c.DownloadWithReader(ctx, key, bucket)
	}

	return c.downloadRanges(ctx, key, bucket, size, storageDownloadPartSize, workers, func() {
		for range workers - 1 {
			<-storageDownloadSlots
		}
	}), nil
}

func acquireStorageDownloadSlots(want int) int {
	for held := 0; held < want; held++ {
		select {
		case storageDownloadSlots <- struct{}{}:
		default:
			return held
		}
	}
	return want
}

type storageRange struct {
	data    []byte
	err     error
	release func()
}

// storageRangeReader delivers the ranges of an object in order while up to
// cap(parts)+1 of them are being fetched or wait to be read.
type storageRangeReader struct {
	ctx     context.Context
	cancel  context.CancelFunc
	parts   chan chan storageRange
	fetches sync.WaitGroup
	closed  sync.Once
	release func()

	// Owned by Read.
	current storageRange
	err     error
}

// downloadRanges reads size bytes in parts of at most storageDownloadPartSize.
func (c *StorageClient) downloadRanges(ctx context.Context, key, bucket string, size, partSize int64, workers int, release func()) *storageRangeReader {
	ctx, cancel := context.WithCancel(ctx)
	r := &storageRangeReader{ctx: ctx, cancel: cancel, parts: make(chan chan storageRange, workers-1), release: release}
	r.fetches.Add(1)
	go func() {
		defer r.fetches.Done()
		defer close(r.parts)
		for offset := int64(0); offset < size; offset += partSize {
			part := make(chan storageRange, 1)
			select {
			case r.parts <- part:
			case <-ctx.Done():
				return
			}

			r.fetches.Add(1)
			go func() {
				defer r.fetches.Done()
				part <- c.downloadRange(ctx, key, bucket, offset, min(partSize, size-offset))
			}()
		}
	}()
	return r
}

// downloadRange fetches one range whole. A response that breaks off is asked
// for again here: the caller could only start the object over.
func (c *StorageClient) downloadRange(ctx context.Context, key, bucket string, offset, length int64) storageRange {
	buffer := storageDownloadParts.Get().(*[]byte)
	release := func() { storageDownloadParts.Put(buffer) }
	data := (*buffer)[:length]

	var err error
	for attempt := 1; ; attempt++ {
		var brokeOff bool
		if brokeOff, err = c.readRange(ctx, key, bucket, offset, data); err == nil {
			return storageRange{data: data, release: release}
		}
		if !brokeOff || attempt == storageDownloadPartAttempts || ctx.Err() != nil {
			break
		}
		select {
		case <-time.After(time.Duration(attempt) * 100 * time.Millisecond):
		case <-ctx.Done():
		}
	}
	release()
	return storageRange{err: fmt.Errorf("read bytes %d-%d of %s: %w", offset, offset+length-1, key, err)}
}

// readRange fills data from offset. brokeOff tells a response that ended
// early from a request the client has already retried or should not.
func (c *StorageClient) readRange(ctx context.Context, key, bucket string, offset int64, data []byte) (brokeOff bool, err error) {
	resp, err := c.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+int64(len(data))-1)),
	}, ordinaryObjectReadOptions)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	// A store that ignored the range would hand back the start of the object.
	if got := aws.ToInt64(resp.ContentLength); got != int64(len(data)) {
		return false, fmt.Errorf("object storage answered a range of %d bytes with %d", len(data), got)
	}
	_, err = io.ReadFull(resp.Body, data)
	return err != nil, err
}

func (r *storageRangeReader) Read(p []byte) (int, error) {
	for len(r.current.data) == 0 {
		if r.err != nil {
			return 0, r.err
		}
		if r.current.release != nil {
			r.current.release()
			r.current = storageRange{}
		}

		// Close ends a read that waits, as closing a response body does.
		var next chan storageRange
		var more bool
		select {
		case next, more = <-r.parts:
		case <-r.ctx.Done():
			r.err = r.ctx.Err()
			return 0, r.err
		}
		if !more {
			r.err = io.EOF
			if err := r.ctx.Err(); err != nil {
				r.err = err
			}
			return 0, r.err
		}

		select {
		case r.current = <-next:
		case <-r.ctx.Done():
			r.err = r.ctx.Err()
			return 0, r.err
		}
		if r.current.err != nil {
			r.err = r.current.err
			r.cancel()
			return 0, r.err
		}
	}

	n := copy(p, r.current.data)
	r.current.data = r.current.data[n:]
	return n, nil
}

func (r *storageRangeReader) Close() error {
	r.closed.Do(func() {
		r.cancel()
		r.fetches.Wait()
		r.release()
	})
	return nil
}
