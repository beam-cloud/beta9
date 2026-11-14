package mirror

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// ObjectCache is a content-addressed cache shared by the mounts on a node.
type ObjectCache interface {
	// ReadObject reports whether it filled dest from the cached object.
	ReadObject(ctx context.Context, digest Digest, offset int64, dest []byte) bool
	// StoreObject keeps verified object bytes for later reads.
	StoreObject(ctx context.Context, digest Digest, data []byte) error
}

// CachedSource reads objects from a cache and, on a miss, downloads the
// whole object, checks it against its name and puts it in the cache. An
// object that does not match its digest is never served.
type CachedSource struct {
	Cache ObjectCache
	// Download opens the object in the store.
	Download func(ctx context.Context, digest Digest) (io.ReadCloser, error)
	Attempts int
	// RecentBytes bounds the downloaded objects held in memory, which is
	// what serves reads when the cache cannot take an object.
	RecentBytes int

	flights singleflight.Group
	mu      sync.Mutex
	recent  map[Digest]*list.Element
	order   list.List
	held    int
}

type recentObject struct {
	digest Digest
	data   []byte
}

var ErrObjectMismatch = errors.New("mirror object does not match its digest")

func (s *CachedSource) ReadObject(ctx context.Context, object Object, offset int64, dest []byte) error {
	if offset < 0 || uint64(offset)+uint64(len(dest)) > object.Size {
		return fmt.Errorf("read past the end of object %s", object.Digest)
	}
	if data := s.remembered(object.Digest); data != nil {
		copy(dest, data[offset:])
		return nil
	}
	if s.Cache != nil && s.Cache.ReadObject(ctx, object.Digest, offset, dest) {
		return nil
	}
	result := s.flights.DoChan(string(object.Digest[:]), func() (any, error) {
		return s.fetch(context.WithoutCancel(ctx), object)
	})
	select {
	case outcome := <-result:
		if outcome.Err != nil {
			return outcome.Err
		}
		copy(dest, outcome.Val.([]byte)[offset:])
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *CachedSource) remembered(digest Digest) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	element, ok := s.recent[digest]
	if !ok {
		return nil
	}
	s.order.MoveToFront(element)
	return element.Value.(*recentObject).data
}

func (s *CachedSource) fetch(ctx context.Context, object Object) ([]byte, error) {
	if data := s.remembered(object.Digest); data != nil {
		return data, nil
	}
	attempts := max(s.Attempts, 1)
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		data, err := s.download(attemptCtx, object)
		cancel()
		if err == nil {
			s.remember(object.Digest, data)
			if s.Cache != nil {
				_ = s.Cache.StoreObject(ctx, object.Digest, data)
			}
			return data, nil
		}
		lastErr = err
		if attempt < attempts {
			time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
		}
	}
	return nil, fmt.Errorf("fetch mirror object %s: %w", object.Digest, lastErr)
}

func (s *CachedSource) download(ctx context.Context, object Object) ([]byte, error) {
	body, err := s.Download(ctx, object.Digest)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	// One byte more than expected tells a longer object from a matching one.
	data := make([]byte, object.Size+1)
	n, err := io.ReadFull(body, data)
	if err != io.ErrUnexpectedEOF && err != io.EOF {
		if err == nil {
			err = ErrObjectMismatch
		}
		return nil, err
	}
	data = data[:n]
	if uint64(n) != object.Size || DigestOf(data) != object.Digest {
		return nil, ErrObjectMismatch
	}
	return data, nil
}

func (s *CachedSource) remember(digest Digest, data []byte) {
	budget := s.RecentBytes
	if budget <= 0 {
		budget = 128 << 20
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.recent == nil {
		s.recent = map[Digest]*list.Element{}
	}
	if _, ok := s.recent[digest]; ok {
		return
	}
	s.recent[digest] = s.order.PushFront(&recentObject{digest: digest, data: data})
	s.held += len(data)
	for s.held > budget && s.order.Len() > 1 {
		oldest := s.order.Remove(s.order.Back()).(*recentObject)
		delete(s.recent, oldest.digest)
		s.held -= len(oldest.data)
	}
}
