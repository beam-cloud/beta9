package mirror

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
	"golang.org/x/sync/singleflight"
)

// ObjectCache keeps verified objects for later reads.
type ObjectCache interface {
	// ReadObject reports whether it filled dest from the cached object.
	ReadObject(ctx context.Context, object Object, offset int64, dest []byte) bool
	StoreObject(ctx context.Context, object Object, data []byte) error
}

// ContentStore is a cache keyed by the SHA-256 of what it holds, shared by
// every tenant on a node.
type ContentStore interface {
	ReadContent(ctx context.Context, key string, offset int64, dest []byte) bool
	StoreContent(ctx context.Context, key string, content []byte) error
}

// ScopedCache keeps one mirror's objects in a ContentStore.
//
// A manifest is written by a tenant and names objects by digest. Looking
// those digests up in a store that other tenants fill would hand a tenant
// any cached object whose digest it can name, without it ever having held
// the bytes. So an object is stored with its mirror's scope appended, under
// the SHA-256 of the whole: only a reader with the same scope computes that
// key, and only someone holding the bytes can have stored under it.
//
// The key can be computed from the digest and size alone because SHA-256
// can be continued from a digest. The object is stored followed by its own
// SHA-256 padding, and at that point the hash state is the object's digest.
// What follows is the scope and the object's size, so that a manifest which
// declares a wrong size for a digest finds nothing rather than the padding.
type ScopedCache struct {
	Store ContentStore
	Scope []byte
}

// sha256Padding is what SHA-256 appends to a message of size bytes.
func sha256Padding(size uint64) []byte {
	padding := make([]byte, 1+(119-size%64)%64, 72)
	padding[0] = 0x80
	return binary.BigEndian.AppendUint64(padding, size*8)
}

func (c ScopedCache) suffix(size uint64) []byte {
	suffix := make([]byte, 0, len(c.Scope)+8)
	suffix = append(suffix, c.Scope...)
	return binary.BigEndian.AppendUint64(suffix, size)
}

func (c ScopedCache) key(object Object) string {
	// crypto/sha256's saved state: magic, the eight state words, the
	// unprocessed block, and the count of bytes hashed.
	state := make([]byte, 0, 4+sha256.Size+sha256.BlockSize+8)
	state = append(state, "sha\x03"...)
	state = append(state, object.Digest[:]...)
	state = append(state, make([]byte, sha256.BlockSize)...)
	state = binary.BigEndian.AppendUint64(state, object.Size+uint64(len(sha256Padding(object.Size))))
	hash := sha256.New()
	if err := hash.(encoding.BinaryUnmarshaler).UnmarshalBinary(state); err != nil {
		return ""
	}
	hash.Write(c.suffix(object.Size))
	return hex.EncodeToString(hash.Sum(nil))
}

func (c ScopedCache) ReadObject(ctx context.Context, object Object, offset int64, dest []byte) bool {
	key := c.key(object)
	return key != "" && c.Store.ReadContent(ctx, key, offset, dest)
}

func (c ScopedCache) StoreObject(ctx context.Context, object Object, data []byte) error {
	key := c.key(object)
	if key == "" {
		return errors.New("cannot derive a cache key")
	}
	size := uint64(len(data))
	content := make([]byte, 0, len(data)+72+len(c.Scope)+8)
	content = append(content, data...)
	content = append(content, sha256Padding(size)...)
	content = append(content, c.suffix(size)...)
	return c.Store.StoreContent(ctx, key, content)
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

const downloadBudgetBytes = 128 << 20

// All mounts in this process share the budget. A manifest can be larger
// than a file object and takes the whole budget if it exceeds it.
var downloadBudget = semaphore.NewWeighted(downloadBudgetBytes)

func (s *CachedSource) ReadObject(ctx context.Context, object Object, offset int64, dest []byte) error {
	if object.Size > MaxManifestSize {
		return ErrObjectMismatch
	}
	if offset < 0 || uint64(offset)+uint64(len(dest)) > object.Size {
		return fmt.Errorf("read past the end of object %s", object.Digest)
	}
	if data := s.remembered(object.Digest); data != nil {
		copy(dest, data[offset:])
		return nil
	}
	if s.Cache != nil && s.Cache.ReadObject(ctx, object, offset, dest) {
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

// Forget drops the downloaded objects held in memory.
func (s *CachedSource) Forget() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recent, s.held = nil, 0
	s.order.Init()
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
	weight := min(int64(object.Size)+1, downloadBudgetBytes)
	waitCtx, cancelWait := context.WithTimeout(ctx, 2*time.Minute)
	err := downloadBudget.Acquire(waitCtx, weight)
	cancelWait()
	if err != nil {
		return nil, fmt.Errorf("wait to fetch mirror object %s: %w", object.Digest, err)
	}
	defer downloadBudget.Release(weight)

	data, err := s.retrieve(ctx, object)
	if err != nil {
		return nil, err
	}
	s.remember(object.Digest, data)
	if s.Cache != nil {
		_ = s.Cache.StoreObject(ctx, object, data)
	}
	return data, nil
}

// prefetchPoll is how often a prefetch looks for room in the download budget.
const prefetchPoll = 50 * time.Millisecond

// Prefetch puts an object in the cache ahead of any read of it, unless the
// cache has it already, and reports whether it downloaded it. It takes room
// in the download budget only while no read is waiting for some, and keeps
// nothing in memory.
func (s *CachedSource) Prefetch(ctx context.Context, object Object) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if s.Cache == nil || s.cached(ctx, object) {
		return false, nil
	}
	weight := min(int64(object.Size)+1, downloadBudgetBytes)
	for !downloadBudget.TryAcquire(weight) {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(prefetchPoll):
		}
	}
	defer downloadBudget.Release(weight)
	// A read may have fetched it in the meantime.
	if s.cached(ctx, object) {
		return false, nil
	}
	data, err := s.retrieve(ctx, object)
	if err != nil {
		return false, err
	}
	if err := s.Cache.StoreObject(ctx, object, data); err != nil {
		return false, err
	}
	return true, nil
}

// cached reports whether a read would find the object without downloading
// it.
func (s *CachedSource) cached(ctx context.Context, object Object) bool {
	s.mu.Lock()
	_, recent := s.recent[object.Digest]
	s.mu.Unlock()
	return recent || s.Cache.ReadObject(ctx, object, int64(object.Size)-1, make([]byte, 1))
}

// retrieve downloads an object and checks it, retrying failed attempts.
func (s *CachedSource) retrieve(ctx context.Context, object Object) ([]byte, error) {
	attempts := max(s.Attempts, 1)
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		data, err := s.download(attemptCtx, object)
		cancel()
		if err == nil {
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
