package clients

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/stretchr/testify/require"
)

// rangeStore serves one object. Each request is counted under its Range
// header, "" for a request for the whole object, and answer may take it over.
type rangeStore struct {
	t        *testing.T
	body     []byte
	answer   func(w http.ResponseWriter, r *http.Request, from, to int, attempt int) bool
	mu       sync.Mutex
	requests map[string]int
	active   atomic.Int32
	most     atomic.Int32
}

func (s *rangeStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	now := s.active.Add(1)
	defer s.active.Add(-1)
	for most := s.most.Load(); now > most && !s.most.CompareAndSwap(most, now); most = s.most.Load() {
	}

	header := r.Header.Get("Range")
	s.mu.Lock()
	s.requests[header]++
	attempt := s.requests[header]
	s.mu.Unlock()

	from, to := 0, len(s.body)-1
	if header != "" {
		if _, err := fmt.Sscanf(header, "bytes=%d-%d", &from, &to); err != nil || from > to || to >= len(s.body) {
			s.t.Errorf("range %q of %d bytes", header, len(s.body))
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
	}
	if s.answer != nil && s.answer(w, r, from, to, attempt) {
		return
	}
	w.Header().Set("Content-Length", fmt.Sprint(to-from+1))
	if header != "" {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", from, to, len(s.body)))
		w.WriteHeader(http.StatusPartialContent)
	}
	_, _ = w.Write(s.body[from : to+1])
}

func (s *rangeStore) requested(header string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests[header]
}

func newRangeStore(t *testing.T, size int) (*rangeStore, *StorageClient) {
	t.Helper()
	store := &rangeStore{t: t, body: make([]byte, size), requests: map[string]int{}}
	rand.New(rand.NewSource(int64(size))).Read(store.body)
	server := httptest.NewServer(store)
	t.Cleanup(server.Close)
	cfg := aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("access", "secret", "")}
	return store, &StorageClient{s3Client: newS3Client(cfg, server.URL)}
}

const testPart = 64 * 1024

func TestDownloadRangesDeliversAnObjectInOrder(t *testing.T) {
	store, client := newRangeStore(t, 9*testPart+123)
	// The earlier a part, the later it arrives.
	store.answer = func(_ http.ResponseWriter, _ *http.Request, from, _ int, _ int) bool {
		time.Sleep(time.Duration(10-from/testPart) * 3 * time.Millisecond)
		return false
	}
	released := 0
	reader := client.downloadRanges(context.Background(), "key", "bucket", int64(len(store.body)), testPart, 4, func() { released++ })
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.True(t, bytes.Equal(store.body, got), "the object's bytes, in order")
	require.NoError(t, reader.Close())
	require.NoError(t, reader.Close())
	require.Equal(t, 1, released)
	require.Equal(t, int32(4), store.most.Load(), "as many requests at once as it has workers, and no more")
	require.Equal(t, 1, store.requested(fmt.Sprintf("bytes=%d-%d", 9*testPart, 9*testPart+122)), "the last part is the remainder")
	require.Zero(t, store.requested(""))
}

func TestDownloadRangesAsksAgainForAResponseThatBreaksOff(t *testing.T) {
	store, client := newRangeStore(t, 4*testPart)
	broken := fmt.Sprintf("bytes=%d-%d", 2*testPart, 3*testPart-1)
	breakOff := func(until int) {
		store.answer = func(w http.ResponseWriter, r *http.Request, from, to int, attempt int) bool {
			if r.Header.Get("Range") != broken || attempt > until {
				return false
			}
			w.Header().Set("Content-Length", fmt.Sprint(to-from+1))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(store.body[from : from+100])
			return true // The server closes a connection it owes bytes to.
		}
	}

	breakOff(storageDownloadPartAttempts - 1)
	reader := client.downloadRanges(context.Background(), "key", "bucket", int64(len(store.body)), testPart, 2, func() {})
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.True(t, bytes.Equal(store.body, got))
	require.NoError(t, reader.Close())
	require.Equal(t, storageDownloadPartAttempts, store.requested(broken))

	breakOff(1 << 30)
	reader = client.downloadRanges(context.Background(), "key", "bucket", int64(len(store.body)), testPart, 2, func() {})
	got, err = io.ReadAll(reader)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF, "the caller decides whether to start over")
	require.ErrorContains(t, err, fmt.Sprintf("read bytes %d-%d of key", 2*testPart, 3*testPart-1))
	require.True(t, bytes.Equal(store.body[:2*testPart], got), "nothing past the part that failed")
	_, again := reader.Read(make([]byte, 1))
	require.Equal(t, err, again)
	require.NoError(t, reader.Close())
	require.Equal(t, 2*storageDownloadPartAttempts, store.requested(broken))
}

func TestDownloadRangesFailsRatherThanMisplaceBytes(t *testing.T) {
	for name, answer := range map[string]func(w http.ResponseWriter, body []byte){
		"a store that ignores the range": func(w http.ResponseWriter, body []byte) {
			w.Header().Set("Content-Length", fmt.Sprint(len(body)))
			_, _ = w.Write(body)
		},
		"an object that is gone": func(w http.ResponseWriter, _ []byte) {
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, "<Error><Code>NoSuchKey</Code></Error>")
		},
	} {
		t.Run(name, func(t *testing.T) {
			store, client := newRangeStore(t, 3*testPart)
			store.answer = func(w http.ResponseWriter, _ *http.Request, from, _ int, _ int) bool {
				if from == 0 {
					return false
				}
				answer(w, store.body)
				return true
			}
			reader := client.downloadRanges(context.Background(), "key", "bucket", int64(len(store.body)), testPart, 3, func() {})
			got, err := io.ReadAll(reader)
			require.Error(t, err)
			require.True(t, bytes.Equal(store.body[:testPart], got))
			require.NoError(t, reader.Close())
			require.Equal(t, 1, store.requested(fmt.Sprintf("bytes=%d-%d", testPart, 2*testPart-1)), "not asked for again")
		})
	}
}

func TestDownloadRangesCloseEndsAWaitingRead(t *testing.T) {
	store, client := newRangeStore(t, 4*testPart)
	store.answer = func(_ http.ResponseWriter, r *http.Request, _, _ int, _ int) bool {
		<-r.Context().Done()
		return true
	}
	for name, end := range map[string]func(cancel context.CancelFunc, reader io.Closer){
		"close":  func(_ context.CancelFunc, reader io.Closer) { _ = reader.Close() },
		"cancel": func(cancel context.CancelFunc, _ io.Closer) { cancel() },
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			released := make(chan struct{})
			reader := client.downloadRanges(ctx, "key", "bucket", int64(len(store.body)), testPart, 2, func() { close(released) })
			read := make(chan error, 1)
			go func() {
				_, err := reader.Read(make([]byte, 1))
				read <- err
			}()
			time.Sleep(20 * time.Millisecond)
			end(cancel, reader)
			select {
			case err := <-read:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(5 * time.Second):
				t.Fatal("the read is still waiting")
			}
			require.NoError(t, reader.Close())
			<-released
		})
	}
}

func TestDownloadLargeWithReaderSharesItsRequests(t *testing.T) {
	read := func(t *testing.T, client *StorageClient, store *rangeStore) {
		t.Helper()
		reader, err := client.DownloadLargeWithReader(context.Background(), "key", "bucket", int64(len(store.body)))
		require.NoError(t, err)
		got, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.True(t, bytes.Equal(store.body, got))
		require.NoError(t, reader.Close())
		require.Empty(t, storageDownloadSlots, "requests are handed back")
	}

	t.Run("an object of one part is one response", func(t *testing.T) {
		store, client := newRangeStore(t, storageDownloadPartSize)
		read(t, client, store)
		require.Equal(t, map[string]int{"": 1}, store.requests)
	})
	t.Run("a larger one is read in parts", func(t *testing.T) {
		store, client := newRangeStore(t, 2*storageDownloadPartSize+1)
		read(t, client, store)
		require.Len(t, store.requests, 3)
		require.Zero(t, store.requested(""))
	})
	t.Run("unless every spare request is taken", func(t *testing.T) {
		store, client := newRangeStore(t, 2*storageDownloadPartSize+1)
		require.Equal(t, storageDownloadExtraWorkers, acquireStorageDownloadSlots(storageDownloadExtraWorkers))
		reader, err := client.DownloadLargeWithReader(context.Background(), "key", "bucket", int64(len(store.body)))
		for range storageDownloadExtraWorkers {
			<-storageDownloadSlots
		}
		require.NoError(t, err)
		got, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.True(t, bytes.Equal(store.body, got))
		require.NoError(t, reader.Close())
		require.Equal(t, map[string]int{"": 1}, store.requests)
	})
	t.Run("and with as many as are spare", func(t *testing.T) {
		store, client := newRangeStore(t, 6*storageDownloadPartSize)
		store.answer = func(_ http.ResponseWriter, _ *http.Request, _, _ int, _ int) bool {
			// A request is held until as many are in flight as may be,
			// however slowly the others are sent.
			for held := time.Now(); store.most.Load() < 3 && time.Since(held) < 10*time.Second; {
				time.Sleep(time.Millisecond)
			}
			return false
		}
		require.Equal(t, storageDownloadExtraWorkers-2, acquireStorageDownloadSlots(storageDownloadExtraWorkers-2))
		defer func() {
			for range storageDownloadExtraWorkers - 2 {
				<-storageDownloadSlots
			}
		}()
		reader, err := client.DownloadLargeWithReader(context.Background(), "key", "bucket", int64(len(store.body)))
		require.NoError(t, err)
		require.Len(t, storageDownloadSlots, storageDownloadExtraWorkers)
		got, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.True(t, bytes.Equal(store.body, got))
		require.NoError(t, reader.Close())
		require.Len(t, storageDownloadSlots, storageDownloadExtraWorkers-2)
		require.Equal(t, int32(3), store.most.Load())
		require.Zero(t, store.requested(""))
	})
}
