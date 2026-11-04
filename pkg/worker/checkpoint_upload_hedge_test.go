package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/stretchr/testify/require"
)

func checkpointHedgeTestFile(t *testing.T) (*os.File, []byte) {
	t.Helper()
	data := bytes.Repeat([]byte("immutable-checkpoint\x00\x17"), 4096)
	path := filepath.Join(t.TempDir(), "archive")
	require.NoError(t, os.WriteFile(path, data, 0600))
	f, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	return f, data
}

func TestCheckpointOriginHedgeUsesIdenticalBytesAfterRealRequestWrite(t *testing.T) {
	f, payload := checkpointHedgeTestFile(t)
	var mu sync.Mutex
	var hashes [][32]byte
	releasePrimary := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		mu.Lock()
		hashes = append(hashes, sha256.Sum256(data))
		attempt := len(hashes)
		mu.Unlock()
		if attempt == 1 {
			select {
			case <-releasePrimary:
			case <-r.Context().Done():
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	defer close(releasePrimary)
	upload := func(ctx context.Context, reader io.Reader) error {
		request, err := http.NewRequestWithContext(ctx, http.MethodPut, server.URL, reader)
		if err != nil {
			return err
		}
		request.ContentLength = int64(len(payload))
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		_, err = io.Copy(io.Discard, response.Body)
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	slots := make(chan struct{}, 1)
	require.NoError(t, uploadCheckpointArchiveWithHedge(ctx, f, f, int64(len(payload)), 15*time.Millisecond, slots, upload))
	require.Empty(t, slots)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, [][32]byte{sha256.Sum256(payload), sha256.Sum256(payload)}, hashes)
}

func TestCheckpointOriginHedgeWaitsForRequestWriteAndJoinsCancellation(t *testing.T) {
	f, payload := checkpointHedgeTestFile(t)
	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int32
	entered := make(chan struct{})
	finished := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- uploadCheckpointArchiveWithHedge(ctx, f, f, int64(len(payload)), time.Millisecond, make(chan struct{}, 1), func(ctx context.Context, _ io.Reader) error {
			calls.Add(1)
			close(entered)
			defer close(finished)
			<-ctx.Done() // No WroteRequest: bandwidth-bound bodies must not be duplicated.
			return ctx.Err()
		})
	}()
	<-entered
	time.Sleep(15 * time.Millisecond)
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
	require.EqualValues(t, 1, calls.Load())
	select {
	case <-finished:
	default:
		t.Fatal("cancellation returned while the reader was still active")
	}
}

func TestCheckpointOriginHedgeRequiresAnAcknowledgementAndJoinsLoser(t *testing.T) {
	for _, primaryWins := range []bool{false, true} {
		t.Run(map[bool]string{false: "both_fail", true: "primary_wins"}[primaryWins], func(t *testing.T) {
			f, payload := checkpointHedgeTestFile(t)
			first, second := errors.New("primary failure"), errors.New("hedge failure")
			hedgeStarted, hedgeFinished := make(chan struct{}), make(chan struct{})
			err := uploadCheckpointArchiveWithHedge(context.Background(), f, f, int64(len(payload)), time.Millisecond, make(chan struct{}, 1), func(ctx context.Context, _ io.Reader) error {
				if trace := httptrace.ContextClientTrace(ctx); trace != nil {
					trace.WroteRequest(httptrace.WroteRequestInfo{})
					<-hedgeStarted
					if primaryWins {
						return nil
					}
					return first
				}
				close(hedgeStarted)
				defer close(hedgeFinished)
				if primaryWins {
					<-ctx.Done()
					return ctx.Err()
				}
				return second
			})
			if primaryWins {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, first)
				require.ErrorIs(t, err, second)
			}
			select {
			case <-hedgeFinished:
			default:
				t.Fatal("upload returned before the losing reader finished")
			}
		})
	}
}

func TestCheckpointOriginHedgeGlobalBudgetAndCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slots := make(chan struct{}, 1)
	started := make(chan struct{}, 8)
	var active, peak atomic.Int32
	var wg sync.WaitGroup
	results := make(chan error, 6)
	for i := 0; i < 6; i++ {
		f, data := checkpointHedgeTestFile(t)
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- uploadCheckpointArchiveWithHedge(ctx, f, f, int64(len(data)), time.Millisecond, slots, func(ctx context.Context, _ io.Reader) error {
				if trace := httptrace.ContextClientTrace(ctx); trace != nil {
					trace.WroteRequest(httptrace.WroteRequestInfo{})
				} else {
					n := active.Add(1)
					for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
					}
					defer active.Add(-1)
					started <- struct{}{}
				}
				<-ctx.Done()
				return ctx.Err()
			})
		}()
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("hedge did not start")
	}
	time.Sleep(15 * time.Millisecond)
	require.EqualValues(t, 1, active.Load())
	cancel()
	wg.Wait()
	close(results)
	for err := range results {
		require.ErrorIs(t, err, context.Canceled)
	}
	require.EqualValues(t, 1, peak.Load())
	require.Zero(t, active.Load())
	require.Empty(t, slots)
}

func TestCheckpointOriginHedgePolicyIsBoundedAndSinglePutOnly(t *testing.T) {
	for _, value := range []string{"", "bad", "0s", "999ms", "5001ms"} {
		t.Setenv("BETA9_CHECKPOINT_ORIGIN_HEDGE_DELAY", value)
		require.Zero(t, checkpointOriginHedgeDelay())
	}
	t.Setenv("BETA9_CHECKPOINT_ORIGIN_HEDGE_DELAY", "1250ms")
	require.Equal(t, 1250*time.Millisecond, checkpointOriginHedgeDelay())
	require.True(t, checkpointOriginHedgeEligible(40<<20, clients.StorageUploadOptions{}))
	require.False(t, checkpointOriginHedgeEligible(64<<20, clients.StorageUploadOptions{}))
	require.False(t, checkpointOriginHedgeEligible(40<<20, clients.StorageUploadOptions{PartSize: 8 << 20}))
	require.False(t, checkpointOriginHedgeEligible(40<<20, clients.StorageUploadOptions{StreamingConcurrency: 2}))
}
