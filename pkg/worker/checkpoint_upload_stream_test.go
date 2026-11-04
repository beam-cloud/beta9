package worker

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStreamingCheckpointOverlapsProductionAndWaitsForOrigin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	root := t.TempDir()
	source, archive := filepath.Join(root, "saved"), filepath.Join(root, "private.tar")
	require.NoError(t, os.Mkdir(source, 0700))
	payload := make([]byte, 2<<20)
	_, err := rand.Read(payload)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(source, "memory"), payload, 0600))
	firstRead, ready, acknowledge := make(chan struct{}), make(chan struct{}), make(chan struct{})
	type result struct {
		hash string
		size int64
		err  error
	}
	done := make(chan result, 1)
	var uploaded []byte
	go func() {
		hash, size, err := streamCheckpointArchive(ctx, source, archive, func(int64) {
			// If the uploader waited for the complete archive this would deadlock.
			select {
			case <-firstRead:
			case <-ctx.Done():
			}
		}, func() { close(ready) }, func(ctx context.Context, reader io.Reader) error {
			first := make([]byte, 1)
			if _, err := io.ReadFull(reader, first); err != nil {
				return err
			}
			close(firstRead)
			rest, err := io.ReadAll(reader)
			if err != nil {
				return err
			}
			uploaded = append(first, rest...)
			select {
			case <-acknowledge:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		done <- result{hash, size, err}
	}()
	select {
	case <-ready:
	case <-ctx.Done():
		t.Fatal("archive did not finish while origin acknowledgement was withheld")
	}
	select {
	case <-done:
		t.Fatal("publication outran the origin acknowledgement")
	default:
	}
	close(acknowledge)
	r := <-done
	require.NoError(t, r.err)
	data, err := os.ReadFile(archive)
	require.NoError(t, err)
	require.Equal(t, data, uploaded)
	hash := sha256.Sum256(data)
	require.Equal(t, hex.EncodeToString(hash[:]), r.hash)
	require.EqualValues(t, len(data), r.size)
	dst := filepath.Join(root, "restored")
	require.NoError(t, os.Mkdir(dst, 0700))
	require.NoError(t, untarCheckpointReader(ctx, bytes.NewReader(data), dst))
	got, err := os.ReadFile(filepath.Join(dst, "saved", "memory"))
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestStreamingCheckpointProducerFailureReachesUploader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var readerErr error
	ready := false
	_, _, err := streamCheckpointArchive(ctx, filepath.Join(t.TempDir(), "missing"), filepath.Join(t.TempDir(), "private.tar"), nil, func() { ready = true }, func(_ context.Context, reader io.Reader) error {
		_, readerErr = io.Copy(io.Discard, reader)
		return readerErr
	})
	require.Error(t, err)
	require.Error(t, readerErr, "an incomplete producer must never look like successful EOF")
	require.False(t, ready)
	require.NoError(t, ctx.Err(), "producer and uploader must join without waiting for the deadline")
}

func TestStreamingCheckpointUploadFailureCancelsProducer(t *testing.T) {
	for _, prematureSuccess := range []bool{false, true} {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		source := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(source, "memory"), bytes.Repeat([]byte("data"), 1<<20), 0600))
		failure := errors.New("origin unavailable")
		_, _, err := streamCheckpointArchive(ctx, source, filepath.Join(t.TempDir(), "private.tar"), nil, nil, func(context.Context, io.Reader) error {
			if prematureSuccess {
				return nil
			}
			return failure
		})
		require.Error(t, err)
		if prematureSuccess {
			require.ErrorContains(t, err, "before archive EOF")
		} else {
			require.ErrorIs(t, err, failure)
		}
		require.NoError(t, ctx.Err())
		cancel()
	}
}

func TestStreamingCheckpointCancellationUnblocksTailReader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	file, err := os.CreateTemp(t.TempDir(), "tail")
	require.NoError(t, err)
	defer file.Close()
	tail := &checkpointArchiveTail{file: file, ctx: ctx, changed: make(chan struct{}, 1)}
	done := make(chan error, 1)
	go func() { _, err := tail.Read(make([]byte, 1)); done <- err }()
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("reader remained blocked after cancellation")
	}
}
