package disk

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

type heldChunkSink struct {
	mu             sync.Mutex
	inflight, peak int
	entered        chan struct{}
	release        <-chan struct{}
}

func (s *heldChunkSink) WriteChunk(ctx context.Context, _ string, _ []byte) error {
	s.mu.Lock()
	s.inflight++
	s.peak = max(s.peak, s.inflight)
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.inflight--; s.mu.Unlock() }()
	s.entered <- struct{}{}
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestUploadLayerSharesCancellableBudgetAcrossVolumes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	sink := &heldChunkSink{entered: make(chan struct{}, 3*chunkUploadConcurrency), release: release}
	path := filepath.Join(t.TempDir(), "layer.qcow2")
	if err := os.WriteFile(path, make([]byte, uploadConcurrency), 0600); err != nil {
		t.Fatal(err)
	}
	layer := &types.DiskSnapshotFile{SizeBytes: int64(uploadConcurrency)}
	for i := range uploadConcurrency {
		layer.Chunks = append(layer.Chunks, types.DiskSnapshotChunk{OffsetBytes: int64(i), SizeBytes: 1})
	}
	volumes := (chunkUploadConcurrency + uploadConcurrency - 1) / uploadConcurrency
	results := make(chan error, volumes)
	for range volumes {
		go func() { results <- UploadLayer(ctx, sink, path, layer) }()
	}
	for range chunkUploadConcurrency {
		select {
		case <-sink.entered:
		case <-ctx.Done():
			t.Fatal("independent volumes did not fill the shared budget")
		}
	}
	// A canceled volume must leave without entering the sink or retaining a
	// buffer while another volume holds every upload slot.
	canceled, stop := context.WithCancel(ctx)
	stop()
	if err := UploadLayer(canceled, sink, path, layer); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled upload: %v", err)
	}
	sink.mu.Lock()
	peak := sink.peak
	sink.mu.Unlock()
	if peak > chunkUploadConcurrency {
		t.Fatalf("concurrent volumes exceeded the shared budget: %d", peak)
	}
	unblock()
	for range volumes {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if err := UploadLayer(ctx, sink, path, layer); err != nil {
		t.Fatalf("upload slots were not released: %v", err)
	}
}
