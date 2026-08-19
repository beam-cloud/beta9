package disk

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
)

type memoryChunkStore struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newMemoryChunkStore() *memoryChunkStore {
	return &memoryChunkStore{objects: make(map[string][]byte)}
}

func (s *memoryChunkStore) WriteChunk(_ context.Context, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = append([]byte(nil), data...)
	return nil
}

func (s *memoryChunkStore) ReadChunk(_ context.Context, chunk types.DiskSnapshotChunk, dest []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.objects[chunk.ObjectKey]
	if !ok {
		return fmt.Errorf("missing chunk %s", chunk.ObjectKey)
	}
	copy(dest, data)
	return nil
}

func chunkKey(digest string) string {
	return "chunks/" + digest
}

func TestLayerScanUploadFetchRoundTrip(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "layer.qcow2")

	// Sparse layout: data at the start, a large hole, data in the middle of a
	// chunk boundary, an explicit all-zero chunk, then a tail.
	file, err := os.Create(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	head := make([]byte, 3000)
	middle := make([]byte, LayerChunkSize/2)
	tail := make([]byte, 12345)
	rand.Read(head)
	rand.Read(middle)
	rand.Read(tail)
	writeAt := func(data []byte, offset int64) {
		if _, err := file.WriteAt(data, offset); err != nil {
			t.Fatal(err)
		}
	}
	writeAt(head, 0)
	writeAt(middle, 10*LayerChunkSize+512)
	writeAt(make([]byte, LayerChunkSize), 20*LayerChunkSize) // explicit zeros
	writeAt(tail, 30*LayerChunkSize)
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	layer, err := ScanLayer(sourcePath, chunkKey)
	if err != nil {
		t.Fatal(err)
	}
	if len(layer.Chunks) == 0 {
		t.Fatal("expected chunks")
	}
	for i, chunk := range layer.Chunks {
		if chunk.OffsetBytes >= 20*LayerChunkSize && chunk.OffsetBytes < 21*LayerChunkSize {
			t.Fatalf("all-zero chunk at %d was not skipped", chunk.OffsetBytes)
		}
		// The parallel scan must keep chunks offset-ordered and densely indexed.
		if chunk.Index != int64(i) {
			t.Fatalf("chunk %d has index %d", i, chunk.Index)
		}
		if i > 0 && chunk.OffsetBytes <= layer.Chunks[i-1].OffsetBytes {
			t.Fatalf("chunks out of order at %d", i)
		}
	}

	store := newMemoryChunkStore()
	if err := UploadLayer(context.Background(), store, sourcePath, layer); err != nil {
		t.Fatal(err)
	}
	if len(store.objects) != len(layer.Chunks) {
		t.Fatalf("uploaded %d objects, expected %d", len(store.objects), len(layer.Chunks))
	}

	destPath := filepath.Join(dir, "restored.qcow2")
	if err := fetchLayer(context.Background(), store, layer, destPath, nil); err != nil {
		t.Fatal(err)
	}

	source, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(source, restored) {
		t.Fatal("restored layer differs from source")
	}
}

// Content-defined boundaries must survive shifts: a flatten relocates
// unchanged disk content within the qcow2 file, and publish dedup relies on
// those bytes keeping their chunk hashes.
func TestScanLayerChunksSurviveContentShift(t *testing.T) {
	dir := t.TempDir()
	base := make([]byte, 64<<20)
	rand.Read(base)
	pathA := filepath.Join(dir, "a.qcow2")
	if err := os.WriteFile(pathA, base, 0o600); err != nil {
		t.Fatal(err)
	}

	// Insert 64 KiB at 8 MiB, shifting everything after it.
	inserted := make([]byte, 64<<10)
	rand.Read(inserted)
	shifted := append(append(append([]byte{}, base[:8<<20]...), inserted...), base[8<<20:]...)
	pathB := filepath.Join(dir, "b.qcow2")
	if err := os.WriteFile(pathB, shifted, 0o600); err != nil {
		t.Fatal(err)
	}

	layerA, err := ScanLayer(pathA, chunkKey)
	if err != nil {
		t.Fatal(err)
	}
	layerB, err := ScanLayer(pathB, chunkKey)
	if err != nil {
		t.Fatal(err)
	}

	digestsA := map[string]bool{}
	for _, chunk := range layerA.Chunks {
		digestsA[chunk.Digest] = true
	}
	var sharedBytes, totalBytes int64
	for _, chunk := range layerB.Chunks {
		totalBytes += chunk.SizeBytes
		if digestsA[chunk.Digest] {
			sharedBytes += chunk.SizeBytes
		}
	}
	if sharedBytes < totalBytes*3/4 {
		t.Fatalf("only %d of %d bytes dedup after a 64KiB shift", sharedBytes, totalBytes)
	}

	// Chunk sizes must respect the configured bounds.
	for _, chunk := range layerB.Chunks {
		if chunk.SizeBytes > chunkMaxSize {
			t.Fatalf("chunk of %d bytes exceeds the max", chunk.SizeBytes)
		}
	}
}

// blockingChunkStore records peak concurrent ReadChunk calls.
type blockingChunkStore struct {
	*memoryChunkStore
	mu       sync.Mutex
	inflight int
	peak     int
}

func (s *blockingChunkStore) ReadChunk(ctx context.Context, chunk types.DiskSnapshotChunk, dest []byte) error {
	s.mu.Lock()
	s.inflight++
	if s.inflight > s.peak {
		s.peak = s.inflight
	}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.inflight--
		s.mu.Unlock()
	}()
	return s.memoryChunkStore.ReadChunk(ctx, chunk, dest)
}

func TestFetchLayerSharesChunkGateAcrossLayers(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "layer.qcow2")
	data := make([]byte, 40*LayerChunkSize)
	rand.Read(data)
	if err := os.WriteFile(sourcePath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	layer, err := ScanLayer(sourcePath, chunkKey)
	if err != nil {
		t.Fatal(err)
	}
	if len(layer.Chunks) < 20 {
		t.Fatalf("expected a multi-chunk layer, got %d", len(layer.Chunks))
	}

	store := &blockingChunkStore{memoryChunkStore: newMemoryChunkStore()}
	if err := UploadLayer(context.Background(), store, sourcePath, layer); err != nil {
		t.Fatal(err)
	}

	// A single layer must saturate the whole shared budget, not a per-layer slice.
	gate := newChunkGate(chunkFetchConcurrency)
	if err := fetchLayer(context.Background(), store, layer, filepath.Join(dir, "a.qcow2"), gate); err != nil {
		t.Fatal(err)
	}
	if store.peak > chunkFetchConcurrency {
		t.Fatalf("peak concurrency %d exceeded budget %d", store.peak, chunkFetchConcurrency)
	}

	// Two layers sharing one gate stay within the same budget.
	store.peak = 0
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = fetchLayer(context.Background(), store, layer, filepath.Join(dir, fmt.Sprintf("b%d.qcow2", i)), gate)
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if store.peak > chunkFetchConcurrency {
		t.Fatalf("shared gate exceeded budget: peak %d", store.peak)
	}
}

func TestFetchLayerRejectsCorruptChunks(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "layer.qcow2")
	data := make([]byte, 5000)
	rand.Read(data)
	if err := os.WriteFile(sourcePath, data, 0o600); err != nil {
		t.Fatal(err)
	}

	layer, err := ScanLayer(sourcePath, chunkKey)
	if err != nil {
		t.Fatal(err)
	}
	store := newMemoryChunkStore()
	if err := UploadLayer(context.Background(), store, sourcePath, layer); err != nil {
		t.Fatal(err)
	}
	for key := range store.objects {
		store.objects[key][0] ^= 0xff
	}

	destPath := filepath.Join(dir, "restored.qcow2")
	if err := fetchLayer(context.Background(), store, layer, destPath, nil); err == nil {
		t.Fatal("expected digest mismatch error")
	}
	if _, err := os.Stat(destPath); !os.IsNotExist(err) {
		t.Fatal("corrupt fetch must not leave a layer file behind")
	}
}
