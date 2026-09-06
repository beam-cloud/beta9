package disk

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	mathrand "math/rand/v2"
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
//
// The gear hash's low chunkMeanMask bits depend on the last 22 bytes only,
// so a shifted stream re-finds its boundaries as soon as one boundary
// re-aligns. What delays that is the max-size clamp: with a 4 MiB mean and
// 8 MiB max, ~17% of chunks are cut at chunkMaxSize (exp(-7/4)), and a
// clamped cut is relative to the chunk start, not the content, so it
// carries a misalignment into the next chunk. Only the run of chunks from
// the insertion point to the first re-aligned boundary may differ; with
// unseeded data that run is geometric and ~8% of the time long enough to
// drop dedup below 3/4, so the input is seeded to keep the test stable.
func TestScanLayerChunksSurviveContentShift(t *testing.T) {
	dir := t.TempDir()
	rng := mathrand.NewChaCha8([32]byte{'l', 'a', 'y', 'e', 'r', '-', 's', 'h', 'i', 'f', 't'})
	base := make([]byte, 64<<20)
	rng.Read(base)
	pathA := filepath.Join(dir, "a.qcow2")
	if err := os.WriteFile(pathA, base, 0o600); err != nil {
		t.Fatal(err)
	}

	// Insert 64 KiB at 8 MiB, shifting everything after it.
	const insertAt = 8 << 20
	inserted := make([]byte, 64<<10)
	rng.Read(inserted)
	shifted := append(append(append([]byte{}, base[:insertAt]...), inserted...), base[insertAt:]...)
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
	// The chunks that miss must be one contiguous run starting at the chunk
	// holding the insertion: everything before it is untouched, and once a
	// boundary re-aligns every later boundary is content-defined from it.
	var sharedBytes, totalBytes int64
	firstMiss, lastMiss := -1, -1
	for i, chunk := range layerB.Chunks {
		totalBytes += chunk.SizeBytes
		if digestsA[chunk.Digest] {
			sharedBytes += chunk.SizeBytes
			continue
		}
		if firstMiss == -1 {
			firstMiss = i
		} else if i != lastMiss+1 {
			t.Fatalf("chunk %d at %d lost dedup after boundaries re-aligned at chunk %d", i, chunk.OffsetBytes, lastMiss+1)
		}
		lastMiss = i
	}
	if firstMiss == -1 {
		t.Fatal("the chunk holding the inserted bytes cannot dedup")
	}
	if first := layerB.Chunks[firstMiss]; first.OffsetBytes > insertAt || first.OffsetBytes+first.SizeBytes <= insertAt {
		t.Fatalf("chunk %d at %d lost dedup before the insertion at %d", firstMiss, first.OffsetBytes, insertAt)
	}
	// Each max-size clamp carries the shift one chunk further; with the seed
	// above the missing run is short (logged for reference).
	t.Logf("%d of %d bytes dedup after a 64KiB shift; %d chunks re-cut", sharedBytes, totalBytes, lastMiss-firstMiss+1)
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
