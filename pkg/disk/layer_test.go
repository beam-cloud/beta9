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
	for _, chunk := range layer.Chunks {
		if chunk.OffsetBytes >= 20*LayerChunkSize && chunk.OffsetBytes < 21*LayerChunkSize {
			t.Fatalf("all-zero chunk at %d was not skipped", chunk.OffsetBytes)
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
	if err := fetchLayer(context.Background(), store, layer, destPath); err != nil {
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
	if err := fetchLayer(context.Background(), store, layer, destPath); err == nil {
		t.Fatal("expected digest mismatch error")
	}
	if _, err := os.Stat(destPath); !os.IsNotExist(err) {
		t.Fatal("corrupt fetch must not leave a layer file behind")
	}
}
