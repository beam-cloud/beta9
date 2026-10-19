package disk

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOfflineFormatFailurePreservesHeadAndCanBeRepeated(t *testing.T) {
	for _, failed := range []string{"mkfs.ext4", "qemu-img"} {
		t.Run(failed, func(t *testing.T) {
			dir := t.TempDir()
			head := filepath.Join(dir, "head.qcow2")
			require.NoError(t, os.WriteFile(head, []byte("original"), 0o600))
			failure := errors.New("interrupted format")
			fail := true
			m := NewManager(Config{Runner: func(_ context.Context, name string, args ...string) ([]byte, error) {
				switch name {
				case "mkfs.ext4":
					info, err := os.Stat(args[len(args)-1])
					require.NoError(t, err)
					require.Equal(t, int64(50<<30), info.Size())
				case "qemu-img":
					require.Equal(t, "convert", args[0])
					require.NoError(t, os.WriteFile(args[len(args)-1], []byte("formatted"), 0o600))
				default:
					t.Fatalf("offline format called %s", name)
				}
				if fail && name == failed {
					return nil, failure
				}
				return nil, nil
			}})
			require.ErrorIs(t, m.formatQcowBase(context.Background(), head, 50<<30), failure)
			data, err := os.ReadFile(head)
			require.NoError(t, err)
			require.Equal(t, "original", string(data))
			fail = false
			require.NoError(t, m.formatQcowBase(context.Background(), head, 50<<30))
			data, err = os.ReadFile(head)
			require.NoError(t, err)
			require.Equal(t, "formatted", string(data))
			entries, err := os.ReadDir(dir)
			require.NoError(t, err)
			require.Len(t, entries, 1, "temporary images must be removed")
		})
	}
}

func TestFormatQcowBaseOffline(t *testing.T) {
	for _, name := range []string{"qemu-img", "mkfs.ext4", "e2fsck"} {
		if _, err := exec.LookPath(name); err != nil {
			t.Skip(name + " is not installed")
		}
	}
	ctx := context.Background()
	dir := t.TempDir()
	m := NewManager(Config{Root: dir})
	head := filepath.Join(dir, "head.qcow2")
	require.NoError(t, m.formatQcowBase(ctx, head, 128<<20))
	info, err := m.queryQcowInfo(ctx, head)
	require.NoError(t, err)
	require.Equal(t, int64(128<<20), info.VirtualSize)
	raw := filepath.Join(dir, "check.raw")
	_, err = execRunner(ctx, "qemu-img", "convert", "-q", "-f", "qcow2", "-O", "raw", head, raw)
	require.NoError(t, err)
	_, err = execRunner(ctx, "e2fsck", "-fn", raw)
	require.NoError(t, err)
}

func TestCanonicalBaseKeepsChunksAcrossFlatten(t *testing.T) {
	for _, name := range []string{"qemu-img", "qemu-io"} {
		if _, err := exec.LookPath(name); err != nil {
			t.Skip(name + " is not installed")
		}
	}
	ctx := context.Background()
	dir := t.TempDir()
	m := NewManager(Config{Root: dir})
	base := filepath.Join(dir, "base.qcow2")
	require.NoError(t, m.createQcowBase(ctx, base, 128<<20))
	// Reproduce a live daemon's allocation order, which is unrelated to the
	// logical order qemu-img uses when it later flattens the published chain.
	rng := rand.NewChaCha8([32]byte{'c', 'a', 'n', 'o', 'n', 'i', 'c', 'a', 'l'})
	data := make([]byte, 8<<20)
	for i := 7; i >= 0; i-- {
		rng.Read(data)
		source := filepath.Join(dir, "data")
		require.NoError(t, os.WriteFile(source, data, 0o600))
		_, err := execRunner(ctx, "qemu-io", "-f", "qcow2", "-c", fmt.Sprintf("write -s %s %d %d", source, i*len(data), len(data)), base)
		require.NoError(t, err)
	}
	canonical := filepath.Join(dir, "canonical.qcow2")
	require.NoError(t, m.flattenQcow(ctx, base, canonical))
	first, err := ScanLayer(ctx, canonical, chunkKey)
	require.NoError(t, err)

	// The live chain still uses the original allocation; publishing a
	// canonical artifact must not alter its backing files.
	overlay := filepath.Join(dir, "head.qcow2")
	require.NoError(t, m.createQcowOverlay(ctx, overlay, base, 128<<20))
	_, err = execRunner(ctx, "qemu-io", "-f", "qcow2", "-c", "write -P 0x41 100M 4k", overlay)
	require.NoError(t, err)
	flat := filepath.Join(dir, "flat.qcow2")
	require.NoError(t, m.flattenQcow(ctx, overlay, flat))
	second, err := ScanLayer(ctx, flat, chunkKey)
	require.NoError(t, err)
	known := make(map[string]bool)
	for _, chunk := range first.Chunks {
		known[chunk.Digest] = true
	}
	var reused int64
	for _, chunk := range second.Chunks {
		if known[chunk.Digest] {
			reused += chunk.SizeBytes
		}
	}
	require.Greater(t, reused, int64(48<<20), "flatten must retain most of the unchanged 64 MiB")
}

func TestFlattenRetainsChunksWhenSparseGapsAreWritten(t *testing.T) {
	for _, name := range []string{"qemu-img", "qemu-io"} {
		if _, err := exec.LookPath(name); err != nil {
			t.Skip(name + " is not installed")
		}
	}
	ctx := context.Background()
	dir := t.TempDir()
	m := NewManager(Config{Root: dir})
	base := filepath.Join(dir, "base.qcow2")
	require.NoError(t, m.createQcowBase(ctx, base, 50<<30))
	rng := rand.NewChaCha8([32]byte{'s', 'p', 'a', 'r', 's', 'e'})
	data := make([]byte, 8<<20)
	source := filepath.Join(dir, "data")
	for i := range 16 {
		rng.Read(data)
		require.NoError(t, os.WriteFile(source, data, 0o600))
		_, err := execRunner(ctx, "qemu-io", "-f", "qcow2", "-c", fmt.Sprintf("write -s %s %d %d", source, i*(128<<20), len(data)), base)
		require.NoError(t, err)
	}
	canonical := filepath.Join(dir, "canonical.qcow2")
	require.NoError(t, m.flattenQcow(ctx, base, canonical))
	first, err := ScanLayer(ctx, canonical, chunkKey)
	require.NoError(t, err)
	overlay := filepath.Join(dir, "head.qcow2")
	require.NoError(t, m.createQcowOverlay(ctx, overlay, base, 50<<30))
	for i := range 16 {
		_, err := execRunner(ctx, "qemu-io", "-f", "qcow2", "-c", fmt.Sprintf("write -P 0x41 %d 4k", i*(128<<20)+(64<<20)), overlay)
		require.NoError(t, err)
	}
	flat := filepath.Join(dir, "flat.qcow2")
	require.NoError(t, m.flattenQcow(ctx, overlay, flat))
	second, err := ScanLayer(ctx, flat, chunkKey)
	require.NoError(t, err)
	known := make(map[string]bool)
	for _, chunk := range first.Chunks {
		known[chunk.Digest] = true
	}
	var uploaded int64
	for _, chunk := range second.Chunks {
		if !known[chunk.Digest] {
			uploaded += chunk.SizeBytes
		}
	}
	t.Logf("flatten after 64 KiB of gap writes uploads %d of %d bytes", uploaded, StoredBytes(second))
	require.Less(t, uploaded, int64(8<<20), "small sparse writes must not re-upload unchanged extents")
	require.Less(t, StoredBytes(second), int64(160<<20), "metadata allocation must leave unwritten data sparse")
	store := newMemoryChunkStore()
	require.NoError(t, UploadLayer(ctx, store, flat, second))
	restored := filepath.Join(dir, "restored.qcow2")
	require.NoError(t, fetchLayer(ctx, store, second, restored, nil))
	_, err = execRunner(ctx, "qemu-img", "compare", "-q", overlay, restored)
	require.NoError(t, err, "published image must contain the identical virtual disk")
}
