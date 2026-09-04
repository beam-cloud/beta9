package gatewayservices

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// memSpliceStore is an in-memory object store that enforces the S3 multipart
// rules the splice has to respect: every part except the last is at least
// minPart bytes, and copy ranges must lie inside the source object.
type memSpliceStore struct {
	mu        sync.Mutex
	objects   map[string][]byte
	metadata  map[string]map[string]string
	uploads   map[string]*memUpload
	minPart   int64
	copied    int64 // bytes moved server-side
	streamed  int64 // bytes read or written through the caller
	aborted   int
	failParts bool
}

type memUpload struct {
	key      string
	metadata map[string]string
	parts    map[int32][]byte
}

func newMemSpliceStore(minPart int64) *memSpliceStore {
	return &memSpliceStore{objects: map[string][]byte{}, metadata: map[string]map[string]string{}, uploads: map[string]*memUpload{}, minPart: minPart}
}

func (m *memSpliceStore) ObjectSize(_ context.Context, key string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[key]
	if !ok {
		return 0, fmt.Errorf("no such object %q", key)
	}
	return int64(len(data)), nil
}

func (m *memSpliceStore) ReadRange(_ context.Context, key string, offset, length int64) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[key]
	if !ok {
		return nil, fmt.Errorf("no such object %q", key)
	}
	if offset < 0 || offset+length > int64(len(data)) {
		return nil, fmt.Errorf("range %d+%d outside %q (%d bytes)", offset, length, key, len(data))
	}
	m.streamed += length
	return append([]byte(nil), data[offset:offset+length]...), nil
}

func (m *memSpliceStore) BeginUpload(_ context.Context, key string, metadata map[string]string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := fmt.Sprintf("upload-%d", len(m.uploads)+1)
	m.uploads[id] = &memUpload{key: key, metadata: metadata, parts: map[int32][]byte{}}
	return id, nil
}

func (m *memSpliceStore) CopyPart(_ context.Context, key, uploadID string, partNumber int32, sourceKey string, offset, length int64) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failParts {
		return "", fmt.Errorf("injected copy failure")
	}
	src, ok := m.objects[sourceKey]
	if !ok || offset < 0 || offset+length > int64(len(src)) {
		return "", fmt.Errorf("copy range %d+%d outside %q", offset, length, sourceKey)
	}
	m.uploads[uploadID].parts[partNumber] = append([]byte(nil), src[offset:offset+length]...)
	m.copied += length
	return fmt.Sprintf("etag-%d", partNumber), nil
}

func (m *memSpliceStore) PutPart(_ context.Context, key, uploadID string, partNumber int32, data []byte) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.uploads[uploadID].parts[partNumber] = append([]byte(nil), data...)
	m.streamed += int64(len(data))
	return fmt.Sprintf("etag-%d", partNumber), nil
}

func (m *memSpliceStore) CompleteUpload(_ context.Context, key, uploadID string, etags []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	up := m.uploads[uploadID]
	var out []byte
	for i := range etags {
		part, ok := up.parts[int32(i+1)]
		if !ok {
			return fmt.Errorf("missing part %d", i+1)
		}
		if i+1 < len(etags) && int64(len(part)) < m.minPart {
			return fmt.Errorf("part %d is %d bytes, below the %d minimum for non-final parts", i+1, len(part), m.minPart)
		}
		out = append(out, part...)
	}
	m.objects[key] = out
	m.metadata[key] = up.metadata
	delete(m.uploads, uploadID)
	return nil
}

func (m *memSpliceStore) AbortUpload(_ context.Context, key, uploadID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.aborted++
	delete(m.uploads, uploadID)
	return nil
}

func zipBytes(t *testing.T, entries []struct{ name, content string }, method uint16) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	for _, e := range entries {
		hdr := &zip.FileHeader{Name: e.name, Method: method}
		hdr.SetMode(0o644)
		fw, err := w.CreateHeader(hdr)
		require.NoError(t, err)
		_, err = fw.Write([]byte(e.content))
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	return buf.Bytes()
}

func TestSpliceZipObjectsReplacesAddsAndRemovesWithFewStreamedBytes(t *testing.T) {
	// Large enough that the 64 KiB end-of-archive reads used to find the
	// central directory are a small fraction of the archive.
	const minPart = 64 << 10
	big := strings.Repeat("x", 3*minPart)
	base := zipBytes(t, []struct{ name, content string }{
		{"app/a.py", "a v1"},
		{"app/big1.bin", big},
		{"app/b.py", "b v1"},
		{"app/big2.bin", big},
		{"app/gone.py", "gone"},
		{"app/big3.bin", big},
		{"README", "readme"},
	}, zip.Store)
	delta := zipBytes(t, []struct{ name, content string }{
		{"app/a.py", "a v2"},
		{"app/new.py", "new"},
	}, zip.Deflate)

	store := newMemSpliceStore(minPart)
	store.objects["objects/base"] = base
	store.objects["objects/delta"] = delta

	meta := map[string]string{"hash": "abc"}
	size, err := spliceZipObjects(context.Background(), store, "objects/base", "objects/delta", "objects/merged", []string{"app/gone.py"}, meta, minPart)
	require.NoError(t, err)

	merged := store.objects["objects/merged"]
	require.Equal(t, int64(len(merged)), size)
	require.Equal(t, meta, store.metadata["objects/merged"])
	require.Equal(t, map[string]string{
		"app/a.py":     "a v2",
		"app/big1.bin": big,
		"app/b.py":     "b v1",
		"app/big2.bin": big,
		"app/big3.bin": big,
		"app/new.py":   "new",
		"README":       "readme",
	}, readZip(t, merged))

	r, err := zip.NewReader(bytes.NewReader(merged), int64(len(merged)))
	require.NoError(t, err)
	for _, f := range r.File {
		require.Equal(t, os.FileMode(0o644), f.Mode().Perm(), f.Name)
		if f.Name == "app/a.py" || f.Name == "app/new.py" {
			require.Equal(t, zip.Deflate, f.Method, f.Name)
		} else {
			require.Equal(t, zip.Store, f.Method, f.Name)
		}
	}

	// The bulk of the archive was copied server-side; only the small entries,
	// borrowed heads and the central directory moved through the caller.
	require.Greater(t, store.copied, int64(len(base))/2)
	require.Less(t, store.streamed, int64(len(base))/2)
	require.Zero(t, store.aborted)
	require.Empty(t, store.uploads)
}

func TestSpliceZipObjectsMatchesLocalMergeAcrossLayouts(t *testing.T) {
	// Various sizes around the part minimum so every branch of the part
	// grouping (borrow, small tail, split copy, literal-only) is exercised.
	for _, minPart := range []int64{64, 1000, 5000, 1 << 20} {
		for _, n := range []int{1, 2, 3, 7, 40} {
			t.Run(fmt.Sprintf("minPart=%d/entries=%d", minPart, n), func(t *testing.T) {
				var baseEntries, deltaEntries []struct{ name, content string }
				for i := 0; i < n; i++ {
					baseEntries = append(baseEntries, struct{ name, content string }{fmt.Sprintf("f%03d", i), strings.Repeat(string(rune('a'+i%26)), (i*577)%int(3*minPart+1))})
				}
				deltaEntries = append(deltaEntries, struct{ name, content string }{"f000", "replaced"})
				if n > 2 {
					deltaEntries = append(deltaEntries, struct{ name, content string }{fmt.Sprintf("f%03d", n/2), strings.Repeat("z", int(2*minPart))})
				}
				deltaEntries = append(deltaEntries, struct{ name, content string }{"added", "added"})
				removed := []string{fmt.Sprintf("f%03d", n-1)}

				base := zipBytes(t, baseEntries, zip.Deflate)
				delta := zipBytes(t, deltaEntries, zip.Store)

				store := newMemSpliceStore(minPart)
				store.objects["b"] = base
				store.objects["d"] = delta
				size, err := spliceZipObjects(context.Background(), store, "b", "d", "m", removed, nil, minPart)
				require.NoError(t, err)
				require.Equal(t, int64(len(store.objects["m"])), size)

				var local bytes.Buffer
				_, err = mergeZipArchives(bytesFile(t, base), bytesFile(t, delta), removed, &local)
				require.NoError(t, err)
				require.Equal(t, readZip(t, local.Bytes()), readZip(t, store.objects["m"]))
			})
		}
	}
}

func TestSpliceZipObjectsAbortsUploadOnFailure(t *testing.T) {
	const minPart = 1024
	base := zipBytes(t, []struct{ name, content string }{{"a", strings.Repeat("a", 4*minPart)}, {"b", "b"}}, zip.Store)
	delta := zipBytes(t, []struct{ name, content string }{{"b", "b2"}}, zip.Store)
	store := newMemSpliceStore(minPart)
	store.objects["b"] = base
	store.objects["d"] = delta
	store.failParts = true

	_, err := spliceZipObjects(context.Background(), store, "b", "d", "m", nil, nil, minPart)
	require.Error(t, err)
	require.Equal(t, 1, store.aborted)
	require.Empty(t, store.uploads)
	require.NotContains(t, store.objects, "m")
}

func TestParseZipDirectoryRejectsNonZip(t *testing.T) {
	data := []byte("definitely not a zip archive, but long enough to look for an end record in")
	_, err := parseZipDirectory(bytes.NewReader(data), int64(len(data)))
	require.ErrorIs(t, err, errZipSpliceUnsupported)
}

func TestSpliceZipObjectsWithPythonZipfile(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	dir := t.TempDir()
	script := `
import sys, zipfile
base, delta = sys.argv[1], sys.argv[2]
with zipfile.ZipFile(base, "w", zipfile.ZIP_DEFLATED) as z:
    for i in range(50):
        z.writestr(f"app/f{i:04d}.bin", bytes([i]) * 20000)
    z.writestr("app/main.py", "print('v1')")
with zipfile.ZipFile(delta, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("app/main.py", "print('v2')")
    z.writestr("app/f0010.bin", b"\xff" * 20000)
`
	outb, err := exec.Command(python, "-c", script, filepath.Join(dir, "base.zip"), filepath.Join(dir, "delta.zip")).CombinedOutput()
	require.NoError(t, err, string(outb))
	base, err := os.ReadFile(filepath.Join(dir, "base.zip"))
	require.NoError(t, err)
	delta, err := os.ReadFile(filepath.Join(dir, "delta.zip"))
	require.NoError(t, err)

	const minPart = 5 << 12
	store := newMemSpliceStore(minPart)
	store.objects["b"] = base
	store.objects["d"] = delta
	_, err = spliceZipObjects(context.Background(), store, "b", "d", "m", []string{"app/f0049.bin"}, nil, minPart)
	require.NoError(t, err)

	mergedPath := filepath.Join(dir, "merged.zip")
	require.NoError(t, os.WriteFile(mergedPath, store.objects["m"], 0o644))
	check := exec.Command(python, "-c", `
import sys, zipfile
z = zipfile.ZipFile(sys.argv[1]); assert z.testzip() is None, z.testzip()
names = z.namelist()
assert len(names) == 50, len(names)
assert "app/f0049.bin" not in names
assert z.read("app/main.py") == b"print('v2')"
assert z.read("app/f0010.bin") == b"\xff" * 20000
assert z.read("app/f0011.bin") == bytes([11]) * 20000
print("ok")`, mergedPath)
	outb, err = check.CombinedOutput()
	require.NoError(t, err, string(outb))
	require.Contains(t, string(outb), "ok")
}

func bytesFile(t *testing.T, data []byte) *os.File {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "zip")
	require.NoError(t, err)
	_, err = f.Write(data)
	require.NoError(t, err)
	_, err = f.Seek(0, 0)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	return f
}
