package mirror

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type zeroSource struct{}

func (zeroSource) ReadObject(_ context.Context, _ Object, _ int64, dest []byte) error {
	clear(dest)
	return nil
}

func heapMiB() float64 {
	runtime.GC()
	debug.FreeOSMemory()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return float64(stats.HeapAlloc) / (1 << 20)
}

// TestScale reports what a large generation costs the worker that mounts it:
//
//	sudo MIRROR_SCALE_FILES=1000000 go test ./pkg/mirror -run TestScale -v
//
// The tree is 24 files to a directory, four levels deep, packed 200 to an
// object. Contents are not read; the numbers are the index alone.
func TestScale(t *testing.T) {
	files, _ := strconv.Atoi(os.Getenv("MIRROR_SCALE_FILES"))
	if files == 0 {
		t.Skip("set MIRROR_SCALE_FILES")
	}

	builder := NewBuilder()
	made := map[string]bool{}
	mkdir := func(path string) {
		if !made[path] {
			made[path] = true
			require.NoError(t, builder.AddDir(path, 0o755, 1))
		}
	}
	var object Object
	for i := 0; i < files; i++ {
		if i%200 == 0 {
			var seed [8]byte
			binary.LittleEndian.PutUint64(seed[:], uint64(i))
			object = Object{Digest: sha256.Sum256(seed[:]), Size: 200 * 4096}
		}
		dir := i / 24
		repo := fmt.Sprintf("projects/repo-%03d", dir/2000)
		pkg := fmt.Sprintf("%s/pkg-%02d", repo, dir/100%20)
		module := fmt.Sprintf("%s/module-%02d", pkg, dir%100)
		for _, path := range []string{"projects", repo, pkg, module} {
			mkdir(path)
		}
		extent := FileExtent{Object: object, Offset: uint32(i%200) * 4096, StoredLen: 4096, RawLen: 4096}
		require.NoError(t, builder.AddFile(fmt.Sprintf("%s/source_file_%02d.go", module, i%24), 0o644, int64(i), []FileExtent{extent}))
	}
	started := time.Now()
	stored, built, err := builder.Encode(nil, false)
	require.NoError(t, err)
	encode := time.Since(started)
	entries, body := built.Entries(), built.IndexBytes()
	builder, built = nil, nil

	before := heapMiB()
	started = time.Now()
	manifest, err := Parse(stored)
	require.NoError(t, err)
	parse := time.Since(started)
	stored = stored[:len(stored):len(stored)]
	parsed := heapMiB() - before
	t.Logf("%d files, %d entries: manifest %.1f MiB stored, %.1f MiB decoded (%.0f bytes per entry); encode %s, parse and validate %s, heap after parse %.1f MiB",
		files, entries, float64(len(stored))/(1<<20), float64(body)/(1<<20), float64(body)/float64(entries), encode.Round(time.Millisecond), parse.Round(time.Millisecond), parsed)

	if _, err := os.Stat("/dev/fuse"); err != nil || os.Geteuid() != 0 {
		t.Log("not root or no /dev/fuse: skipping the mounted walk")
		return
	}
	fsys, err := NewFS(manifest, zeroSource{}, FSOptions{})
	require.NoError(t, err)
	mountPoint := t.TempDir()
	server, err := Mount(mountPoint, fsys)
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Unmount() })

	started = time.Now()
	walked := 0
	require.NoError(t, filepath.WalkDir(filepath.Join(mountPoint, "root"), func(_ string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		_, err = entry.Info()
		walked++
		return err
	}))
	require.Equal(t, int(entries), walked)
	t.Logf("walk and stat of every entry through the mount: %s; heap after the walk %.1f MiB", time.Since(started).Round(time.Millisecond), heapMiB()-before)
}
