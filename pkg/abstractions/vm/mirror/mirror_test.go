package mirror

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

type memoryStore struct {
	mu      sync.Mutex
	objects map[Digest][]byte
	reads   atomic.Int64
}

func newMemoryStore() *memoryStore { return &memoryStore{objects: map[Digest][]byte{}} }

func (s *memoryStore) put(object Object, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[object.Digest] = bytes.Clone(data)
	return nil
}

func (s *memoryStore) ReadObject(_ context.Context, object Object, offset int64, dest []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads.Add(1)
	data, ok := s.objects[object.Digest]
	if !ok || uint64(len(data)) != object.Size {
		return fmt.Errorf("object %s missing", object.Digest)
	}
	copy(dest, data[offset:])
	return nil
}

func pseudoRandom(seed int64, size int) []byte {
	data := make([]byte, size)
	rand.New(rand.NewSource(seed)).Read(data)
	return data
}

// testTree is a small home directory that touches every part of the format:
// empty, packed, compressible and chunked files, nested directories and
// symlinks.
func testTree() map[string][]byte {
	tree := map[string][]byte{
		".zshrc":                  []byte("export EDITOR=vim\n"),
		".ssh/config":             []byte("Host *\n  ServerAliveInterval 60\n"),
		"projects/app/main.go":    []byte("package main\n\nfunc main() {}\n"),
		"projects/app/empty":      {},
		"projects/app/notes.txt":  bytes.Repeat([]byte("all work and no play\n"), 4000),
		"projects/data/model.bin": pseudoRandom(1, 9<<20+12345),
		"projects/data/log.txt":   bytes.Repeat([]byte("2026-01-01 INFO request served in 12ms\n"), 60000),
		"Documents/ünïcödé.txt":   []byte("names are UTF-8\n"),
	}
	for i := range 300 {
		tree[fmt.Sprintf("projects/app/src/file%03d.go", i)] = pseudoRandom(int64(100+i), 200+i*37)
	}
	return tree
}

func writeTree(t testing.TB, tree map[string][]byte, opts WriterOptions) (*Manifest, []byte, *memoryStore) {
	t.Helper()
	store := newMemoryStore()
	writer, err := NewWriter(opts, store.put)
	require.NoError(t, err)
	paths := make([]string, 0, len(tree))
	for path := range tree {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	added := map[string]bool{}
	for _, path := range paths {
		parts := strings.Split(path, "/")
		for i := 1; i < len(parts); i++ {
			if dir := strings.Join(parts[:i], "/"); !added[dir] {
				added[dir] = true
				require.NoError(t, writer.AddDir(dir, 0o755, 1_700_000_000_000_000_000))
			}
		}
		mode := uint32(0o644)
		if strings.HasPrefix(path, ".ssh/") {
			mode = 0o600
		}
		require.NoError(t, writer.AddFile(path, mode, 1_700_000_000_123_456_789, bytes.NewReader(tree[path])))
	}
	if _, ok := tree["projects/data/model.bin"]; ok {
		require.NoError(t, writer.AddSymlink("projects/current", "app/src", 1_700_000_000_000_000_000))
		require.NoError(t, writer.AddSymlink("projects/app/up", "../data/model.bin", 0))
	}
	stored, manifest, err := writer.Finish([]byte(`{"client":"test"}`))
	require.NoError(t, err)
	return manifest, stored, store
}

func readAll(t testing.TB, fsys *FS, manifest *Manifest, path string) []byte {
	t.Helper()
	index, ok := manifest.Find(path)
	require.True(t, ok, path)
	data := make([]byte, manifest.Entry(index).Size)
	n, err := fsys.ReadFile(context.Background(), index, 0, data)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	return data
}

func TestWriterRoundTrip(t *testing.T) {
	tree := testTree()
	opts := DefaultWriterOptions()
	manifest, stored, store := writeTree(t, tree, opts)

	parsed, err := Parse(stored)
	require.NoError(t, err)
	require.Equal(t, manifest.Digest(), parsed.Digest())
	require.Equal(t, sha256.Sum256(stored), [32]byte(parsed.Digest()))
	require.Equal(t, `{"client":"test"}`, string(parsed.Meta()))

	var total uint64
	for _, data := range tree {
		total += uint64(len(data))
	}
	require.Equal(t, total, parsed.TotalSize())
	require.Equal(t, int(parsed.Objects()), len(store.objects), "every stored object is referenced")
	// 300 small files share packs instead of taking an object each.
	require.Less(t, len(store.objects), 20)

	fsys, err := NewFS(parsed, store, FSOptions{})
	require.NoError(t, err)
	seen := 0
	require.NoError(t, parsed.Walk(func(path string, index uint32, entry Entry) error {
		if entry.Kind == KindFile {
			seen++
			require.Equal(t, tree[path], readAll(t, fsys, parsed, path), path)
		}
		return nil
	}))
	require.Equal(t, len(tree), seen)

	index, ok := parsed.Find("projects/current")
	require.True(t, ok)
	require.Equal(t, "app/src", string(parsed.Target(index)))
	index, _ = parsed.Find(".ssh/config")
	require.Equal(t, uint32(0o600), parsed.Entry(index).Mode)
	require.Equal(t, int64(1_700_000_000_123_456_789), parsed.Entry(index).MtimeNs)
	_, ok = parsed.Find("projects/missing")
	require.False(t, ok)

	// The chunked file is several extents; reads that start and end inside
	// extents return the right bytes.
	model, _ := parsed.Find("projects/data/model.bin")
	require.Greater(t, parsed.Entry(model).Count, uint32(1))
	want := tree["projects/data/model.bin"]
	random := rand.New(rand.NewSource(7))
	for range 200 {
		offset := random.Intn(len(want))
		length := 1 + random.Intn(3<<20)
		buffer := make([]byte, length)
		n, err := fsys.ReadFile(context.Background(), model, uint64(offset), buffer)
		require.NoError(t, err)
		require.Equal(t, want[offset:min(offset+length, len(want))], buffer[:n])
	}
	n, err := fsys.ReadFile(context.Background(), model, uint64(len(want)), make([]byte, 10))
	require.NoError(t, err)
	require.Zero(t, n)

	// The compressible log was stored compressed and still reads back.
	logFile, _ := parsed.Find("projects/data/log.txt")
	require.True(t, parsed.Extent(parsed.Entry(logFile).First).Compressed())
}

func TestEncodingIsCanonical(t *testing.T) {
	build := func(order []string) []byte {
		builder := NewBuilder()
		object := Object{Digest: DigestOf([]byte("x")), Size: 100}
		for _, path := range order {
			switch path {
			case "a", "a/b", "c":
				require.NoError(t, builder.AddDir(path, 0o755, 5))
			case "a/link":
				require.NoError(t, builder.AddSymlink(path, "b/file", 6))
			default:
				require.NoError(t, builder.AddFile(path, 0o644, 7, []FileExtent{{Object: object, Offset: 10, StoredLen: 20, RawLen: 20}}))
			}
		}
		stored, _, err := builder.Encode(nil, true)
		require.NoError(t, err)
		return stored
	}
	first := build([]string{"a", "a/b", "a/b/file", "a/link", "c", "c/z", "top"})
	second := build([]string{"top", "c", "c/z", "a", "a/link", "a/b", "a/b/file"})
	require.Equal(t, first, second)
}

// Every structural rule the format states is enforced on read: a manifest
// that breaks one is rejected rather than served.
func TestParseRejectsInvalidManifests(t *testing.T) {
	object := Object{Digest: DigestOf([]byte("object")), Size: 1000}
	extent := FileExtent{Object: object, Offset: 0, StoredLen: 100, RawLen: 100}
	valid := func() *Builder {
		builder := NewBuilder()
		require.NoError(t, builder.AddDir("dir", 0o755, 1))
		require.NoError(t, builder.AddFile("dir/file", 0o644, 1, []FileExtent{extent}))
		require.NoError(t, builder.AddSymlink("dir/link", "file", 1))
		require.NoError(t, builder.AddFile("z", 0o644, 1, []FileExtent{{Object: object, Offset: 100, StoredLen: 50, RawLen: 80}}))
		return builder
	}
	stored, manifest, err := valid().Encode(nil, true)
	require.NoError(t, err)
	require.Equal(t, uint32(5), manifest.Entries())

	// Offsets of the sections in this raw manifest.
	objects := envelopeSize + headerSize
	entries := objects + objectSize
	extents := entries + 5*entrySize
	names := extents + 2*extentSize
	entry := func(i int) int { return entries + i*entrySize }
	// Entry order: root, dir, z, dir/file, dir/link.
	const dirEntry, zEntry, fileEntry, linkEntry = 1, 2, 3, 4

	mutate := func(change func(data []byte)) error {
		data := bytes.Clone(stored)
		change(data)
		_, err := Parse(data)
		return err
	}
	put32 := func(offset int, value uint32) func([]byte) {
		return func(data []byte) { binary.LittleEndian.PutUint32(data[offset:], value) }
	}
	cases := map[string]func([]byte){
		"magic":                     func(data []byte) { data[0] = 'X' },
		"version":                   put32(envelopeSize, 2),
		"entry count":               put32(envelopeSize+8, 4),
		"reserved header":           func(data []byte) { data[envelopeSize+50] = 1 },
		"unknown kind":              func(data []byte) { data[entry(zEntry)+30] = 4 },
		"device node":               func(data []byte) { data[entry(zEntry)+30] = 0 },
		"setuid bit":                put32(entry(fileEntry)+32, 0o4755),
		"root not a directory":      func(data []byte) { data[entry(0)+30] = KindFile },
		"directory claims too many": put32(entry(0)+20, 3),
		"directory cycle":           put32(entry(dirEntry)+16, 1),
		"extent past object":        put32(extents+4, 951),
		"extent longer than raw":    put32(extents+8, 101),
		"extent object index":       put32(extents, 7),
		"file size":                 func(data []byte) { binary.LittleEndian.PutUint64(data[entry(fileEntry):], 99) },
		"total size":                func(data []byte) { binary.LittleEndian.PutUint64(data[envelopeSize+32:], 1) },
		"object size zero":          func(data []byte) { binary.LittleEndian.PutUint64(data[objects+32:], 0) },
		"name with slash":           func(data []byte) { data[names] = '/' },
		"name dot dot": func(data []byte) {
			// "dir" -> "..x" keeps the length; ".." needs the length cut too.
			copy(data[names:], "..")
			binary.LittleEndian.PutUint16(data[entry(dirEntry)+28:], 2)
		},
		"unsorted names":      func(data []byte) { data[names] = 'z' + 1 },
		"absolute symlink":    func(data []byte) { data[len(data)-4] = '/' },
		"truncated":           nil,
		"trailing bytes":      nil,
		"symlink mode":        put32(entry(linkEntry)+32, 0o644),
		"extent zero length":  put32(extents+12, 0),
		"file extent overlap": put32(entry(fileEntry)+16, 0),
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			var err error
			switch name {
			case "truncated":
				_, err = Parse(stored[:len(stored)-1])
			case "trailing bytes":
				_, err = Parse(append(bytes.Clone(stored), 0))
			default:
				err = mutate(change)
			}
			require.ErrorIs(t, err, ErrInvalidManifest)
		})
	}

	escapes := map[string]bool{
		"file": true, "../z": true, "./file": true, "a/../../z": true,
		"../../etc/passwd": false, "/etc/passwd": false, "..": true, "../..": false, "": false,
	}
	for target, ok := range escapes {
		builder := valid()
		err := builder.AddSymlink("dir/probe", target, 0)
		require.NoError(t, err)
		_, _, err = builder.Encode(nil, true)
		if ok {
			require.NoError(t, err, target)
		} else {
			require.ErrorIs(t, err, ErrInvalidManifest, target)
		}
	}

	// With a root that claims nothing, a directory claiming the run that
	// starts at itself would tile the table with entries no path reaches.
	orphans := NewBuilder()
	require.NoError(t, orphans.AddDir("a", 0o755, 1))
	require.NoError(t, orphans.AddFile("b", 0o644, 1, nil))
	stored, _, err = orphans.Encode(nil, true)
	require.NoError(t, err)
	first := envelopeSize + headerSize
	binary.LittleEndian.PutUint32(stored[first+20:], 0)
	binary.LittleEndian.PutUint32(stored[first+entrySize+16:], 1)
	binary.LittleEndian.PutUint32(stored[first+entrySize+20:], 2)
	_, err = Parse(stored)
	require.ErrorIs(t, err, ErrInvalidManifest)
	require.ErrorContains(t, err, "claimed by no directory before it")

	builder := NewBuilder()
	require.Error(t, builder.AddFile("missing/file", 0o644, 0, nil), "parents are explicit")
	require.Error(t, builder.AddDir("..", 0o755, 0))
	require.Error(t, builder.AddDir("a/", 0o755, 0))
	require.NoError(t, builder.AddDir("a", 0o755, 0))
	require.Error(t, builder.AddDir("a", 0o755, 0))
}

// A compressed manifest cannot make the reader allocate more than the
// envelope declares.
// allocated reports the bytes run allocates.
func allocated(run func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	run()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// A frame that decodes to more than the manifest declares for it is refused
// without being decoded: what a tenant's manifest can make a reader allocate
// is what the manifest says, not what its frames expand to.
func TestDecompressionIsBoundedByDeclaredSizes(t *testing.T) {
	bomb := zstdEncoder.EncodeAll(make([]byte, 128<<20), nil)
	require.Less(t, len(bomb), 1<<20)

	stored := make([]byte, envelopeSize, envelopeSize+len(bomb))
	copy(stored, Magic)
	binary.LittleEndian.PutUint32(stored[8:], CodecZstd)
	binary.LittleEndian.PutUint64(stored[16:], headerSize)
	stored = append(stored, bomb...)
	require.EqualValues(t, headerSize, BodySize(stored))
	var err error
	require.Less(t, allocated(func() { _, err = Parse(stored) }), uint64(8<<20))
	require.ErrorIs(t, err, ErrInvalidManifest)

	object := Object{Digest: DigestOf(bomb), Size: uint64(len(bomb))}
	store := newMemoryStore()
	require.NoError(t, store.put(object, bomb))
	builder := NewBuilder()
	extent := FileExtent{Object: object, StoredLen: uint32(len(bomb)), RawLen: 1 << 20}
	require.NoError(t, builder.AddFile("bomb", 0o644, 0, []FileExtent{extent}))
	_, manifest, err := builder.Encode(nil, false)
	require.NoError(t, err)
	fsys, err := NewFS(manifest, store, FSOptions{})
	require.NoError(t, err)
	index, _ := manifest.Find("bomb")
	require.Less(t, allocated(func() { _, err = fsys.ReadFile(context.Background(), index, 0, make([]byte, 4096)) }), uint64(8<<20))
	require.ErrorIs(t, err, zstd.ErrDecoderSizeExceeded)
}

// The chunk boundaries are shared with the Rust client: both must cut the
// same input at the same offsets or they would not deduplicate against each
// other's uploads. testdata/chunker-vectors.json was produced by the Rust
// chunker and checked by an independent Python implementation.
func TestChunkerMatchesSharedVectors(t *testing.T) {
	type vectorSet struct {
		Input struct {
			Length int    `json:"length"`
			SHA256 string `json:"sha256"`
		} `json:"input"`
		Vectors []struct {
			Name         string   `json:"name"`
			Min          int      `json:"min"`
			Avg          int      `json:"avg"`
			Max          int      `json:"max"`
			ChunkLengths []int    `json:"chunk_lengths"`
			ChunkSHA256  []string `json:"chunk_sha256"`
		} `json:"vectors"`
	}
	var file struct {
		vectorSet
		Extended vectorSet `json:"extended"`
	}
	raw, err := os.ReadFile("testdata/chunker-vectors.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &file))

	// xorshift64* seeded with the gear seed.
	generate := func(size int) []byte {
		data := make([]byte, 0, size+8)
		x := uint64(gearSeed)
		for len(data) < size {
			x ^= x >> 12
			x ^= x << 25
			x ^= x >> 27
			data = binary.LittleEndian.AppendUint64(data, x*0x2545F4914F6CDD1D)
		}
		return data[:size]
	}
	var data []byte
	for _, set := range []vectorSet{file.vectorSet, file.Extended} {
		require.NotEmpty(t, set.Vectors)
		data = generate(set.Input.Length)
		sum := sha256.Sum256(data)
		require.Equal(t, set.Input.SHA256, hex.EncodeToString(sum[:]))
		for _, vector := range set.Vectors {
			params := ChunkParams{Min: vector.Min, Avg: vector.Avg, Max: vector.Max}
			var lengths []int
			var digests []string
			for rest := data; len(rest) > 0; {
				n := params.Cut(rest)
				lengths = append(lengths, n)
				digests = append(digests, DigestOf(rest[:n]).String())
				rest = rest[n:]
			}
			require.Equal(t, vector.ChunkLengths, lengths, vector.Name)
			if len(vector.ChunkSHA256) > 0 {
				require.Equal(t, vector.ChunkSHA256, digests, vector.Name)
			}
		}
	}
	data = data[:8<<20]
	// A cut depends only on the bytes since the previous cut, so an edit
	// changes the chunks around it and the rest line up again.
	params := ChunkParams{Min: 64 << 10, Avg: 256 << 10, Max: 1 << 20}
	edited := append(append(bytes.Clone(data[:3<<20]), []byte("inserted")...), data[3<<20:]...)
	chunks := func(data []byte) map[Digest]bool {
		set := map[Digest]bool{}
		for rest := data; len(rest) > 0; {
			n := params.Cut(rest)
			set[DigestOf(rest[:n])] = true
			rest = rest[n:]
		}
		return set
	}
	before, after := chunks(data), chunks(edited)
	changed := 0
	for digest := range after {
		if !before[digest] {
			changed++
		}
	}
	require.LessOrEqual(t, changed, 3)
	require.Greater(t, len(after), 10)
}

type countingCache struct {
	objects map[Digest][]byte
	stores  int
	fail    bool
}

func (c *countingCache) ReadObject(_ context.Context, object Object, offset int64, dest []byte) bool {
	data, ok := c.objects[object.Digest]
	if !ok {
		return false
	}
	copy(dest, data[offset:])
	return true
}

func (c *countingCache) StoreObject(_ context.Context, object Object, data []byte) error {
	if c.fail {
		return errors.New("cache unavailable")
	}
	c.stores++
	c.objects[object.Digest] = data
	return nil
}

// contentStore takes content only under its SHA-256, as a node cache does.
type contentStore map[string][]byte

func (c contentStore) ReadContent(_ context.Context, key string, offset int64, dest []byte) bool {
	data, ok := c[key]
	if !ok || offset+int64(len(dest)) > int64(len(data)) {
		return false
	}
	copy(dest, data[offset:])
	return true
}

func (c contentStore) StoreContent(_ context.Context, key string, content []byte) error {
	if DigestOf(content).String() != key {
		return errors.New("content does not match its key")
	}
	c[key] = bytes.Clone(content)
	return nil
}

func TestScopedCacheKeepsMirrorsApart(t *testing.T) {
	ctx := context.Background()
	store := contentStore{}
	ours := ScopedCache{Store: store, Scope: []byte("mirror a")}
	theirs := ScopedCache{Store: store, Scope: []byte("mirror b")}

	// Every length around the block and padding boundaries of SHA-256 is
	// stored under a key the store accepts as the hash of what it is given,
	// and read back from the digest and size alone.
	sizes := []int{0, 1, 54, 55, 56, 57, 63, 64, 65, 118, 119, 120, 121, 127, 128, 129, 5000, 1 << 20}
	for _, size := range sizes {
		data := pseudoRandom(int64(size), size)
		object := Object{Digest: DigestOf(data), Size: uint64(size)}
		require.NoError(t, ours.StoreObject(ctx, object, data), size)
		dest := make([]byte, size)
		require.True(t, ours.ReadObject(ctx, object, 0, dest), size)
		require.Equal(t, data, dest, size)
		if size > 10 {
			tail := make([]byte, 10)
			require.True(t, ours.ReadObject(ctx, object, int64(size-10), tail))
			require.Equal(t, data[size-10:], tail)
		}

		// Knowing an object's digest and size is not enough to read it
		// from another mirror's entries, nor under its bare digest.
		require.False(t, theirs.ReadObject(ctx, object, 0, dest), size)
		require.NotContains(t, store, object.Digest.String())
		// A wrong size finds nothing, and so never the padding or the
		// scope that follow the object in the store.
		for _, wrong := range []uint64{object.Size + 1, object.Size + 9, object.Size + 64} {
			require.False(t, ours.ReadObject(ctx, Object{Digest: object.Digest, Size: wrong}, 0, nil), "%d as %d", size, wrong)
			require.Error(t, ours.StoreObject(ctx, Object{Digest: object.Digest, Size: wrong}, data))
		}
	}
	require.Len(t, store, len(sizes))

	// The same object in two mirrors is two entries.
	data := pseudoRandom(9, 300)
	object := Object{Digest: DigestOf(data), Size: 300}
	require.NoError(t, ours.StoreObject(ctx, object, data))
	require.NoError(t, theirs.StoreObject(ctx, object, data))
	require.Len(t, store, len(sizes)+2)
}

func TestCachedSourceVerifiesAndCaches(t *testing.T) {
	good := pseudoRandom(3, 5000)
	object := Object{Digest: DigestOf(good), Size: uint64(len(good))}
	downloads := 0
	var serve func() ([]byte, error)
	cache := &countingCache{objects: map[Digest][]byte{}}
	source := &CachedSource{
		Cache:    cache,
		Attempts: 3,
		Download: func(context.Context, Digest) (io.ReadCloser, error) {
			downloads++
			data, err := serve()
			if err != nil {
				return nil, err
			}
			return io.NopCloser(bytes.NewReader(data)), nil
		},
	}
	read := func() ([]byte, error) {
		dest := make([]byte, 100)
		err := source.ReadObject(context.Background(), object, 4000, dest)
		return dest, err
	}

	// Wrong bytes under the right name are refused, whether they are
	// altered, cut short or padded.
	for _, bad := range [][]byte{pseudoRandom(4, 5000), good[:4999], append(bytes.Clone(good), 0)} {
		serve = func() ([]byte, error) { return bad, nil }
		_, err := read()
		require.ErrorIs(t, err, ErrObjectMismatch)
	}
	require.Zero(t, cache.stores)

	// A failed attempt is retried, and the verified object lands in the cache.
	downloads = 0
	serve = func() ([]byte, error) {
		if downloads == 1 {
			return nil, errors.New("connection reset")
		}
		return good, nil
	}
	dest, err := read()
	require.NoError(t, err)
	require.Equal(t, good[4000:4100], dest)
	require.Equal(t, 2, downloads)
	require.Equal(t, 1, cache.stores)

	// With the node cache lost the object is fetched again from the store.
	cache.objects = map[Digest][]byte{}
	source.recent, source.held = nil, 0
	source.order.Init()
	downloads = 0
	serve = func() ([]byte, error) { return good, nil }
	dest, err = read()
	require.NoError(t, err)
	require.Equal(t, good[4000:4100], dest)
	require.Equal(t, 1, downloads)

	// A cache that cannot store does not stop reads or cause refetches.
	cache.fail = true
	cache.objects = map[Digest][]byte{}
	_, err = read()
	require.NoError(t, err)
	require.Equal(t, 1, downloads)

	require.Error(t, source.ReadObject(context.Background(), object, 4990, make([]byte, 100)), "read past the end")
}

func TestCachedSourcesShareDownloadBudget(t *testing.T) {
	ctx := context.Background()
	first, second := []byte("one"), []byte("two")
	reserved := int64(downloadBudgetBytes - len(first) - 1)
	require.NoError(t, downloadBudget.Acquire(ctx, reserved))
	defer downloadBudget.Release(reserved)

	opened := make(chan Digest, 3)
	release := make(chan struct{})
	defer close(release)
	source := func(data []byte) (*CachedSource, Object) {
		return &CachedSource{Download: func(_ context.Context, digest Digest) (io.ReadCloser, error) {
			opened <- digest
			<-release
			return io.NopCloser(bytes.NewReader(data)), nil
		}}, Object{Digest: DigestOf(data), Size: uint64(len(data))}
	}
	a, aObject := source(first)
	b, bObject := source(second)
	readCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	go func() { _ = a.ReadObject(readCtx, aObject, 0, nil) }()
	select {
	case got := <-opened:
		require.Equal(t, aObject.Digest, got)
	case <-readCtx.Done():
		t.Fatal("first download did not start")
	}
	blockedCtx, cancelBlocked := context.WithTimeout(ctx, 25*time.Millisecond)
	defer cancelBlocked()
	require.ErrorIs(t, b.ReadObject(blockedCtx, bObject, 0, nil), context.DeadlineExceeded)
	select {
	case got := <-opened:
		t.Fatalf("download %s opened with the budget exhausted", got)
	default:
	}
}

func TestCachedSourcePrefetch(t *testing.T) {
	ctx := context.Background()
	data := pseudoRandom(5, 3000)
	object := Object{Digest: DigestOf(data), Size: uint64(len(data))}
	downloads := 0
	store := contentStore{}
	download := func(context.Context, Digest) (io.ReadCloser, error) {
		downloads++
		return io.NopCloser(bytes.NewReader(data)), nil
	}
	source := &CachedSource{Cache: ScopedCache{Store: store, Scope: []byte("mirror a")}, Download: download}

	// Without a cache there is nowhere to put it.
	fetched, err := (&CachedSource{Download: download}).Prefetch(ctx, object)
	require.NoError(t, err)
	require.False(t, fetched)
	require.Zero(t, downloads)

	// It lands in the cache and not in memory, once, and a read then
	// downloads nothing.
	fetched, err = source.Prefetch(ctx, object)
	require.NoError(t, err)
	require.True(t, fetched)
	require.Len(t, store, 1)
	require.Empty(t, source.recent)
	fetched, err = source.Prefetch(ctx, object)
	require.NoError(t, err)
	require.False(t, fetched)
	dest := make([]byte, 10)
	require.NoError(t, source.ReadObject(ctx, object, 100, dest))
	require.Equal(t, data[100:110], dest)
	require.Equal(t, 1, downloads)

	// Bytes that do not match their name are not kept.
	other := pseudoRandom(6, 3000)
	mismatched := Object{Digest: DigestOf(other), Size: uint64(len(other))}
	fetched, err = source.Prefetch(ctx, mismatched)
	require.ErrorIs(t, err, ErrObjectMismatch)
	require.False(t, fetched)
	require.Len(t, store, 1)

	// It waits for room in the download budget, and gives up when asked to.
	reserved := int64(downloadBudgetBytes - 1000)
	require.NoError(t, downloadBudget.Acquire(ctx, reserved))
	full, cancel := context.WithTimeout(ctx, 3*prefetchPoll)
	defer cancel()
	before := downloads
	_, err = source.Prefetch(full, mismatched)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, before, downloads)

	// Room there is goes to a read that waits for more before a prefetch.
	large := pseudoRandom(7, 2000)
	reader := &CachedSource{Download: func(context.Context, Digest) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(large)), nil
	}}
	read := make(chan error, 1)
	go func() {
		read <- reader.ReadObject(ctx, Object{Digest: DigestOf(large), Size: uint64(len(large))}, 0, make([]byte, len(large)))
	}()
	require.Eventually(t, func() bool {
		if downloadBudget.TryAcquire(1) {
			downloadBudget.Release(1)
			return false
		}
		return true
	}, 5*time.Second, time.Millisecond, "the read waits for the budget")
	small := pseudoRandom(8, 500)
	waiting, cancelWaiting := context.WithTimeout(ctx, 3*prefetchPoll)
	defer cancelWaiting()
	_, err = (&CachedSource{Cache: source.Cache, Download: func(context.Context, Digest) (io.ReadCloser, error) {
		downloads++
		return io.NopCloser(bytes.NewReader(small)), nil
	}}).Prefetch(waiting, Object{Digest: DigestOf(small), Size: uint64(len(small))})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, before, downloads)
	downloadBudget.Release(reserved)
	require.NoError(t, <-read)
}

func TestSourceHome(t *testing.T) {
	meta := func(home string) []byte {
		data, err := json.Marshal(map[string]any{"client": "beam-mirror", "source": map[string]any{"os": "macos", "arch": "aarch64", "host": "mbp", "home": home, "user": "luke"}})
		require.NoError(t, err)
		return data
	}
	longest := strings.Repeat("a", 32)
	for _, home := range []string{"/Users/luke", "/home/dev.user-1", "/home/_svc", "/home/" + longest} {
		require.Equal(t, home, SourceHome(meta(home)), home)
	}
	for _, home := range []string{
		"", "/", "/root", "/Users", "/Users/", "/Users/luke/", "/Users/luke/code", "/Users//luke",
		"//Users/luke", "/Users/./luke", "/Users/.", "/Users/..", "/home/.hidden", "/home/-x",
		"/home/a b", "/home/a:b", "/home/a\nb", "/var/luke", "Users/luke", "/home/" + longest + "a",
	} {
		require.Empty(t, SourceHome(meta(home)), home)
	}

	// Meta is the client's to fill: what it holds can only fail to name a home.
	for _, malformed := range []string{`{"source":{"home":7}}`, `{"source":"/Users/luke"}`, `{"client":"beam-mirror"}`, `["/Users/luke"]`, `not json`, ``} {
		require.Empty(t, SourceHome([]byte(malformed)), malformed)
	}
	require.Equal(t, "/Users/luke", SourceHome([]byte(`{"source":{"home":"/Users/luke","user":7},"chunker":"fast"}`)), "members it does not read are not checked")
}

func TestPlacements(t *testing.T) {
	manifest, _, store := writeTree(t, map[string][]byte{"a": []byte("a")}, DefaultWriterOptions())
	for _, bad := range [][]Placement{
		{{Path: "/"}}, {{Path: "root"}}, {{Path: "/root"}, {Path: "/root"}},
		{{Path: "/home"}, {Path: "/home/beam"}}, {{Path: "/home/beam"}, {Path: "/home"}},
	} {
		_, err := NewFS(manifest, store, FSOptions{Placements: bad})
		require.Error(t, err, "%v", bad)
	}
	_, err := NewFS(manifest, store, FSOptions{Placements: []Placement{{Path: "/home/a"}, {Path: "/home/b"}, {Path: "/root"}}})
	require.NoError(t, err)
}

// A desktop VM's two homes.
var desktopPlacements = []Placement{RootHome, {Path: "/home/beam", UID: 1001, GID: 1002, Mode: 0o750}}

// The mounted filesystem, read through the kernel: the tree appears under
// both homes with each user's ownership, contents match, nothing can be
// written, and nothing outside the homes exists.
func TestMountedFilesystem(t *testing.T) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skip("requires /dev/fuse")
	}
	if os.Geteuid() != 0 {
		t.Skip("requires root")
	}
	tree := testTree()
	manifest, _, store := writeTree(t, tree, DefaultWriterOptions())
	fsys, err := NewFS(manifest, store, FSOptions{Placements: desktopPlacements})
	require.NoError(t, err)
	mountPoint := t.TempDir()
	server, err := Mount(mountPoint, fsys)
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Unmount() })

	top, err := os.ReadDir(mountPoint)
	require.NoError(t, err)
	require.Len(t, top, 2)
	require.Equal(t, "home", top[0].Name())
	require.Equal(t, "root", top[1].Name())

	for home, owner := range map[string][2]uint32{"root": {0, 0}, "home/beam": {1001, 1002}} {
		base := filepath.Join(mountPoint, home)
		files := 0
		require.NoError(t, filepath.WalkDir(base, func(path string, entry os.DirEntry, err error) error {
			require.NoError(t, err)
			info, err := entry.Info()
			require.NoError(t, err)
			stat := info.Sys().(*syscall.Stat_t)
			require.Equal(t, owner, [2]uint32{stat.Uid, stat.Gid}, path)
			if entry.Type().IsRegular() {
				files++
				relative, _ := filepath.Rel(base, path)
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				require.True(t, bytes.Equal(tree[relative], data), relative)
			}
			return nil
		}))
		require.Equal(t, len(tree), files)

		info, err := os.Stat(filepath.Join(base, ".ssh/config"))
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
		require.Equal(t, int64(1_700_000_000_123_456_789), info.ModTime().UnixNano())
		target, err := os.Readlink(filepath.Join(base, "projects/current"))
		require.NoError(t, err)
		require.Equal(t, "app/src", target)
		data, err := os.ReadFile(filepath.Join(base, "projects/app/up"))
		require.NoError(t, err)
		require.True(t, bytes.Equal(tree["projects/data/model.bin"], data), "symlinks resolve inside the tree")

		require.ErrorIs(t, os.WriteFile(filepath.Join(base, "new"), nil, 0o644), syscall.EROFS)
		require.ErrorIs(t, os.Remove(filepath.Join(base, ".zshrc")), syscall.EROFS)
		_, err = os.Stat(filepath.Join(base, "projects/missing"))
		require.ErrorIs(t, err, os.ErrNotExist)
	}
	for home, mode := range map[string]os.FileMode{"root": 0o700, "home/beam": 0o750} {
		info, err := os.Stat(filepath.Join(mountPoint, home))
		require.NoError(t, err)
		require.Equal(t, mode, info.Mode().Perm(), home)
	}

	// The same file has the same inode number on a second mount of the
	// generation, which is what lets a guest overlay survive a remount.
	first, err := os.Stat(filepath.Join(mountPoint, "root/projects/app/main.go"))
	require.NoError(t, err)
	again, err := NewFS(manifest, store, FSOptions{Placements: desktopPlacements})
	require.NoError(t, err)
	second := t.TempDir()
	server2, err := Mount(second, again)
	require.NoError(t, err)
	t.Cleanup(func() { _ = server2.Unmount() })
	other, err := os.Stat(filepath.Join(second, "root/projects/app/main.go"))
	require.NoError(t, err)
	require.Equal(t, first.Sys().(*syscall.Stat_t).Ino, other.Sys().(*syscall.Stat_t).Ino)

	// A revoked mount keeps its names and returns no contents, including
	// those the kernel had cached and those of files already open.
	cached := filepath.Join(second, "root/projects/app/main.go")
	data, err := os.ReadFile(cached)
	require.NoError(t, err)
	require.True(t, bytes.Equal(tree["projects/app/main.go"], data))
	open, err := os.Open(filepath.Join(second, "home/beam/projects/data/model.bin"))
	require.NoError(t, err)
	defer open.Close()
	head := make([]byte, 4096)
	_, err = io.ReadFull(open, head)
	require.NoError(t, err)

	again.Revoke()
	_, err = os.ReadFile(cached)
	require.ErrorIs(t, err, syscall.EIO, "read before the purge")
	_, err = open.ReadAt(head, 0)
	require.ErrorIs(t, err, syscall.EIO, "open before the purge")
	_, err = os.ReadFile(filepath.Join(second, "root/projects/data/model.bin"))
	require.ErrorIs(t, err, syscall.EIO)
	_, err = os.ReadFile(filepath.Join(second, "home/beam/.zshrc"))
	require.ErrorIs(t, err, syscall.EIO)
	entries, err := os.ReadDir(filepath.Join(second, "root/projects"))
	require.NoError(t, err)
	require.NotEmpty(t, entries)
	target, err := os.Readlink(filepath.Join(second, "root/projects/current"))
	require.NoError(t, err)
	require.Equal(t, "app/src", target)
	_, err = os.ReadFile(filepath.Join(mountPoint, "root/projects/data/model.bin"))
	require.NoError(t, err, "other mounts are unaffected")
}

// The manifest vector is shared with the Rust client: a tree with explicit
// extents and the canonical bytes every writer must encode it to. Run with
// UPDATE_MIRROR_VECTORS=1 to regenerate it after a deliberate format change.
type manifestVector struct {
	Description string        `json:"description"`
	Meta        string        `json:"meta"`
	Root        vectorEntry   `json:"root"`
	Entries     []vectorEntry `json:"entries"`
	// The body, stored uncompressed (envelope codec 0).
	RawManifest       string `json:"raw_manifest_hex"`
	RawManifestSHA256 string `json:"raw_manifest_sha256"`
}

type vectorEntry struct {
	Path    string         `json:"path,omitempty"`
	Kind    string         `json:"kind"`
	Mode    uint32         `json:"mode"`
	MtimeNs int64          `json:"mtime_ns"`
	Target  string         `json:"target,omitempty"`
	Extents []vectorExtent `json:"extents,omitempty"`
}

type vectorExtent struct {
	Object     string `json:"object"`
	ObjectSize uint64 `json:"object_size"`
	Offset     uint32 `json:"offset"`
	StoredLen  uint32 `json:"stored_len"`
	RawLen     uint32 `json:"raw_len"`
}

const manifestVectorPath = "testdata/manifest-vector.json"

func TestManifestMatchesSharedVector(t *testing.T) {
	data, err := os.ReadFile(manifestVectorPath)
	require.NoError(t, err)
	var vector manifestVector
	require.NoError(t, json.Unmarshal(data, &vector))

	builder := NewBuilder()
	builder.SetRoot(vector.Root.Mode, vector.Root.MtimeNs)
	// Entries are listed parents first but otherwise out of order: the
	// encoding must not depend on the order they were added in.
	for _, entry := range vector.Entries {
		switch entry.Kind {
		case "dir":
			require.NoError(t, builder.AddDir(entry.Path, entry.Mode, entry.MtimeNs))
		case "symlink":
			require.NoError(t, builder.AddSymlink(entry.Path, entry.Target, entry.MtimeNs))
		case "file":
			extents := make([]FileExtent, 0, len(entry.Extents))
			for _, extent := range entry.Extents {
				digest, err := ParseDigest(extent.Object)
				require.NoError(t, err)
				extents = append(extents, FileExtent{Object: Object{Digest: digest, Size: extent.ObjectSize}, Offset: extent.Offset, StoredLen: extent.StoredLen, RawLen: extent.RawLen})
			}
			require.NoError(t, builder.AddFile(entry.Path, entry.Mode, entry.MtimeNs, extents))
		default:
			t.Fatalf("unknown kind %q", entry.Kind)
		}
	}
	raw, manifest, err := builder.Encode([]byte(vector.Meta), true)
	require.NoError(t, err)

	if os.Getenv("UPDATE_MIRROR_VECTORS") != "" {
		vector.RawManifest = hex.EncodeToString(raw)
		vector.RawManifestSHA256 = manifest.Digest().String()
		updated, err := json.MarshalIndent(vector, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(manifestVectorPath, append(updated, '\n'), 0o644))
	}
	require.Equal(t, vector.RawManifest, hex.EncodeToString(raw))
	require.Equal(t, vector.RawManifestSHA256, manifest.Digest().String())

	// The compressed envelope holds the same body. Its bytes depend on the
	// zstd implementation, so only the decoded form is comparable.
	compressed, decoded, err := builder.Encode([]byte(vector.Meta), false)
	require.NoError(t, err)
	require.Less(t, len(compressed), len(raw))
	require.Equal(t, raw[envelopeSize:], decoded.body)
	reparsed, err := Parse(raw)
	require.NoError(t, err)
	require.Equal(t, decoded.body, reparsed.body)

	index, ok := reparsed.Find("projects/app/src/main.rs")
	require.True(t, ok)
	require.Equal(t, uint64(5000), reparsed.Entry(index).Size)
	index, ok = reparsed.Find("projects/current")
	require.True(t, ok)
	require.Equal(t, "app/src", string(reparsed.Target(index)))
}

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
//	sudo MIRROR_SCALE_FILES=1000000 go test ./pkg/abstractions/vm/mirror -run TestScale -v
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
	// A desktop VM: a purge visits every file once for each home.
	fsys, err := NewFS(manifest, zeroSource{}, FSOptions{Placements: desktopPlacements})
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

	// A purge tells the kernel to drop the data of every file, here with
	// every file known to the kernel and one page of each in its cache.
	read := 0
	page := make([]byte, 4096)
	require.NoError(t, filepath.WalkDir(filepath.Join(mountPoint, "root"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || !entry.Type().IsRegular() || read >= 50_000 {
			return err
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		read++
		_, err = file.Read(page)
		return err
	}))
	started = time.Now()
	fsys.Revoke()
	t.Logf("revoke with %d files cached: %s", read, time.Since(started).Round(time.Millisecond))
}
