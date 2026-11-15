package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/mirror"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type fakeMirrorBucket struct {
	mu        sync.Mutex
	objects   map[string][]byte
	downloads map[string]int
	outage    bool
	corrupt   map[string]bool
	// hold keeps downloads of the keys it names open until their context
	// ends; inflight and peak count them.
	hold           func(key string) bool
	inflight, peak int
}

func (b *fakeMirrorBucket) Exists(_ context.Context, key string) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.outage {
		return false, errors.New("store unreachable")
	}
	_, ok := b.objects[key]
	return ok, nil
}

func (b *fakeMirrorBucket) DownloadWithReader(ctx context.Context, key string) (io.ReadCloser, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.objects[key]
	if !ok || b.outage {
		return nil, errors.New("no such object")
	}
	b.downloads[key]++
	if b.hold != nil && b.hold(key) {
		b.inflight++
		b.peak = max(b.peak, b.inflight)
		b.mu.Unlock()
		<-ctx.Done()
		b.mu.Lock()
		b.inflight--
		return nil, ctx.Err()
	}
	if b.corrupt[key] {
		data = append([]byte("x"), data[1:]...)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

// state reports the downloads in progress, the most there were at once, and
// how many content objects (everything but the manifest) were ever asked for.
func (b *fakeMirrorBucket) state(manifestKey string) (inflight, peak, asked int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for key := range b.downloads {
		if key != manifestKey {
			asked++
		}
	}
	return b.inflight, b.peak, asked
}

// memoryMirrorCache is a node content cache: like the real one it takes
// content only under its SHA-256.
type memoryMirrorCache struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func (c *memoryMirrorCache) ReadContent(_ context.Context, key string, offset int64, dest []byte) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	data, ok := c.objects[key]
	if !ok || offset+int64(len(dest)) > int64(len(data)) {
		return false
	}
	copy(dest, data[offset:])
	return true
}

func (c *memoryMirrorCache) StoreContent(_ context.Context, key string, content []byte) error {
	if mirror.DigestOf(content).String() != key {
		return errors.New("content does not match its key")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.objects[key] = bytes.Clone(content)
	return nil
}

func (c *memoryMirrorCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.objects)
}

type fakeMirrorMount struct{ unmounted *atomic.Int32 }

func (m fakeMirrorMount) Unmount() error {
	m.unmounted.Add(1)
	return nil
}

type blockingMirrorMount struct {
	started chan struct{}
	finish  chan struct{}
}

func (m blockingMirrorMount) Unmount() error {
	close(m.started)
	<-m.finish
	return nil
}

type mirrorLayerHarness struct {
	layers    *mirrorLayers
	bucket    *fakeMirrorBucket
	cache     *memoryMirrorCache
	mounts    map[string]*mirror.FS
	unmounted atomic.Int32
	opened    atomic.Int32
	mirrorID  string
}

func newMirrorLayerHarness(t *testing.T) *mirrorLayerHarness {
	t.Helper()
	h := &mirrorLayerHarness{
		bucket:   &fakeMirrorBucket{objects: map[string][]byte{}, downloads: map[string]int{}, corrupt: map[string]bool{}},
		cache:    &memoryMirrorCache{objects: map[string][]byte{}},
		mounts:   map[string]*mirror.FS{},
		mirrorID: uuid.NewString(),
	}
	var mu sync.Mutex
	h.layers = &mirrorLayers{
		ctx:    context.Background(),
		root:   t.TempDir(),
		cache:  h.cache,
		idle:   time.Minute,
		layers: map[string]*mirrorLayer{},
		open: func(context.Context, *types.ContainerRequest) (mirrorBucket, error) {
			h.opened.Add(1)
			return h.bucket, nil
		},
		mount: func(path string, fsys *mirror.FS) (mirrorMount, error) {
			mu.Lock()
			defer mu.Unlock()
			h.mounts[path] = fsys
			return fakeMirrorMount{&h.unmounted}, nil
		},
	}
	return h
}

// generation uploads a generation holding files and returns its pin.
func (h *mirrorLayerHarness) generation(t *testing.T, number int64, files map[string]string) *types.VMMirror {
	t.Helper()
	writer, err := mirror.NewWriter(mirror.DefaultWriterOptions(), func(object mirror.Object, data []byte) error {
		h.bucket.objects[types.MirrorObjectKey(h.mirrorID, object.Digest.String())] = bytes.Clone(data)
		return nil
	})
	require.NoError(t, err)
	for name, content := range files {
		require.NoError(t, writer.AddFile(name, 0o644, 0, strings.NewReader(content)))
	}
	stored, manifest, err := writer.Finish(nil)
	require.NoError(t, err)
	h.bucket.objects[types.MirrorObjectKey(h.mirrorID, manifest.Digest().String())] = stored
	return &types.VMMirror{Name: "laptop", ID: h.mirrorID, Generation: number, ManifestDigest: manifest.Digest().String(), ManifestSize: int64(len(stored))}
}

func mirrorRequest(workspace uint) *types.ContainerRequest {
	return &types.ContainerRequest{ContainerId: uuid.NewString(), Workspace: types.Workspace{Id: workspace, Name: "ws"}}
}

func readMirrorFile(t *testing.T, fsys *mirror.FS, name string) (string, error) {
	t.Helper()
	parsed := fsys.Manifest()
	index, ok := parsed.Find(name)
	require.True(t, ok, name)
	buffer := make([]byte, parsed.Entry(index).Size)
	n, err := fsys.ReadFile(context.Background(), index, 0, buffer)
	return string(buffer[:n]), err
}

func TestMirrorLayersShareOneMountPerGeneration(t *testing.T) {
	h := newMirrorLayerHarness(t)
	first := h.generation(t, 1, map[string]string{"notes.txt": "one"})
	second := h.generation(t, 2, map[string]string{"notes.txt": "two"})
	ctx := context.Background()

	// Many VMs starting at once on one generation mount it once.
	var wg sync.WaitGroup
	paths := make([]string, 8)
	releases := make([]func(), 8)
	for i := range paths {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			paths[i], releases[i], err = h.layers.acquire(ctx, mirrorRequest(7), first, nil)
			require.NoError(t, err)
		}()
	}
	wg.Wait()
	for _, path := range paths {
		require.Equal(t, paths[0], path)
	}
	require.Len(t, h.mounts, 1)
	require.Equal(t, int32(1), h.opened.Load())
	require.DirExists(t, paths[0])
	content, err := readMirrorFile(t, h.mounts[paths[0]], "notes.txt")
	require.NoError(t, err)
	require.Equal(t, "one", content)

	// Another generation, and the same generation for another workspace,
	// are separate mounts.
	other, releaseOther, err := h.layers.acquire(ctx, mirrorRequest(7), second, nil)
	require.NoError(t, err)
	require.NotEqual(t, paths[0], other)
	foreign, releaseForeign, err := h.layers.acquire(ctx, mirrorRequest(8), first, nil)
	require.NoError(t, err)
	require.NotEqual(t, paths[0], foreign)
	require.Len(t, h.mounts, 3)

	// A mount outlives its VMs by the idle timeout and no longer.
	for _, release := range releases[1:] {
		release()
		release()
	}
	require.Zero(t, h.layers.pruneIdle(time.Now().Add(time.Hour)), "one VM still uses it")
	releases[0]()
	releaseOther()
	releaseForeign()
	require.Zero(t, h.layers.pruneIdle(time.Now().Add(-time.Minute)))
	again, release, err := h.layers.acquire(ctx, mirrorRequest(7), first, nil)
	require.NoError(t, err)
	require.Equal(t, paths[0], again, "a restart inside the timeout finds the mount")
	require.Equal(t, int32(3), h.opened.Load())
	require.Equal(t, 2, h.layers.pruneIdle(time.Now().Add(time.Hour)))
	require.Equal(t, int32(2), h.unmounted.Load())
	release()
	require.Equal(t, 1, h.layers.pruneIdle(time.Now().Add(time.Hour)))
	require.Equal(t, int32(3), h.unmounted.Load())
	require.NoDirExists(t, paths[0])
}

func TestMirrorLayersReservePathUntilIdleTeardownFinishes(t *testing.T) {
	h := newMirrorLayerHarness(t)
	pin := h.generation(t, 1, map[string]string{"notes.txt": "one"})
	mount := blockingMirrorMount{started: make(chan struct{}), finish: make(chan struct{})}
	var finish sync.Once
	defer finish.Do(func() { close(mount.finish) })
	h.layers.mount = func(string, *mirror.FS) (mirrorMount, error) { return mount, nil }
	path, release, err := h.layers.acquire(t.Context(), mirrorRequest(7), pin, nil)
	require.NoError(t, err)
	release()
	pruned := make(chan int, 1)
	go func() { pruned <- h.layers.pruneIdle(time.Now().Add(time.Hour)) }()
	select {
	case <-mount.started:
	case <-time.After(time.Second):
		t.Fatal("unmount did not start")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	_, _, err = h.layers.acquire(ctx, mirrorRequest(7), pin, nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, int32(1), h.opened.Load())

	finish.Do(func() { close(mount.finish) })
	require.Equal(t, 1, <-pruned)
	require.NoDirExists(t, path)
	again, releaseAgain, err := h.layers.acquire(t.Context(), mirrorRequest(7), pin, nil)
	require.NoError(t, err)
	defer releaseAgain()
	require.Equal(t, path, again)
	require.Equal(t, int32(2), h.opened.Load())
}

// The homes are the image's, so VMs of images whose desktop users differ
// cannot share a mount: ownership belongs to the mount.
func TestMirrorLayersMountPerSetOfHomes(t *testing.T) {
	h := newMirrorLayerHarness(t)
	pin := h.generation(t, 1, map[string]string{"notes.txt": "one"})
	ctx := context.Background()
	desktop := func(uid uint32) []mirror.Placement {
		return []mirror.Placement{mirror.RootHome, {Path: "/home/beam", UID: uid, GID: uid, Mode: 0o750}}
	}

	headless, release, err := h.layers.acquire(ctx, mirrorRequest(7), pin, nil)
	require.NoError(t, err)
	defer release()
	same, release, err := h.layers.acquire(ctx, mirrorRequest(7), pin, []mirror.Placement{mirror.RootHome})
	require.NoError(t, err)
	defer release()
	require.Equal(t, headless, same)
	require.Equal(t, []mirror.Placement{mirror.RootHome}, h.mounts[headless].Placements())

	paths := map[string]bool{headless: true}
	for _, uid := range []uint32{1000, 1001, 1001} {
		path, release, err := h.layers.acquire(ctx, mirrorRequest(7), pin, desktop(uid))
		require.NoError(t, err)
		defer release()
		paths[path] = true
		require.Equal(t, desktop(uid), h.mounts[path].Placements())
	}
	require.Len(t, paths, 3)
	require.Len(t, h.mounts, 3)

	// A home of its own is another set.
	source := []mirror.Placement{{Path: "/Users/luke", UID: 1001, GID: 1001, Mode: 0o750}}
	path, release, err := h.layers.acquire(ctx, mirrorRequest(7), pin, source)
	require.NoError(t, err)
	defer release()
	require.False(t, paths[path])
	require.Equal(t, source, h.mounts[path].Placements())
}

func TestMirrorPlacementsAreTheImagesHomes(t *testing.T) {
	// What a host file would say if a symlink in an image could reach it.
	outside := filepath.Join(t.TempDir(), "passwd")
	require.NoError(t, os.WriteFile(outside, []byte("beam:x:4242:4242::/home/host:/bin/bash\n"), 0o644))

	image := func(files map[string]string, dirs map[string]os.FileMode) string {
		rootfs := t.TempDir()
		for name, mode := range dirs {
			require.NoError(t, os.MkdirAll(filepath.Join(rootfs, name), 0o755))
			require.NoError(t, os.Chmod(filepath.Join(rootfs, name), mode))
		}
		for name, content := range files {
			require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(rootfs, name)), 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(rootfs, name), []byte(content), 0o644))
		}
		return rootfs
	}
	// Ubuntu's image has an account at 1000 before the desktop's is added.
	passwd := "root:x:0:0:root:/root:/bin/bash\nubuntu:x:1000:1000:Ubuntu:/home/ubuntu:/bin/bash\nbeam:x:1001:1002::/home/beam:/bin/bash\n"
	root := mirror.RootHome
	beam := mirror.Placement{Path: "/home/beam", UID: 1001, GID: 1002, Mode: 0o750}

	linked := image(map[string]string{imageDesktopUserFile: "beam\n"}, nil)
	require.NoError(t, os.Mkdir(filepath.Join(linked, "etc"), 0o755))
	require.NoError(t, os.Symlink(outside, filepath.Join(linked, "etc/passwd")))
	notAFile := image(map[string]string{"etc/passwd": passwd}, map[string]os.FileMode{imageDesktopUserFile: 0o755})

	desktopImage := image(map[string]string{"etc/passwd": passwd, imageDesktopUserFile: "beam\n"}, map[string]os.FileMode{"home/beam": 0o755})
	for _, test := range []struct {
		name    string
		rootfs  string
		home    string
		want    []mirror.Placement
		wantErr string
	}{
		// A home from the source machine is the only placement, the desktop
		// user's, with a fixed mode.
		{name: "source home", rootfs: desktopImage, home: "/Users/luke", want: []mirror.Placement{{Path: "/Users/luke", UID: 1001, GID: 1002, Mode: 0o750}}},
		{name: "source home over the image's", rootfs: desktopImage, home: "/home/beam", want: []mirror.Placement{{Path: "/home/beam", UID: 1001, GID: 1002, Mode: 0o750}}},
		{name: "source home without a desktop", rootfs: image(map[string]string{"etc/passwd": passwd}, nil), home: "/Users/luke", want: []mirror.Placement{{Path: "/Users/luke", Mode: 0o750}}},
		{name: "source home with a root desktop", rootfs: image(map[string]string{"etc/passwd": passwd, imageDesktopUserFile: "root\n"}, nil), home: "/home/luke", want: []mirror.Placement{{Path: "/home/luke", Mode: 0o750}}},
		{name: "source home still reads the image", rootfs: notAFile, home: "/Users/luke", wantErr: "not a regular file"},
		{name: "no desktop", rootfs: image(map[string]string{"etc/passwd": passwd}, nil), want: []mirror.Placement{root}},
		{name: "desktop user", rootfs: image(map[string]string{"etc/passwd": passwd, imageDesktopUserFile: "beam\n"}, nil), want: []mirror.Placement{root, beam}},
		{
			name:   "homes keep the image's modes",
			rootfs: image(map[string]string{"etc/passwd": passwd, imageDesktopUserFile: "beam\n"}, map[string]os.FileMode{"root": 0o710, "home/beam": 0o755}),
			want:   []mirror.Placement{{Path: "/root", Mode: 0o710}, {Path: "/home/beam", UID: 1001, GID: 1002, Mode: 0o755}},
		},
		{name: "desktop runs as root", rootfs: image(map[string]string{"etc/passwd": passwd, imageDesktopUserFile: "root\n"}, nil), want: []mirror.Placement{root}},
		{name: "no such account", rootfs: image(map[string]string{"etc/passwd": passwd, imageDesktopUserFile: "kiosk\n"}, nil), want: []mirror.Placement{root}},
		{name: "no accounts", rootfs: image(map[string]string{imageDesktopUserFile: "beam\n"}, nil), want: []mirror.Placement{root}},
		{name: "accounts linked out of the image", rootfs: linked, want: []mirror.Placement{root}},
		{name: "oversized uid", rootfs: image(map[string]string{"etc/passwd": "beam:x:4294967296:1::/home/beam:\n", imageDesktopUserFile: "beam\n"}, nil), wantErr: "out of range"},
		{name: "negative gid", rootfs: image(map[string]string{"etc/passwd": "beam:x:1:-1::/home/beam:\n", imageDesktopUserFile: "beam\n"}, nil), wantErr: "out of range"},
		{name: "desktop user is not a file", rootfs: notAFile, wantErr: "not a regular file"},
		{name: "no image", rootfs: filepath.Join(t.TempDir(), "missing"), wantErr: "no such file"},
	} {
		t.Run(test.name, func(t *testing.T) {
			placements, err := mirrorPlacements(test.rootfs, test.home)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, placements)
		})
	}
}

func TestMirrorLayerReadsThroughTheNodeCacheAndVerifies(t *testing.T) {
	h := newMirrorLayerHarness(t)
	pin := h.generation(t, 1, map[string]string{"a.txt": "alpha", "b.txt": "beta"})
	ctx := context.Background()
	manifestKey := types.MirrorObjectKey(h.mirrorID, pin.ManifestDigest)

	path, release, err := h.layers.acquire(ctx, mirrorRequest(7), pin, nil)
	require.NoError(t, err)
	content, err := readMirrorFile(t, h.mounts[path], "a.txt")
	require.NoError(t, err)
	require.Equal(t, "alpha", content)
	require.Len(t, h.cache.objects, 2, "the manifest and the pack are in the node cache")
	release()
	require.Equal(t, 1, h.layers.pruneIdle(time.Now().Add(time.Hour)))

	// A second worker on the node downloads nothing.
	downloads := func() int {
		total := 0
		for _, count := range h.bucket.downloads {
			total += count
		}
		return total
	}
	before := downloads()
	path, release, err = h.layers.acquire(ctx, mirrorRequest(7), pin, nil)
	require.NoError(t, err)
	content, err = readMirrorFile(t, h.mounts[path], "b.txt")
	require.NoError(t, err)
	require.Equal(t, "beta", content)
	require.Equal(t, before, downloads())
	release()
	require.Equal(t, 1, h.layers.pruneIdle(time.Now().Add(time.Hour)))

	// Objects that do not match their name are not served, and a failed
	// mount is retried by the next VM.
	h.cache.objects = map[string][]byte{}
	h.bucket.corrupt[manifestKey] = true
	_, _, err = h.layers.acquire(ctx, mirrorRequest(7), pin, nil)
	require.ErrorIs(t, err, mirror.ErrObjectMismatch)
	require.Empty(t, h.cache.objects)
	h.bucket.corrupt[manifestKey] = false
	_, release, err = h.layers.acquire(ctx, mirrorRequest(7), pin, nil)
	require.NoError(t, err)
	release()

	// A manifest the cache has damaged is refused before it is read.
	require.Equal(t, 1, h.layers.pruneIdle(time.Now().Add(time.Hour)))
	stored := h.bucket.objects[manifestKey]
	damaged := 0
	for _, content := range h.cache.objects {
		if bytes.HasPrefix(content, stored) {
			content[len(stored)-1] ^= 1
			damaged++
		}
	}
	require.Equal(t, 1, damaged)
	_, _, err = h.layers.acquire(ctx, mirrorRequest(7), pin, nil)
	require.ErrorIs(t, err, mirror.ErrObjectMismatch)

	for _, invalid := range []types.VMMirror{
		{ID: "../../etc", ManifestDigest: pin.ManifestDigest, ManifestSize: pin.ManifestSize},
		{ID: h.mirrorID, ManifestDigest: "../" + pin.ManifestDigest[3:], ManifestSize: pin.ManifestSize},
		{ID: h.mirrorID, ManifestDigest: pin.ManifestDigest, ManifestSize: 0},
		{ID: h.mirrorID, ManifestDigest: pin.ManifestDigest, ManifestSize: mirror.MaxManifestSize + 1},
	} {
		_, _, err = h.layers.acquire(ctx, mirrorRequest(7), &invalid, nil)
		require.Error(t, err, invalid)
	}
}

func TestMirrorLayersDoNotShareCachedObjects(t *testing.T) {
	h := newMirrorLayerHarness(t)
	ctx := context.Background()
	files := map[string]string{"secret.txt": "the same bytes in two mirrors"}
	downloads := func(mirrorID string) int {
		total := 0
		for key, count := range h.bucket.downloads {
			if strings.HasPrefix(key, types.MirrorPrefix(mirrorID)) {
				total += count
			}
		}
		return total
	}

	theirs := h.generation(t, 1, files)
	path, release, err := h.layers.acquire(ctx, mirrorRequest(7), theirs, nil)
	require.NoError(t, err)
	defer release()
	content, err := readMirrorFile(t, h.mounts[path], "secret.txt")
	require.NoError(t, err)
	require.Equal(t, files["secret.txt"], content)
	require.Len(t, h.cache.objects, 2)
	for key := range h.cache.objects {
		require.NotContains(t, h.bucket.objects, types.MirrorObjectKey(theirs.ID, key), "no object is cached under its own digest")
	}

	// Another workspace's mirror names the same objects, so its manifest
	// is the same bytes, but it has only the manifest in its store. The
	// gateway refuses such a commit; the worker does not depend on that.
	h.mirrorID = uuid.NewString()
	ours := h.generation(t, 1, files)
	require.Equal(t, theirs.ManifestDigest, ours.ManifestDigest)
	ourManifest := types.MirrorObjectKey(ours.ID, ours.ManifestDigest)
	held := map[string][]byte{}
	for key, data := range h.bucket.objects {
		if strings.HasPrefix(key, types.MirrorPrefix(ours.ID)) && key != ourManifest {
			held[key] = data
			delete(h.bucket.objects, key)
		}
	}
	require.Len(t, held, 1)
	path, releaseOurs, err := h.layers.acquire(ctx, mirrorRequest(8), ours, nil)
	require.NoError(t, err)
	require.Equal(t, 1, downloads(ours.ID), "the manifest came from this mirror's store, not the cache")
	_, err = readMirrorFile(t, h.mounts[path], "secret.txt")
	require.Error(t, err, "the cache holds the object, but not for this mirror")
	releaseOurs()

	// With the object in its own store the mirror reads it from there.
	for key, data := range held {
		h.bucket.objects[key] = data
	}
	content, err = readMirrorFile(t, h.mounts[path], "secret.txt")
	require.NoError(t, err)
	require.Equal(t, files["secret.txt"], content)
	require.Len(t, h.cache.objects, 4)

	// One mirror pinned by two workspaces, which the gateway never does,
	// shares nothing either.
	before := downloads(theirs.ID)
	path, releaseForeign, err := h.layers.acquire(ctx, mirrorRequest(9), theirs, nil)
	require.NoError(t, err)
	defer releaseForeign()
	_, err = readMirrorFile(t, h.mounts[path], "secret.txt")
	require.NoError(t, err)
	require.Equal(t, before+2, downloads(theirs.ID))
}

func TestPurgedMirrorLayerStopsServing(t *testing.T) {
	h := newMirrorLayerHarness(t)
	pin := h.generation(t, 1, map[string]string{"secret.txt": "do not keep"})
	ctx := context.Background()
	path, release, err := h.layers.acquire(ctx, mirrorRequest(7), pin, nil)
	require.NoError(t, err)
	fsys := h.mounts[path]
	content, err := readMirrorFile(t, fsys, "secret.txt")
	require.NoError(t, err)
	require.Equal(t, "do not keep", content)

	// An unreachable store revokes nothing.
	h.bucket.outage = true
	require.Zero(t, h.layers.revokePurged(ctx))
	h.bucket.outage = false
	require.Zero(t, h.layers.revokePurged(ctx))
	require.False(t, fsys.Revoked())

	// The purge deletes the mirror's objects. The node cache still has
	// them, and the layer has them in memory; neither is served again.
	h.bucket.objects = map[string][]byte{}
	require.Equal(t, 1, h.layers.revokePurged(ctx))
	require.True(t, fsys.Revoked())
	_, err = readMirrorFile(t, fsys, "secret.txt")
	require.ErrorIs(t, err, mirror.ErrRevoked)
	_, ok := fsys.Manifest().Find("secret.txt")
	require.True(t, ok, "names stay so the overlay above keeps working")
	require.Zero(t, h.unmounted.Load(), "the running VM keeps its mount")

	// A new VM cannot mount the purged generation from the cache.
	require.NotEmpty(t, h.cache.objects)
	_, _, err = h.layers.acquire(ctx, mirrorRequest(7), pin, nil)
	require.ErrorContains(t, err, "no longer in the store")
	release()
	require.Equal(t, int32(1), h.unmounted.Load(), "unmounted when its last VM stops")
}

// incompressible is content zstd cannot shrink, so that it is stored as is.
func incompressible(seed int64, size int) string {
	data := make([]byte, size)
	rand.New(rand.NewSource(seed)).Read(data)
	return string(data)
}

func TestPrefetchOrder(t *testing.T) {
	writer, err := mirror.NewWriter(mirror.DefaultWriterOptions(), func(mirror.Object, []byte) error { return nil })
	require.NoError(t, err)
	for _, dir := range []string{"projects", "projects/app", "projects/app/src"} {
		require.NoError(t, writer.AddDir(dir, 0o755, 0))
	}
	// Deep small files fill a pack before the home's own dotfile is added,
	// which lands in the next; a large file is chunked.
	for i := range 6 {
		require.NoError(t, writer.AddFile(fmt.Sprintf("projects/app/src/file%d", i), 0o644, 0, strings.NewReader(incompressible(int64(i), 200<<10))))
	}
	require.NoError(t, writer.AddFile(".zshrc", 0o644, 0, strings.NewReader("export EDITOR=vim\n")))
	require.NoError(t, writer.AddFile("projects/model.bin", 0o644, 0, strings.NewReader(incompressible(9, 5<<20))))
	_, manifest, err := writer.Finish(nil)
	require.NoError(t, err)
	objects := func(path string) []mirror.Object {
		t.Helper()
		index, ok := manifest.Find(path)
		require.True(t, ok, path)
		entry := manifest.Entry(index)
		var held []mirror.Object
		for i := entry.First; i < entry.First+entry.Count; i++ {
			held = append(held, manifest.Object(manifest.Extent(i).Object))
		}
		return held
	}
	dotfile, deep, chunks := objects(".zshrc"), objects("projects/app/src/file0"), objects("projects/model.bin")
	require.NotEqual(t, dotfile, deep)
	require.Greater(t, len(chunks), 1)

	// Packs come first, the shallow file's first, and every object once.
	require.Equal(t, slices.Concat(dotfile, deep, chunks), prefetchOrder(manifest))
	require.Len(t, prefetchOrder(manifest), int(manifest.Objects()))
}

func TestMirrorLayerPrefetchesIntoTheNodeCache(t *testing.T) {
	h := newMirrorLayerHarness(t)
	h.layers.prefetchBytes = 1 << 30
	files := map[string]string{"model.bin": incompressible(1, 5<<20)}
	for i := range 8 {
		files[fmt.Sprintf("file%d", i)] = incompressible(int64(10+i), 200<<10)
	}
	pin := h.generation(t, 1, files)
	objects := len(h.bucket.objects) - 1
	downloads := func() map[string]int {
		h.bucket.mu.Lock()
		defer h.bucket.mu.Unlock()
		return maps.Clone(h.bucket.downloads)
	}

	path, release, err := h.layers.acquire(context.Background(), mirrorRequest(7), pin, nil)
	require.NoError(t, err)
	defer release()
	require.Eventually(t, func() bool { return h.cache.len() == 1+objects }, 10*time.Second, 5*time.Millisecond, "every object and the manifest are in the node cache")
	fetched := downloads()
	require.Len(t, fetched, 1+objects)
	for key, count := range fetched {
		require.Equal(t, 1, count, key)
	}

	// The guest then reads every file without a download.
	for name, content := range files {
		read, err := readMirrorFile(t, h.mounts[path], name)
		require.NoError(t, err)
		require.Equal(t, content, read, name)
	}
	require.Equal(t, fetched, downloads())
}

func TestMirrorLayerPrefetchStops(t *testing.T) {
	files := map[string]string{}
	for i := range 10 {
		files[fmt.Sprintf("file%d", i)] = incompressible(int64(i), 300<<10)
	}
	ctx := context.Background()
	setup := func(t *testing.T, prefetchBytes int64, hold bool) (*mirrorLayerHarness, *types.VMMirror, string) {
		h := newMirrorLayerHarness(t)
		h.layers.prefetchBytes = prefetchBytes
		pin := h.generation(t, 1, files)
		manifestKey := types.MirrorObjectKey(h.mirrorID, pin.ManifestDigest)
		require.Greater(t, len(h.bucket.objects)-1, mirrorPrefetchConcurrency+1)
		if hold {
			h.bucket.hold = func(key string) bool { return key != manifestKey }
		}
		return h, pin, manifestKey
	}
	// Downloads held open fill every slot and no more; once stopped, no
	// other object is asked for.
	stopped := func(t *testing.T, h *mirrorLayerHarness, manifestKey string, stop func()) {
		require.Eventually(t, func() bool {
			inflight, _, _ := h.bucket.state(manifestKey)
			return inflight == mirrorPrefetchConcurrency
		}, 5*time.Second, time.Millisecond)
		stop()
		require.Never(t, func() bool {
			_, _, asked := h.bucket.state(manifestKey)
			return asked > mirrorPrefetchConcurrency
		}, time.Second, 10*time.Millisecond)
		require.Eventually(t, func() bool {
			inflight, _, _ := h.bucket.state(manifestKey)
			return inflight == 0
		}, 5*time.Second, time.Millisecond)
		_, peak, asked := h.bucket.state(manifestKey)
		require.Equal(t, mirrorPrefetchConcurrency, peak)
		require.Equal(t, mirrorPrefetchConcurrency, asked)
	}

	t.Run("at the byte cap", func(t *testing.T) {
		h, pin, manifestKey := setup(t, 1, false)
		_, release, err := h.layers.acquire(ctx, mirrorRequest(7), pin, nil)
		require.NoError(t, err)
		defer release()
		require.Eventually(t, func() bool {
			_, _, asked := h.bucket.state(manifestKey)
			return asked > 0
		}, 5*time.Second, time.Millisecond)
		require.Never(t, func() bool {
			_, _, asked := h.bucket.state(manifestKey)
			return asked > mirrorPrefetchConcurrency
		}, 200*time.Millisecond, 5*time.Millisecond)
	})

	t.Run("when unmounted", func(t *testing.T) {
		h, pin, manifestKey := setup(t, 1<<30, true)
		_, release, err := h.layers.acquire(ctx, mirrorRequest(7), pin, nil)
		require.NoError(t, err, "the mount does not wait for the prefetch")
		stopped(t, h, manifestKey, func() {
			release()
			require.Equal(t, 1, h.layers.pruneIdle(time.Now().Add(time.Hour)))
		})
	})

	t.Run("when revoked", func(t *testing.T) {
		h, pin, manifestKey := setup(t, 1<<30, true)
		_, release, err := h.layers.acquire(ctx, mirrorRequest(7), pin, nil)
		require.NoError(t, err)
		defer release()
		stopped(t, h, manifestKey, func() {
			h.bucket.mu.Lock()
			delete(h.bucket.objects, manifestKey)
			h.bucket.mu.Unlock()
			require.Equal(t, 1, h.layers.revokePurged(ctx))
		})
	})
}

func TestAttachMirrorLayer(t *testing.T) {
	h := newMirrorLayerHarness(t)
	pin := h.generation(t, 1, map[string]string{"a.txt": "alpha"})
	stub := func(config types.StubConfigV1) *types.ContainerRequest {
		request := mirrorRequest(7)
		data, err := json.Marshal(config)
		require.NoError(t, err)
		request.Stub.Config = string(data)
		region, key, bucket := "us-east-1", "key", "bucket"
		request.Workspace.Storage = &types.WorkspaceStorage{Region: &region, AccessKey: &key, SecretKey: &key, BucketName: &bucket}
		return request
	}
	worker := &Worker{mirrorLayers: h.layers}

	// Nothing happens for containers without a mirror.
	plain := stub(types.StubConfigV1{})
	overlay := common.NewContainerOverlay(plain, t.TempDir(), t.TempDir())
	home, release, err := worker.attachMirrorLayer(context.Background(), plain, overlay)
	require.NoError(t, err)
	require.Empty(t, home)
	release()
	require.Empty(t, h.mounts)

	fakeBin := t.TempDir()
	logPath := filepath.Join(fakeBin, "mount.log")
	require.NoError(t, os.WriteFile(filepath.Join(fakeBin, "mount"), []byte("#!/bin/sh\necho \"$*\" >> "+logPath+"\nexit 0\n"), 0o755))
	t.Setenv("PATH", fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))
	pinned := stub(types.StubConfigV1{Mirror: pin})
	image := t.TempDir()
	overlay = common.NewContainerOverlay(pinned, image, t.TempDir())
	home, release, err = worker.attachMirrorLayer(context.Background(), pinned, overlay)
	require.NoError(t, err)
	require.Empty(t, home, "a pin without a home leaves the guest's homes as they are")
	require.Len(t, h.mounts, 1)
	require.NoError(t, overlay.Setup())
	var layer string
	for path := range h.mounts {
		layer = path
	}
	options, err := os.ReadFile(logPath)
	require.NoError(t, err)
	require.Contains(t, string(options), "lowerdir="+layer+":"+image+",upperdir=", "the mirror sits between the image and the writable layer")
	release()
	require.Equal(t, 1, h.layers.pruneIdle(time.Now().Add(time.Hour)))

	// A pin with a home has the tree there, and says so for the launch.
	located := *pin
	located.Home = "/Users/luke"
	overlay = common.NewContainerOverlay(pinned, image, t.TempDir())
	home, release, err = worker.attachMirrorLayer(context.Background(), stub(types.StubConfigV1{Mirror: &located}), overlay)
	require.NoError(t, err)
	require.Equal(t, "/Users/luke", home)
	require.Len(t, h.mounts, 2)
	for path, fsys := range h.mounts {
		if path != layer {
			require.Equal(t, []mirror.Placement{{Path: "/Users/luke", Mode: 0o750}}, fsys.Placements(), "the image has no desktop user")
		}
	}
	release()
	require.Equal(t, 1, h.layers.pruneIdle(time.Now().Add(time.Hour)))

	// The home came from a laptop: anything but a home is refused.
	for _, invalid := range []string{"/etc", "/root", "/Users/luke/..", "/Users/../etc", "Users/luke", "/home/a:b"} {
		located.Home = invalid
		_, _, err = worker.attachMirrorLayer(context.Background(), stub(types.StubConfigV1{Mirror: &located}), overlay)
		require.ErrorContains(t, err, "invalid mirror home", invalid)
	}
	require.Len(t, h.mounts, 2)

	// A VM whose generation cannot be mounted does not start without it.
	h.bucket.objects = map[string][]byte{}
	overlay = common.NewContainerOverlay(pinned, image, t.TempDir())
	_, _, err = worker.attachMirrorLayer(context.Background(), pinned, overlay)
	require.ErrorContains(t, err, "no longer in the store")
	_, _, err = (&Worker{}).attachMirrorLayer(context.Background(), pinned, overlay)
	require.Error(t, err)
}
