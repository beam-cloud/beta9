package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
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

func (b *fakeMirrorBucket) DownloadWithReader(_ context.Context, key string) (io.ReadCloser, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.objects[key]
	if !ok || b.outage {
		return nil, errors.New("no such object")
	}
	b.downloads[key]++
	if b.corrupt[key] {
		data = append([]byte("x"), data[1:]...)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
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

type fakeMirrorMount struct{ unmounted *atomic.Int32 }

func (m fakeMirrorMount) Unmount() error {
	m.unmounted.Add(1)
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

	for _, test := range []struct {
		name    string
		rootfs  string
		want    []mirror.Placement
		wantErr string
	}{
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
			placements, err := mirrorPlacements(test.rootfs)
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
	release, err := worker.attachMirrorLayer(context.Background(), plain, overlay)
	require.NoError(t, err)
	release()
	require.Empty(t, h.mounts)

	fakeBin := t.TempDir()
	logPath := filepath.Join(fakeBin, "mount.log")
	require.NoError(t, os.WriteFile(filepath.Join(fakeBin, "mount"), []byte("#!/bin/sh\necho \"$*\" >> "+logPath+"\nexit 0\n"), 0o755))
	t.Setenv("PATH", fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))
	pinned := stub(types.StubConfigV1{Mirror: pin})
	image := t.TempDir()
	overlay = common.NewContainerOverlay(pinned, image, t.TempDir())
	release, err = worker.attachMirrorLayer(context.Background(), pinned, overlay)
	require.NoError(t, err)
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

	// A VM whose generation cannot be mounted does not start without it.
	h.bucket.objects = map[string][]byte{}
	overlay = common.NewContainerOverlay(pinned, image, t.TempDir())
	_, err = worker.attachMirrorLayer(context.Background(), pinned, overlay)
	require.ErrorContains(t, err, "no longer in the store")
	_, err = (&Worker{}).attachMirrorLayer(context.Background(), pinned, overlay)
	require.Error(t, err)
}
