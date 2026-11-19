package worker

import (
	"archive/tar"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/disk"
	"github.com/beam-cloud/beta9/pkg/runtime"
	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/require"
)

func rootLayerRequest(t *testing.T, layer *types.RootLayer, skipExitSnapshot bool) *types.ContainerRequest {
	t.Helper()
	config, err := json.Marshal(types.StubConfigV1{RootLayer: layer})
	require.NoError(t, err)
	return &types.ContainerRequest{
		ContainerId: "vm-1",
		WorkspaceId: "workspace-1",
		Stub:        types.StubWithRelated{Stub: types.Stub{Config: string(config)}},
		Mounts: []types.Mount{{
			MountPath: types.DurableDiskRootMountPath,
			DurableDisk: &types.DurableDiskMountConfig{
				Name: "root", Size: "1Gi", Driver: types.DurableDiskDriverQcow, SkipExitSnapshot: skipExitSnapshot,
			},
		}},
	}
}

func rootLayerWorker(t *testing.T, capabilities runtime.Capabilities) *Worker {
	t.Helper()
	request := &types.ContainerRequest{ContainerId: "unused"}
	worker := newContainerFinalizationTestWorker(request, &fakeContainerRepoClient{}, NewMockRuntime("microvm", capabilities))
	worker.diskManager = disk.NewManager(disk.Config{Root: t.TempDir()})
	worker.rootLayers = newRootLayers(worker)
	return worker
}

// mountsRootLayerExports skips a test that mounts an export where it cannot.
func mountsRootLayerExports(t *testing.T) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("mounting an export needs root")
	}
	if _, err := exec.LookPath("mkfs.ext4"); err != nil {
		t.Skip("mounting an export needs mkfs.ext4")
	}
}

// A layer's files reach a root disk as references to the layer. A root that
// is restored may come back over another generation, or none, so only a VM
// whose root is thrown away gets one.
func TestRootLayerIsOnlyForARootThatIsNeverRestored(t *testing.T) {
	layer := &types.RootLayer{DiskName: types.RootLayerDiskPrefix + "0123456789abcdef", Size: "1Gi", SnapshotID: "generation-1"}
	vm := rootLayerWorker(t, runtime.Capabilities{BlockRoot: true})

	require.Equal(t, layer, vm.requestRootLayer(rootLayerRequest(t, layer, true)))

	require.Nil(t, vm.requestRootLayer(rootLayerRequest(t, layer, false)), "the root is captured when the VM exits")
	require.Nil(t, vm.requestRootLayer(rootLayerRequest(t, nil, true)), "no layer was asked for")
	require.Nil(t, vm.requestRootLayer(rootLayerRequest(t, &types.RootLayer{DiskName: "data", SnapshotID: "generation-1"}, true)),
		"a workspace's own disk is not a layer")

	withoutRoot := rootLayerRequest(t, layer, true)
	withoutRoot.Mounts = nil
	require.Nil(t, vm.requestRootLayer(withoutRoot), "the VM has no root disk")

	container := rootLayerWorker(t, runtime.Capabilities{})
	require.Nil(t, container.requestRootLayer(rootLayerRequest(t, layer, true)), "a container's root is not an overlay in a guest")
}

// A VM starts on whatever is attached, and without a layer when nothing is.
func TestRootLayerIsNeverWaitedFor(t *testing.T) {
	worker := rootLayerWorker(t, runtime.Capabilities{BlockRoot: true})
	layers := worker.rootLayers

	// A disk nothing was published to has nothing to attach.
	empty := &types.RootLayer{DiskName: types.RootLayerDiskPrefix + "0123456789abcdef", Size: "1Gi"}
	dir, release := layers.acquire(rootLayerRequest(t, empty, true))
	require.Empty(t, dir)
	release()
	require.Empty(t, layers.disks, "nothing is attached for a disk without a generation")

	var none *rootLayers
	dir, release = none.acquire(rootLayerRequest(t, empty, true))
	require.Empty(t, dir)
	release()
}

// A generation that a newer one replaced stays until the last VM on it is
// gone, and a generation no VM has used for a while is detached.
func TestRootLayerGenerationOutlivesTheVMsOnIt(t *testing.T) {
	worker := rootLayerWorker(t, runtime.Capabilities{BlockRoot: true})
	layers := worker.rootLayers

	mountDir := func(name string) string {
		dir := filepath.Join(t.TempDir(), name)
		require.NoError(t, os.MkdirAll(dir, 0o755))
		return dir
	}
	old := &rootLayerMount{snapshotID: "generation-1", key: "workspace-1-layer-generation-1", dir: mountDir("old"), refs: 2}
	current := &rootLayerMount{snapshotID: "generation-2", key: "workspace-1-layer-generation-2", dir: mountDir("current"), usedAt: time.Now()}
	entry := &rootLayerDisk{ready: current, retired: []*rootLayerMount{old}}
	layers.disks["workspace-1/"+types.RootLayerDiskPrefix+"0123456789abcdef"] = entry

	layers.release(entry, old)
	require.Len(t, entry.retired, 1, "a VM is still on the old generation")
	require.DirExists(t, old.dir)

	layers.release(entry, old)
	require.Empty(t, entry.retired)
	require.NoDirExists(t, old.dir)

	// The current generation is kept while it is in use or was used lately.
	current.refs = 1
	current.usedAt = time.Now().Add(-2 * rootLayerIdle)
	layers.prune()
	require.Same(t, current, entry.ready)

	layers.release(entry, current)
	require.Same(t, current, entry.ready, "the newest generation waits for the next VM")
	require.DirExists(t, current.dir)

	current.usedAt = time.Now().Add(-2 * rootLayerIdle)
	layers.prune()
	require.Nil(t, entry.ready)
	require.NoDirExists(t, current.dir)
	require.Empty(t, layers.disks)
}

// The entry of a disk is what holds publishes to it to one at a time on a
// worker, so it stays while one is under way or waiting, though no VM has
// the disk.
func TestRootLayerDiskStaysWhilePublishedTo(t *testing.T) {
	worker := rootLayerWorker(t, runtime.Capabilities{BlockRoot: true})
	layers := worker.rootLayers
	key := "workspace-1/" + types.RootLayerDiskPrefix + "0123456789abcdef"

	first, firstDone := layers.publisher(key)
	first.publish.Lock()
	layers.prune()
	second, secondDone := layers.publisher(key)
	require.Same(t, first, second)
	require.False(t, second.publish.TryLock(), "a second publish waits for the first")

	first.publish.Unlock()
	firstDone()
	layers.prune()
	require.Same(t, second, layers.disks[key])

	secondDone()
	layers.prune()
	require.Empty(t, layers.disks)
}

// A guest writes its export into a filesystem of its own, and can fill only
// that, whatever it writes.
func TestRootLayerExportIsBounded(t *testing.T) {
	mountsRootLayerExports(t)
	dir := filepath.Join(t.TempDir(), "container-1")
	const size = 64 << 20
	require.NoError(t, makeRootLayerExport(dir, size))
	t.Cleanup(func() { removeRootLayerExport(dir) })

	file, err := os.Create(filepath.Join(dir, "fill"))
	require.NoError(t, err)
	chunk := make([]byte, 1<<20)
	var written int64
	for ; written <= 2*size; written += int64(len(chunk)) {
		if _, err = file.Write(chunk); err != nil {
			break
		}
	}
	require.ErrorIs(t, err, syscall.ENOSPC)
	require.NoError(t, file.Close())
	require.Less(t, written, int64(size))

	var image syscall.Stat_t
	require.NoError(t, syscall.Stat(dir+".img", &image))
	require.LessOrEqual(t, image.Blocks*512, int64(size), "the worker's disk gives no more than the export's size")

	out, err := exec.Command("losetup", "--noheadings", "--output", "DIO,AUTOCLEAR", "--associated", dir+".img").Output()
	require.NoError(t, err)
	require.Equal(t, []string{"1", "1"}, strings.Fields(string(out)), "the image is written past the page cache, and detached with the export")

	removeRootLayerExport(dir)
	require.NoDirExists(t, dir)
	require.NoFileExists(t, dir+".img")
	out, err = exec.Command("losetup", "--noheadings", "--associated", dir+".img").Output()
	require.NoError(t, err)
	require.Empty(t, strings.TrimSpace(string(out)), "no loop device is left")
}

// A worker in a container has the loop devices that its host had when it
// started. With all of those in use the kernel adds one, which /dev has no
// file for until the worker makes it.
func TestRootLayerExportAddsTheLoopDeviceItNeeds(t *testing.T) {
	mountsRootLayerExports(t)
	// Only a container's own /dev is taken apart: a host's is its devices.
	if out, err := exec.Command("findmnt", "-no", "FSTYPE", "/dev").Output(); err != nil || strings.TrimSpace(string(out)) != "tmpfs" {
		t.Skip("needs a /dev of its own, as a container has")
	}
	devices, err := filepath.Glob("/dev/loop[0-9]*")
	require.NoError(t, err)
	require.NotEmpty(t, devices)
	for _, device := range devices {
		require.NoError(t, os.Remove(device))
	}
	t.Cleanup(addLoopDevices)

	dir := filepath.Join(t.TempDir(), "vm-1")
	t.Cleanup(func() { removeRootLayerExport(dir) })
	require.NoError(t, makeRootLayerExport(dir, 64<<20))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "entries.tar"), []byte("entries"), 0o600))
	for _, device := range devices {
		require.FileExists(t, device)
	}
}

// A start can fail after the export of its VM is mounted: the worker waits
// for a VM that it evicts, or is told to stop. No exit collects that export,
// so the failure removes it, with its loop device.
func TestRootLayerExportOfAStartThatFailedIsRemoved(t *testing.T) {
	mountsRootLayerExports(t)
	worker := rootLayerWorker(t, runtime.Capabilities{BlockRoot: true})
	request := rootLayerRequest(t, &types.RootLayer{DiskName: types.RootLayerDiskPrefix + "0123456789abcdef", Size: "1Gi",
		Paths: []string{"/home/runner/.cache/go-build"}, Publish: true}, true)
	dir := worker.rootLayerExportDir(request.ContainerId)
	t.Cleanup(func() { removeRootLayerExport(dir) })

	spec := &specs.Spec{Annotations: map[string]string{}}
	require.NoError(t, worker.annotateRootLayer(request, spec))
	require.Len(t, spec.Mounts, 1)
	require.Equal(t, dir, spec.Mounts[0].Source)
	require.FileExists(t, dir+".img")

	worker.discardRootLayerExport(request.ContainerId, request)
	require.NoDirExists(t, dir)
	require.NoFileExists(t, dir+".img")
	out, err := exec.Command("losetup", "--noheadings", "--associated", dir+".img").Output()
	require.NoError(t, err)
	require.Empty(t, strings.TrimSpace(string(out)), "no loop device is left")

	// A VM that does not add to its layer has no export to remove.
	reader := rootLayerRequest(t, &types.RootLayer{DiskName: types.RootLayerDiskPrefix + "0123456789abcdef", Size: "1Gi"}, true)
	require.NoError(t, worker.annotateRootLayer(reader, &specs.Spec{Annotations: map[string]string{}}))
	require.NoDirExists(t, dir)
}

// VMs that were told of an older generation can start after ones that were
// told of a newer one. A worker never goes back for them.
func TestRootLayerOnlyMovesToANewerGeneration(t *testing.T) {
	worker := rootLayerWorker(t, runtime.Capabilities{BlockRoot: true})
	layers := worker.rootLayers
	attached := true
	layers.attached = func(string) bool { return attached }
	name := types.RootLayerDiskPrefix + "0123456789abcdef"
	backend := &fakeBackendRepoClient{sourceSnapshot: &pb.DiskSnapshot{ExternalId: "generation-1", DiskName: name, Generation: 10}}
	worker.backendRepoClient = backend

	dir := filepath.Join(t.TempDir(), "current")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	current := &rootLayerMount{snapshotID: "generation-2", generation: 20, key: "workspace-1-layer-generation-2", dir: dir}
	entry := &rootLayerDisk{ready: current, attaching: "generation-1"}
	layers.disks["workspace-1/"+name] = entry

	old := &types.RootLayer{DiskName: name, Size: "1Gi", SnapshotID: "generation-1"}
	request := rootLayerRequest(t, old, true)
	layers.attach("workspace-1/"+name, entry, request, *old)
	require.Equal(t, "generation-1", backend.requestedSnapshotId)
	require.Same(t, current, entry.ready)
	require.Empty(t, entry.attaching)
	require.Empty(t, entry.retired)

	// The VMs that ask for it get the newer one, and it is not looked up again.
	backend.requestedSnapshotId = ""
	for range 3 {
		got, release := layers.acquire(request)
		require.Equal(t, dir, got)
		release()
	}
	require.Empty(t, entry.attaching)
	require.Empty(t, backend.requestedSnapshotId)

	// Once the newer one is gone, an older generation is better than none.
	attached = false
	backend.getDiskSnapshotErr = errors.New("not now")
	got, release := layers.acquire(request)
	release()
	require.Empty(t, got)
	require.Eventually(t, func() bool {
		layers.mu.Lock()
		defer layers.mu.Unlock()
		// The attach has ended by the time it is no longer marked.
		return entry.attaching == "" && backend.requestedSnapshotId == "generation-1"
	}, 5*time.Second, 10*time.Millisecond, "the older generation is asked for again")
}

type cacheEntry struct {
	name     string
	kind     byte
	content  string
	mode     int64
	uid      int
	modTime  time.Time
	touched  bool
	linkname string
}

// writeCacheExportTar writes the tar a guest would, with whatever entries.
func writeCacheExportTar(t *testing.T, entries []cacheEntry) string {
	t.Helper()
	file, err := os.Create(filepath.Join(t.TempDir(), microvm.CacheExportFile))
	require.NoError(t, err)
	defer file.Close()
	writer := tar.NewWriter(file)
	for _, entry := range entries {
		// A test that is not root's can give files only to itself.
		gid := entry.uid
		if entry.uid == os.Getuid() {
			gid = os.Getgid()
		}
		header := &tar.Header{
			Name: entry.name, Typeflag: entry.kind, Mode: entry.mode, Linkname: entry.linkname,
			Uid: entry.uid, Gid: gid, ModTime: entry.modTime, Format: tar.FormatPAX,
		}
		if entry.kind == tar.TypeReg && !entry.touched {
			header.Size = int64(len(entry.content))
		}
		if entry.touched {
			header.PAXRecords = map[string]string{microvm.CacheExportTouched: "1"}
		}
		require.NoError(t, writer.WriteHeader(header))
		if header.Size > 0 {
			_, err := writer.Write([]byte(entry.content))
			require.NoError(t, err)
		}
	}
	require.NoError(t, writer.Close())
	return file.Name()
}

func cacheDirs(uid int, names ...string) []cacheEntry {
	entries := make([]cacheEntry, 0, len(names))
	for _, name := range names {
		entries = append(entries, cacheEntry{name: name + "/", kind: tar.TypeDir, mode: 0o755, uid: uid})
	}
	return entries
}

var cacheTestDirs = []string{"/home/runner/.cache/go-build", "/home/runner/go/pkg/mod"}

func TestMergeCacheExportAddsFilesBelowTheLayersDirectories(t *testing.T) {
	uid := os.Getuid()
	written := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	export := writeCacheExportTar(t, append(
		cacheDirs(uid, "home", "home/runner", "home/runner/.cache", "home/runner/.cache/go-build", "home/runner/.cache/go-build/ab"),
		cacheEntry{name: "home/runner/.cache/go-build/ab/abcd-d", kind: tar.TypeReg, content: "object", mode: 0o644, uid: uid, modTime: written},
		// Set-id bits never reach the layer.
		cacheEntry{name: "home/runner/.cache/go-build/ab/tool", kind: tar.TypeReg, content: "#!", mode: 0o6755, uid: uid, modTime: written},
	))
	layer := t.TempDir()

	stats, err := mergeCacheExport(export, layer, cacheTestDirs, 0)
	require.NoError(t, err)
	require.Equal(t, 2, stats.files)
	require.EqualValues(t, len("object")+len("#!"), stats.bytes)
	require.Zero(t, stats.refused)

	object := filepath.Join(layer, "home/runner/.cache/go-build/ab/abcd-d")
	content, err := os.ReadFile(object)
	require.NoError(t, err)
	require.Equal(t, "object", string(content))
	info, err := os.Stat(object)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o644), info.Mode())
	require.True(t, info.ModTime().Equal(written))

	info, err = os.Stat(filepath.Join(layer, "home/runner/.cache/go-build/ab/tool"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o755), info.Mode(), "no set-id bit, and still a program")

	info, err = os.Stat(filepath.Join(layer, "home/runner/.cache"))
	require.NoError(t, err)
	require.Equal(t, os.ModeDir|0o755, info.Mode())

	// Nothing is left of the names files are written under.
	leftovers, err := filepath.Glob(filepath.Join(layer, "home/runner/.cache/go-build/ab/*"+cacheMergeSuffix))
	require.NoError(t, err)
	require.Empty(t, leftovers)
}

func TestMergeCacheExportRefusesWhatIsNotACacheFile(t *testing.T) {
	uid := os.Getuid()
	outside := t.TempDir()
	export := writeCacheExportTar(t, append(
		cacheDirs(uid, "home", "home/runner", "home/runner/.cache", "home/runner/.cache/go-build", "etc", "home/runner/.ssh"),
		// Not below one of the layer's directories.
		cacheEntry{name: "etc/passwd", kind: tar.TypeReg, content: "root::0:0", mode: 0o644, uid: uid},
		cacheEntry{name: "home/runner/.ssh/authorized_keys", kind: tar.TypeReg, content: "key", mode: 0o600, uid: uid},
		cacheEntry{name: "home/runner/.cache/go-build", kind: tar.TypeReg, content: "file", mode: 0o644, uid: uid},
		// Names that would leave the layer.
		cacheEntry{name: "home/runner/.cache/go-build/../../../../escape", kind: tar.TypeReg, content: "x", mode: 0o644, uid: uid},
		cacheEntry{name: "../escape", kind: tar.TypeReg, content: "x", mode: 0o644, uid: uid},
		// Anything that is not a file or a directory.
		cacheEntry{name: "home/runner/.cache/go-build/link", kind: tar.TypeSymlink, linkname: outside, uid: uid},
		cacheEntry{name: "home/runner/.cache/go-build/hard", kind: tar.TypeLink, linkname: "etc/passwd", uid: uid},
		cacheEntry{name: "home/runner/.cache/go-build/device", kind: tar.TypeChar, mode: 0o666, uid: uid},
		cacheEntry{name: "home/runner/.cache/go-build/pipe", kind: tar.TypeFifo, mode: 0o666, uid: uid},
		// A file whose directory the export did not bring.
		cacheEntry{name: "home/runner/.cache/go-build/missing/entry", kind: tar.TypeReg, content: "x", mode: 0o644, uid: uid},
		cacheEntry{name: "home/runner/.cache/go-build/kept", kind: tar.TypeReg, content: "kept", mode: 0o644, uid: uid},
	))
	layer := t.TempDir()

	stats, err := mergeCacheExport(export, layer, cacheTestDirs, 0)
	require.NoError(t, err)
	require.Equal(t, 1, stats.files)
	require.Equal(t, 12, stats.refused)

	var found []string
	require.NoError(t, filepath.WalkDir(layer, func(path string, entry os.DirEntry, err error) error {
		if err == nil && !entry.IsDir() {
			found = append(found, path)
		}
		return err
	}))
	require.Equal(t, []string{filepath.Join(layer, "home/runner/.cache/go-build/kept")}, found)
	require.NoDirExists(t, filepath.Join(layer, "etc"))
	require.NoDirExists(t, filepath.Join(layer, "home/runner/.ssh"))
	escaped, err := os.ReadDir(outside)
	require.NoError(t, err)
	require.Empty(t, escaped)
	require.NoFileExists(t, filepath.Join(filepath.Dir(layer), "escape"))
}

func TestMergeCacheExportBringsForwardTheTimeOfAFileThatWasUsed(t *testing.T) {
	uid := os.Getuid()
	layer := t.TempDir()
	dir := filepath.Join(layer, "home/runner/.cache/go-build/ab")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	old := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	for _, name := range []string{"used", "rewritten", "unused"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("before"), 0o644))
		require.NoError(t, os.Chtimes(filepath.Join(dir, name), old, old))
	}

	used := time.Date(2026, 11, 17, 9, 0, 0, 0, time.UTC)
	export := writeCacheExportTar(t, append(
		cacheDirs(uid, "home", "home/runner", "home/runner/.cache", "home/runner/.cache/go-build", "home/runner/.cache/go-build/ab"),
		cacheEntry{name: "home/runner/.cache/go-build/ab/used", kind: tar.TypeReg, touched: true, mode: 0o644, uid: uid, modTime: used},
		// The layer has no such file, so there is nothing to bring forward.
		cacheEntry{name: "home/runner/.cache/go-build/ab/gone", kind: tar.TypeReg, touched: true, mode: 0o644, uid: uid, modTime: used},
		cacheEntry{name: "home/runner/.cache/go-build/ab/rewritten", kind: tar.TypeReg, content: "after", mode: 0o644, uid: uid, modTime: used},
	))

	stats, err := mergeCacheExport(export, layer, cacheTestDirs, 0)
	require.NoError(t, err)
	require.Equal(t, 1, stats.touched)
	require.Equal(t, 1, stats.files)

	info, err := os.Stat(filepath.Join(dir, "used"))
	require.NoError(t, err)
	require.True(t, info.ModTime().Equal(used))
	content, err := os.ReadFile(filepath.Join(dir, "used"))
	require.NoError(t, err)
	require.Equal(t, "before", string(content), "a header carries no content")

	content, err = os.ReadFile(filepath.Join(dir, "rewritten"))
	require.NoError(t, err)
	require.Equal(t, "after", string(content))

	info, err = os.Stat(filepath.Join(dir, "unused"))
	require.NoError(t, err)
	require.True(t, info.ModTime().Equal(old))
	require.NoFileExists(t, filepath.Join(dir, "gone"))
}

func TestMergeCacheExportNeverFollowsALinkOutOfTheLayer(t *testing.T) {
	uid := os.Getuid()
	layer, outside := t.TempDir(), t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(layer, "home/runner/.cache"), 0o755))
	// No merge makes a link. One that is there anyway leads nowhere.
	require.NoError(t, os.Symlink(outside, filepath.Join(layer, "home/runner/.cache/go-build")))

	export := writeCacheExportTar(t, append(
		cacheDirs(uid, "home", "home/runner", "home/runner/.cache", "home/runner/.cache/go-build"),
		cacheEntry{name: "home/runner/.cache/go-build/entry", kind: tar.TypeReg, content: "x", mode: 0o644, uid: uid},
	))

	stats, err := mergeCacheExport(export, layer, cacheTestDirs, 0)
	require.Zero(t, stats.files)
	if err == nil {
		require.NotZero(t, stats.refused)
	}
	escaped, readErr := os.ReadDir(outside)
	require.NoError(t, readErr)
	require.Empty(t, escaped)
}

func TestMergeCacheExportStopsWhenTheLayerIsFull(t *testing.T) {
	uid := os.Getuid()
	export := writeCacheExportTar(t, append(
		cacheDirs(uid, "home", "home/runner", "home/runner/.cache", "home/runner/.cache/go-build"),
		cacheEntry{name: "home/runner/.cache/go-build/entry", kind: tar.TypeReg, content: "x", mode: 0o644, uid: uid},
	))
	layer := t.TempDir()

	// More is to stay free than the filesystem has.
	stats, err := mergeCacheExport(export, layer, cacheTestDirs, 1<<62)
	require.NoError(t, err)
	require.True(t, stats.full)
	require.Zero(t, stats.files)
	require.NoFileExists(t, filepath.Join(layer, "home/runner/.cache/go-build/entry"))
}

func TestMergeCacheExportRefusesADirectoryItsJobCouldNotWriteIn(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("giving files to other users requires root")
	}
	const runner = 1001
	export := writeCacheExportTar(t, []cacheEntry{
		{name: "home/", kind: tar.TypeDir, mode: 0o755, uid: 0},
		{name: "home/runner/", kind: tar.TypeDir, mode: 0o755, uid: runner},
		// Made with sudo: every later job would find a directory of root's
		// where it expects to make its own.
		{name: "home/runner/.cache/", kind: tar.TypeDir, mode: 0o755, uid: 0},
		{name: "home/runner/.cache/go-build/", kind: tar.TypeDir, mode: 0o755, uid: 0},
		{name: "home/runner/.cache/go-build/entry", kind: tar.TypeReg, content: "x", mode: 0o644, uid: 0},
		{name: "home/runner/go/", kind: tar.TypeDir, mode: 0o755, uid: runner},
		{name: "home/runner/go/pkg/", kind: tar.TypeDir, mode: 0o755, uid: runner},
		{name: "home/runner/go/pkg/mod/", kind: tar.TypeDir, mode: 0o755, uid: runner},
		{name: "home/runner/go/pkg/mod/module.zip", kind: tar.TypeReg, content: "zip", mode: 0o444, uid: runner},
	})
	layer := t.TempDir()

	stats, err := mergeCacheExport(export, layer, cacheTestDirs, 0)
	require.NoError(t, err)
	require.Equal(t, 1, stats.files)
	require.NoDirExists(t, filepath.Join(layer, "home/runner/.cache"))
	require.FileExists(t, filepath.Join(layer, "home/runner/go/pkg/mod/module.zip"))
}

// Only a root that goes with its container may skip flushes: a worker
// publishes what it holds of a captured one, crash or not.
func TestAnnotateRootDiskMarksOnlyAnUncapturedRootVolatile(t *testing.T) {
	for _, uncaptured := range []bool{false, true} {
		request := rootLayerRequest(t, nil, uncaptured)
		spec := &specs.Spec{Annotations: map[string]string{}}
		annotateRootDisk(spec, qcowRootDiskMount(request), "/run/qcow/root/vhost.sock")

		require.Equal(t, "/run/qcow/root/vhost.sock", spec.Annotations[runtime.MicroVMRootDiskAnnotation])
		_, volatile := spec.Annotations[runtime.MicroVMRootVolatileAnnotation]
		require.Equal(t, uncaptured, volatile)
	}
}
