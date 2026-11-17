package worker

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/disk"
	"github.com/beam-cloud/beta9/pkg/runtime"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
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
	if os.Geteuid() != 0 {
		t.Skip("mounting an export needs root")
	}
	if _, err := exec.LookPath("mkfs.ext4"); err != nil {
		t.Skip("mounting an export needs mkfs.ext4")
	}
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

	removeRootLayerExport(dir)
	require.NoDirExists(t, dir)
	require.NoFileExists(t, dir+".img")
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
