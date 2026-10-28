package disk

import (
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

type countingChunkStore struct {
	*memoryChunkStore
	reads atomic.Int64
}

func (s *countingChunkStore) ReadChunk(ctx context.Context, chunk types.DiskSnapshotChunk, dest []byte) error {
	s.reads.Add(1)
	return s.memoryChunkStore.ReadChunk(ctx, chunk, dest)
}

// publishTestChain uploads one random layer per snapshot ID and returns the
// chain an attach would name, base first, with the layers' contents.
func publishTestChain(t *testing.T, store *countingChunkStore, ids ...string) ([]ChainLayer, [][]byte) {
	t.Helper()
	chain := make([]ChainLayer, len(ids))
	contents := make([][]byte, len(ids))
	for i, id := range ids {
		contents[i] = make([]byte, 256<<10)
		_, _ = rand.Read(contents[i])
		path := filepath.Join(t.TempDir(), id+".qcow2")
		require.NoError(t, os.WriteFile(path, contents[i], 0o600))
		layer, err := ScanLayer(context.Background(), path, chunkKey)
		require.NoError(t, err)
		require.NoError(t, UploadLayer(context.Background(), store, path, layer))
		chain[i] = ChainLayer{SnapshotID: id, Layer: layer}
	}
	return chain, contents
}

type rebaseRecorder struct {
	mu      sync.Mutex
	rebases [][2]string // image, new backing file
}

func (r *rebaseRecorder) run(ctx context.Context, name string, args ...string) ([]byte, error) {
	if strings.Contains(name, "qemu-img") && len(args) > 0 && args[0] == "rebase" {
		backing := args[slices.Index(args, "-b")+1]
		r.mu.Lock()
		r.rebases = append(r.rebases, [2]string{args[len(args)-1], backing})
		r.mu.Unlock()
	}
	return fakeRunner(ctx, name, args...)
}

func TestMaterializeSharesLayersBetweenVolumes(t *testing.T) {
	ctx := context.Background()
	recorder := &rebaseRecorder{}
	manager := NewManager(Config{Root: t.TempDir(), Runner: recorder.run})
	store := &countingChunkStore{memoryChunkStore: newMemoryChunkStore()}
	chain, contents := publishTestChain(t, store, "snap-a", "snap-b")
	chunks := int64(len(chain[0].Layer.Chunks) + len(chain[1].Layer.Chunks))

	pathA, err := manager.sharedLayerPath("snap-a", "")
	require.NoError(t, err)
	pathB, err := manager.sharedLayerPath("snap-b", "snap-a")
	require.NoError(t, err)

	for _, key := range []string{"vol-a", "vol-b"} {
		layersDir := filepath.Join(manager.volumeDir(key), layersSubdir)
		require.NoError(t, os.MkdirAll(layersDir, 0o700))
		state, err := manager.materializeChain(ctx, AttachSpec{Key: key, VirtualSizeBytes: 1 << 30, Chain: chain}, layersDir, store)
		require.NoError(t, err)
		require.Equal(t, []stateLayer{{SnapshotID: "snap-a", Path: pathA}, {SnapshotID: "snap-b", Path: pathB}}, state.Chain)
		require.Equal(t, layersDir, filepath.Dir(state.HeadPath), "the head is the volume's own")
		require.Equal(t, chunks, store.reads.Load(), "a layer on the node is fetched once")
	}

	for i, path := range []string{pathA, pathB} {
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, contents[i], data)
	}
	entries, err := os.ReadDir(manager.sharedLayersDir())
	require.NoError(t, err)
	require.Len(t, entries, 2, "no partial fetch stays behind")
	require.Equal(t, [][2]string{{pathB + partialLayerSuffix, pathA}}, recorder.rebases,
		"a layer is rebased onto its parent's copy before it is installed")
}

func TestConcurrentMaterializationsFetchEachLayerOnce(t *testing.T) {
	manager := NewManager(Config{Root: t.TempDir(), Runner: fakeRunner})
	store := &countingChunkStore{memoryChunkStore: newMemoryChunkStore()}
	chain, _ := publishTestChain(t, store, "snap-a", "snap-b", "snap-c")
	var chunks int64
	for _, layer := range chain {
		chunks += int64(len(layer.Layer.Chunks))
	}

	var group sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		group.Add(1)
		go func() {
			defer group.Done()
			key := "vol-" + string(rune('a'+i))
			layersDir := filepath.Join(manager.volumeDir(key), layersSubdir)
			if errs[i] = os.MkdirAll(layersDir, 0o700); errs[i] == nil {
				_, errs[i] = manager.materializeChain(context.Background(), AttachSpec{Key: key, VirtualSizeBytes: 1 << 30, Chain: chain}, layersDir, store)
			}
		}()
	}
	group.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, chunks, store.reads.Load())
}

func TestSharedLayerPathNamesTheParent(t *testing.T) {
	manager := NewManager(Config{Root: t.TempDir(), Runner: fakeRunner})
	onA, err := manager.sharedLayerPath("snap-b", "snap-a")
	require.NoError(t, err)
	onOther, err := manager.sharedLayerPath("snap-b", "snap-other")
	require.NoError(t, err)
	require.NotEqual(t, onA, onOther)
	require.True(t, manager.isSharedLayer(onA))
	for _, id := range []string{"", "../snap", "a/b", ".hidden"} {
		_, err := manager.sharedLayerPath(id, "")
		require.Error(t, err, id)
	}
}

// newSharedChainVolume returns a volume whose two lowest published layers are
// in the node's layer store and whose other two are its own.
func newSharedChainVolume(t *testing.T) (*Volume, *fakeQMP, []string) {
	t.Helper()
	volume, server := newTestVolume(t)
	paths := make([]string, 4)
	for i, id := range []string{"snap-a", "snap-b"} {
		parent := ""
		if i > 0 {
			parent = "snap-a"
		}
		path, err := volume.manager.sharedLayerPath(id, parent)
		require.NoError(t, err)
		paths[i] = path
	}
	for i, name := range []string{"c", "d"} {
		paths[i+2] = filepath.Join(volume.dir, layersSubdir, name+".qcow2")
	}
	require.NoError(t, os.MkdirAll(volume.manager.sharedLayersDir(), 0o700))
	for _, path := range paths {
		require.NoError(t, os.WriteFile(path, []byte("qcow2-stub"), 0o600))
	}
	volume.state.Chain = []stateLayer{
		{SnapshotID: "snap-a", Path: paths[0]},
		{SnapshotID: "snap-b", Path: paths[1]},
		{SnapshotID: "snap-c", Path: paths[2]},
		{SnapshotID: "snap-d", Path: paths[3]},
	}
	require.NoError(t, saveVolumeState(volume.dir, volume.state))
	server.setImages(map[string][]string{
		"fmt-0": {volume.state.HeadPath, paths[3], paths[2], paths[1], paths[0]},
	})
	return volume, server, paths
}

func TestCompactCommitsOnlyIntoTheVolumesOwnLayers(t *testing.T) {
	for _, first := range []int{0, 1} {
		volume, server, paths := newSharedChainVolume(t)
		merged, err := volume.Merge(context.Background(), first)
		require.NoError(t, err)
		require.Equal(t, PublishedLayer{SnapshotID: "snap-d", Path: paths[2]}, merged)
		require.Equal(t, []stateLayer{
			{SnapshotID: "snap-a", Path: paths[0]},
			{SnapshotID: "snap-b", Path: paths[1]},
			{SnapshotID: "snap-d", Path: paths[2]},
		}, volume.state.Chain)
		require.FileExists(t, paths[0])
		require.FileExists(t, paths[1])
		require.NoFileExists(t, paths[3])
		server.mu.Lock()
		require.Equal(t, []fakeCommit{{device: "fmt-0", top: paths[3], base: paths[2]}}, server.commits)
		server.mu.Unlock()

		// One own layer has nothing to merge into.
		require.NoError(t, volume.Compact(context.Background()))
		server.mu.Lock()
		require.Len(t, server.commits, 1)
		server.mu.Unlock()
	}
}

// A live graph missing a layer above a shared one would mean a commit wrote
// into the shared layer; the merge refuses to record that.
func TestMergeRefusesACommitIntoASharedLayer(t *testing.T) {
	volume, server, paths := newSharedChainVolume(t)
	server.setImages(map[string][]string{
		"fmt-0": {volume.state.HeadPath, paths[3], paths[2], paths[0]},
	})
	_, err := volume.Merge(context.Background(), 0)
	require.ErrorContains(t, err, "shared layer")
	require.Len(t, volume.state.Chain, 4)
	for _, path := range paths {
		require.FileExists(t, path)
	}
}

func TestRecoverPrunesUnreferencedSharedLayers(t *testing.T) {
	root := t.TempDir()
	sysBlock, dev := t.TempDir(), t.TempDir()
	writeTestNBDDevice(t, sysBlock, "nbd0", "43:0")
	manager := NewManager(Config{
		Root:         root,
		SysBlockPath: sysBlock,
		DevPath:      dev,
		Runner: func(ctx context.Context, name string, args ...string) ([]byte, error) {
			switch name {
			case "mknod":
				return nil, os.WriteFile(args[2], nil, 0o600)
			case "stat":
				return []byte("6180:2b:0\n"), nil
			default:
				return fakeRunner(ctx, name, args...)
			}
		},
	})

	referenced, err := manager.sharedLayerPath("snap-a", "")
	require.NoError(t, err)
	unreferenced, err := manager.sharedLayerPath("snap-old", "")
	require.NoError(t, err)
	interrupted := referenced + ".other" + partialLayerSuffix + ".tmp"
	require.NoError(t, os.MkdirAll(manager.sharedLayersDir(), 0o700))
	for _, path := range []string{referenced, unreferenced, interrupted} {
		require.NoError(t, os.WriteFile(path, []byte("qcow2-stub"), 0o600))
	}
	dir := manager.volumeDir("detached")
	require.NoError(t, os.MkdirAll(dir, 0o700))
	require.NoError(t, saveVolumeState(dir, &volumeState{Key: "detached", Chain: []stateLayer{{SnapshotID: "snap-a", Path: referenced}}}))

	require.NoError(t, manager.Recover(context.Background()))
	require.FileExists(t, referenced)
	require.NoFileExists(t, unreferenced)
	require.NoFileExists(t, interrupted)
}
