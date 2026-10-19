package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/disk"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// qcowTestChain returns a published chain r0, r1, ... with the given stored
// sizes, each generation holding one chunk named after it.
func qcowTestChain(sizes ...int64) []qcowChainEntry {
	chain := make([]qcowChainEntry, len(sizes))
	for i, size := range sizes {
		id := fmt.Sprintf("r%d", i)
		chain[i] = qcowChainEntry{
			row: &types.DiskSnapshot{ExternalId: id, Generation: int64(i + 1), StoredSizeBytes: size},
			manifest: &types.DiskSnapshotManifest{
				Format:           types.DiskSnapshotFormatQcowV1,
				LogicalSizeBytes: 1 << 30,
				Files:            []types.DiskSnapshotFile{{Path: disk.LayerFileName, Chunks: []types.DiskSnapshotChunk{{Digest: "sha256:" + id}}}},
			},
		}
	}
	return chain
}

func TestQcowMergeBase(t *testing.T) {
	const mb = 1 << 20
	repeat := func(n int, size int64) []int64 {
		sizes := make([]int64, n)
		for i := range sizes {
			sizes[i] = size
		}
		return sizes
	}
	template := []int64{30 << 30, 20 << 30, 5 << 30, 1 << 30, 100 * mb, 10 * mb, mb}
	growing := []int64{mb, 2 * mb, 3 * mb, 4 * mb, 5 * mb, 6 * mb, 7 * mb, 8 * mb, 9 * mb, 10 * mb}

	for _, tc := range []struct {
		name  string
		sizes []int64
		base  int
		ok    bool
	}{
		{"shallow chain", repeat(qcowMergeDepth, mb), 0, false},
		{"equal layers merge the newest two", repeat(qcowMergeDepth+1, mb), qcowMergeDepth - 1, true},
		{"small layers merge into the nearest larger one", append(repeat(13, 100*mb), 1<<30, 10*mb, 10*mb, 20*mb), 13, true},
		{"a copy's layers stop at its template's first layer as large", append(template, growing...), 5, true},
		{"a newest layer larger than all waits for one above it", append(repeat(qcowMergeDepth, mb), 1<<30), 0, false},
	} {
		base, ok := qcowMergeBase(qcowTestChain(tc.sizes...))
		require.Equal(t, tc.ok, ok, tc.name)
		require.Equal(t, tc.base, base, tc.name)
	}
}

func TestQcowMergedLayer(t *testing.T) {
	chain := qcowTestChain(make([]int64, 5)...)
	layers := func(ids ...string) []disk.PublishedLayer {
		published := make([]disk.PublishedLayer, len(ids))
		for i, id := range ids {
			published[i] = disk.PublishedLayer{SnapshotID: id}
		}
		return published
	}

	for _, tc := range []struct {
		name   string
		layers []disk.PublishedLayer
		merged int
		ok     bool
	}{
		{"one layer per generation", layers("r0", "r1", "r2", "r3", "r4"), -1, true},
		{"merged into the base", layers("r4"), 0, true},
		{"merged below newer generations", layers("r0", "r2", "r3", "r4"), 1, true},
		{"local chain behind", layers("r0", "r1"), -1, false},
		{"local chain ahead", layers("r0", "r1", "r2", "r3", "r4", "r5"), -1, false},
		{"diverged above a merge", layers("r0", "r2", "r9"), -1, false},
	} {
		merged, ok := qcowMergedLayer(tc.layers, chain)
		require.Equal(t, tc.ok, ok, tc.name)
		require.Equal(t, tc.merged, merged, tc.name)
	}
}

type mergedQcowVolume struct {
	layers  []disk.PublishedLayer
	renamed map[string]string
}

func (v *mergedQcowVolume) Published() ([]disk.PublishedLayer, bool) { return v.layers, false }
func (v *mergedQcowVolume) Check() error                             { return nil }
func (v *mergedQcowVolume) Republished(ids map[string]string) error {
	v.renamed = ids
	return nil
}

type qcowMergeBackend struct {
	*fakeBackendRepoClient
	created []*pb.DiskSnapshot
}

func (b *qcowMergeBackend) CreateDiskSnapshot(_ context.Context, in *pb.CreateDiskSnapshotRequest, _ ...grpc.CallOption) (*pb.CreateDiskSnapshotResponse, error) {
	in.Snapshot.ExternalId = fmt.Sprintf("new-%d", len(b.created))
	b.created = append(b.created, in.Snapshot)
	return &pb.CreateDiskSnapshotResponse{Ok: true, Snapshot: in.Snapshot}, nil
}

// newManifestBucket returns a store backed by a stub bucket and a function
// listing the manifests uploaded to it, in order.
func newManifestBucket(t *testing.T) (*durableDiskSnapshotBucketStore, func() []types.DiskSnapshotManifest) {
	t.Helper()
	var mu sync.Mutex
	var manifests []types.DiskSnapshotManifest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/"+durableDiskManifestFileName) {
			var manifest types.DiskSnapshotManifest
			if err := json.NewDecoder(r.Body).Decode(&manifest); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			mu.Lock()
			manifests = append(manifests, manifest)
			mu.Unlock()
		}
	}))
	t.Cleanup(server.Close)
	region, key, secret := "us-east-1", "key", "secret"
	client, err := clients.NewWorkspaceStorageClient(context.Background(), "workspace", &types.WorkspaceStorage{
		EndpointUrl: &server.URL, Region: &region, AccessKey: &key, SecretKey: &secret,
	})
	require.NoError(t, err)
	return &durableDiskSnapshotBucketStore{client: client, bucket: "disks"}, func() []types.DiskSnapshotManifest {
		mu.Lock()
		defer mu.Unlock()
		return append([]types.DiskSnapshotManifest(nil), manifests...)
	}
}

func TestPublishQcowMergeRepublishesTheGenerationsAboveIt(t *testing.T) {
	// Layer 1 holds r1 to r3; r4 was published above it during the upload.
	merged := disk.PublishedLayer{SnapshotID: "r3", Path: "merged.qcow2"}
	layer := &types.DiskSnapshotFile{Path: disk.LayerFileName, Chunks: []types.DiskSnapshotChunk{{Digest: "sha256:merged"}}}
	request := &types.ContainerRequest{ContainerId: "container"}
	mount := &types.Mount{DurableDisk: &types.DurableDiskMountConfig{Name: "disk"}}
	publish := func(latest string, layers ...disk.PublishedLayer) (*Worker, *qcowMergeBackend, *mergedQcowVolume, []types.DiskSnapshotManifest, error) {
		store, manifests := newManifestBucket(t)
		backend := &qcowMergeBackend{fakeBackendRepoClient: &fakeBackendRepoClient{latestSnapshot: &pb.DiskSnapshot{ExternalId: latest, Generation: 5}}}
		worker := &Worker{backendRepoClient: backend}
		worker.qcowChains.Store("disk", qcowTestChain(make([]int64, 5)...))
		volume := &mergedQcowVolume{layers: layers}
		err := worker.publishQcowMerge(context.Background(), request, mount, volume, store, "disk", merged, layer)
		return worker, backend, volume, manifests(), err
	}

	worker, backend, volume, manifests, err := publish("r4", disk.PublishedLayer{SnapshotID: "r0"}, merged, disk.PublishedLayer{SnapshotID: "r4"})
	require.NoError(t, err)
	require.Len(t, backend.created, 2)
	require.Equal(t, "r0", backend.created[0].ParentSnapshotId)
	require.Equal(t, int64(4), backend.created[0].Generation, "the merged layer keeps its newest generation's")
	require.Equal(t, "new-0", backend.created[1].ParentSnapshotId)
	require.Greater(t, backend.created[1].Generation, int64(5), "the copy of the head must outrank it")
	require.Len(t, manifests, 2)
	require.Equal(t, "sha256:merged", manifests[0].Files[0].Chunks[0].Digest)
	require.Equal(t, "sha256:r4", manifests[1].Files[0].Chunks[0].Digest, "a copy reuses its generation's layer")
	require.Equal(t, map[string]string{"r3": "new-0", "r4": "new-1"}, volume.renamed)
	var ids []string
	for _, entry := range worker.qcowChain("disk") {
		ids = append(ids, entry.row.ExternalId)
	}
	require.Equal(t, []string{"r0", "new-0", "new-1"}, ids)

	_, backend, _, _, err = publish("elsewhere", disk.PublishedLayer{SnapshotID: "r0"}, merged, disk.PublishedLayer{SnapshotID: "r4"})
	require.Error(t, err, "a generation another worker published must not be overwritten")
	require.Empty(t, backend.created)

	_, backend, _, _, err = publish("r4", disk.PublishedLayer{SnapshotID: "r4", Path: "compacted.qcow2"})
	require.NoError(t, err, "a compaction that replaced the merged layer leaves nothing to publish")
	require.Empty(t, backend.created)
}
