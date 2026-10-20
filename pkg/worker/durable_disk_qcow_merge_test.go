package worker

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/disk"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// qcowTestChain returns the published chain r0, r1, ... with the given stored
// sizes, each generation's layer holding one chunk named after it.
func qcowTestChain(sizes ...int64) []qcowChainEntry {
	chain := make([]qcowChainEntry, len(sizes))
	for i, size := range sizes {
		id := fmt.Sprintf("r%d", i)
		chain[i] = qcowChainEntry{
			row: &types.DiskSnapshot{ExternalId: id, Generation: int64(i + 1), StoredSizeBytes: size},
			manifest: &types.DiskSnapshotManifest{
				Format: types.DiskSnapshotFormatQcowV1,
				Files:  []types.DiskSnapshotFile{{Path: disk.LayerFileName, Chunks: []types.DiskSnapshotChunk{{Digest: "sha256:" + id}}}},
			},
		}
	}
	return chain
}

// qcowTestLayers returns local layers named after the newest generation each
// holds.
func qcowTestLayers(ids ...string) []disk.PublishedLayer {
	layers := make([]disk.PublishedLayer, len(ids))
	for i, id := range ids {
		layers[i] = disk.PublishedLayer{SnapshotID: id, Path: id + ".qcow2"}
	}
	return layers
}

func TestQcowMergeBase(t *testing.T) {
	equal := func(n int) []int64 { return slices.Repeat([]int64{1}, n) }
	for _, tc := range []struct {
		name  string
		sizes []int64
		base  int // -1 when nothing merges
	}{
		{"shallow chain", equal(qcowMergeDepth), -1},
		{"equal layers merge the newest two", equal(qcowMergeDepth + 1), qcowMergeDepth - 1},
		{"a newest layer larger than all waits", append(equal(qcowMergeDepth), 2), -1},
		{"merges into the nearest layer as large as those above", slices.Concat([]int64{1000, 100}, equal(qcowMergeDepth-2), []int64{50}), 1},
	} {
		base, ok := qcowMergeBase(qcowTestChain(tc.sizes...))
		if !ok {
			base = -1
		}
		require.Equal(t, tc.base, base, tc.name)
	}
}

func TestQcowMergedLayer(t *testing.T) {
	chain := qcowTestChain(1, 1, 1, 1, 1)
	for _, tc := range []struct {
		name   string
		local  []string
		merged int
		ok     bool
	}{
		{"one layer per generation", []string{"r0", "r1", "r2", "r3", "r4"}, -1, true},
		{"merged into the base", []string{"r4"}, 0, true},
		{"merged below newer generations", []string{"r0", "r2", "r3", "r4"}, 1, true},
		{"local chain behind", []string{"r0", "r1"}, -1, false},
		{"local chain ahead", []string{"r0", "r1", "r2", "r3", "r4", "r5"}, -1, false},
		{"diverged above a merge", []string{"r0", "r2", "r9"}, -1, false},
	} {
		merged, ok := qcowMergedLayer(qcowTestLayers(tc.local...), chain)
		require.Equal(t, tc.ok, ok, tc.name)
		require.Equal(t, tc.merged, merged, tc.name)
	}
}

// qcowMergeFake is both the merged volume and the repository its merge
// publishes to.
type qcowMergeFake struct {
	*fakeBackendRepoClient
	layers  []disk.PublishedLayer
	created []*pb.DiskSnapshot
	failed  []string
	renamed map[string]string
	failAt  int // the CreateDiskSnapshot call that fails, from 1
}

func (f *qcowMergeFake) Published() ([]disk.PublishedLayer, bool) { return f.layers, false }
func (f *qcowMergeFake) Check() error                             { return nil }
func (f *qcowMergeFake) Republished(ids map[string]string) error {
	f.renamed = ids
	return nil
}
func (f *qcowMergeFake) CreateDiskSnapshot(_ context.Context, in *pb.CreateDiskSnapshotRequest, _ ...grpc.CallOption) (*pb.CreateDiskSnapshotResponse, error) {
	if len(f.created)+1 == f.failAt {
		return &pb.CreateDiskSnapshotResponse{ErrorMsg: "repository unavailable"}, nil
	}
	in.Snapshot.ExternalId = fmt.Sprintf("new-%d", len(f.created))
	f.created = append(f.created, in.Snapshot)
	return &pb.CreateDiskSnapshotResponse{Ok: true, Snapshot: in.Snapshot}, nil
}
func (f *qcowMergeFake) FailDiskSnapshot(_ context.Context, in *pb.FailDiskSnapshotRequest, _ ...grpc.CallOption) (*pb.FailDiskSnapshotResponse, error) {
	f.failed = append(f.failed, in.SnapshotId)
	return &pb.FailDiskSnapshotResponse{Ok: true}, nil
}

// The published chain is r0 to r4; local layer 1 holds r1 to r3, merged.
func TestPublishQcowMerge(t *testing.T) {
	bucket := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer bucket.Close()
	region, key, bucketName := "us-east-1", "key", "disks"
	client, err := clients.NewWorkspaceStorageClient(context.Background(), "workspace", &types.WorkspaceStorage{EndpointUrl: &bucket.URL, Region: &region, AccessKey: &key, SecretKey: &key, BucketName: &bucketName})
	require.NoError(t, err)
	store := &durableDiskSnapshotBucketStore{client: client, bucket: "disks"}
	mount := &types.Mount{DurableDisk: &types.DurableDiskMountConfig{Name: "disk"}}
	layer := &types.DiskSnapshotFile{Path: disk.LayerFileName, Chunks: []types.DiskSnapshotChunk{{Digest: "sha256:merged"}}}
	var failAt int
	publish := func(latest string, local ...string) (*Worker, *qcowMergeFake, error) {
		fake := &qcowMergeFake{fakeBackendRepoClient: &fakeBackendRepoClient{latestSnapshot: &pb.DiskSnapshot{ExternalId: latest, Generation: 5}}, layers: qcowTestLayers(local...), failAt: failAt}
		worker := &Worker{backendRepoClient: fake}
		worker.qcowChains.Store("disk", qcowTestChain(1, 1, 1, 1, 1))
		err := worker.publishQcowMerge(context.Background(), &types.ContainerRequest{}, mount, fake, store, "disk", qcowTestLayers("r3")[0], layer)
		return worker, fake, err
	}

	worker, fake, err := publish("r4", "r0", "r3", "r4")
	require.NoError(t, err)
	type entry struct{ id, parent, chunk string }
	var chain []entry
	for _, e := range worker.qcowChain("disk") {
		chain = append(chain, entry{e.row.ExternalId, e.row.ParentSnapshotId, e.manifest.Files[0].Chunks[0].Digest})
	}
	require.Equal(t, []entry{{"r0", "", "sha256:r0"}, {"new-0", "r0", "sha256:merged"}, {"new-1", "new-0", "sha256:r4"}}, chain)
	require.Equal(t, int64(4), fake.created[0].Generation, "the merged layer keeps r3's generation")
	require.Greater(t, fake.created[1].Generation, int64(5), "the copy of r4 outranks it")
	require.Equal(t, map[string]string{"r3": "new-0", "r4": "new-1"}, fake.renamed)
	require.Empty(t, fake.failed)

	_, fake, err = publish("elsewhere", "r0", "r3", "r4")
	require.ErrorContains(t, err, "did not publish")
	require.Empty(t, fake.created)

	_, fake, err = publish("r4", "r4") // compacted after the merge
	require.NoError(t, err)
	require.Empty(t, fake.created)

	failAt = 2 // the copy of r4
	worker, fake, err = publish("r4", "r0", "r3", "r4")
	require.ErrorContains(t, err, "repository unavailable")
	require.Equal(t, []string{"new-0"}, fake.failed, "the merged layer's row is withdrawn")
	require.Nil(t, fake.renamed)
	require.Len(t, worker.qcowChain("disk"), 5)
}

func TestStoppedQcowMergeKeepsAnotherFromStarting(t *testing.T) {
	var worker Worker
	var stopped error
	running := &qcowMerge{stop: func(cause error) { stopped = cause }}
	worker.qcowMerges.Store("disk", running)
	resume := worker.stopQcowMerge("disk")
	require.ErrorIs(t, stopped, errQcowMergeStopped)

	worker.qcowMerges.CompareAndDelete("disk", running) // the stopped merge exits
	worker.scheduleQcowMerge(&types.ContainerRequest{}, &types.Mount{}, "disk")
	resume()
	_, scheduled := worker.qcowMerges.Load("disk")
	require.False(t, scheduled, "no merge starts while the disk detaches")
}
