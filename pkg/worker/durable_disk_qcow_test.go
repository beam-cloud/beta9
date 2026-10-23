package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/disk"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestQcowChunkReadRestartsInterruptedBodies(t *testing.T) {
	content := []byte("complete chunk")
	for name, fault := range map[string]error{
		"empty body":       io.EOF,
		"short body":       io.ErrUnexpectedEOF,
		"connection reset": &net.OpError{Op: "read", Err: syscall.ECONNRESET},
		"read timeout":     &net.OpError{Op: "read", Err: os.ErrDeadlineExceeded},
	} {
		t.Run(name, func(t *testing.T) {
			store := &faultingQcowChunkStore{open: func(_ context.Context, attempt int) (io.ReadCloser, error) {
				if attempt == 1 {
					partial := []byte("wrong prefix")
					if fault == io.EOF {
						partial = nil
					}
					return io.NopCloser(&qcowFaultReader{data: partial, err: fault}), nil
				}
				return io.NopCloser(bytes.NewReader(content)), nil
			}}
			dest := make([]byte, len(content))
			err := (&qcowChunkSource{stores: []durableDiskSnapshotStore{store}}).ReadChunk(context.Background(), types.DiskSnapshotChunk{ObjectKey: "chunk"}, dest)
			require.NoError(t, err)
			require.Equal(t, content, dest, "the second body must overwrite, not append to, the first")
			require.Equal(t, 2, store.attempts)
			require.Equal(t, 2, store.closed)
		})
	}
}

func TestQcowChunkReadBoundsRetriesAndPreservesStoreFallback(t *testing.T) {
	short := &faultingQcowChunkStore{open: func(context.Context, int) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader([]byte("x"))), nil
	}}
	source := &qcowChunkSource{stores: []durableDiskSnapshotStore{short}}
	dest := make([]byte, 4)
	err := source.ReadChunk(context.Background(), types.DiskSnapshotChunk{ObjectKey: "chunk"}, dest)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.ErrorContains(t, err, "read 1/4 chunk bytes")
	require.ErrorContains(t, err, "attempt 3")
	require.Equal(t, 3, short.attempts)
	require.Equal(t, 3, short.closed)

	complete := &fakeDurableDiskSnapshotStore{objects: map[string][]byte{"chunk": []byte("good")}}
	source.stores = append(source.stores, complete)
	require.NoError(t, source.ReadChunk(context.Background(), types.DiskSnapshotChunk{ObjectKey: "chunk"}, dest))
	require.Equal(t, []byte("good"), dest)
	require.Equal(t, 6, short.attempts)
	require.Equal(t, 6, short.closed)
}

func TestQcowChunkReadDoesNotRetryPermanentOrCanceledFailures(t *testing.T) {
	for _, code := range []string{"AccessDenied", "NoSuchKey"} {
		t.Run(code, func(t *testing.T) {
			failure := &smithy.GenericAPIError{Code: code}
			store := &faultingQcowChunkStore{open: func(context.Context, int) (io.ReadCloser, error) { return nil, failure }}
			err := (&qcowChunkSource{stores: []durableDiskSnapshotStore{store}}).ReadChunk(context.Background(), types.DiskSnapshotChunk{}, make([]byte, 4))
			require.ErrorIs(t, err, failure)
			require.Equal(t, 1, store.attempts)
			require.Zero(t, store.closed)
		})
	}
	t.Run("permanent body failure", func(t *testing.T) {
		failure := errors.New("permanent body failure")
		store := &faultingQcowChunkStore{open: func(context.Context, int) (io.ReadCloser, error) {
			return io.NopCloser(&qcowFaultReader{err: failure}), nil
		}}
		err := (&qcowChunkSource{stores: []durableDiskSnapshotStore{store}}).ReadChunk(context.Background(), types.DiskSnapshotChunk{}, make([]byte, 4))
		require.ErrorIs(t, err, failure)
		require.Equal(t, 1, store.attempts)
		require.Equal(t, 1, store.closed)
	})
	t.Run("already canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		store := &faultingQcowChunkStore{}
		err := (&qcowChunkSource{stores: []durableDiskSnapshotStore{store}}).ReadChunk(ctx, types.DiskSnapshotChunk{}, make([]byte, 4))
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, store.attempts)
	})
	for _, phase := range []string{"open", "body"} {
		t.Run("canceled "+phase, func(t *testing.T) {
			store := &faultingQcowChunkStore{open: func(context.Context, int) (io.ReadCloser, error) {
				if phase == "open" {
					return nil, context.Canceled
				}
				return io.NopCloser(&qcowFaultReader{err: context.Canceled}), nil
			}}
			fallback := &fakeDurableDiskSnapshotStore{objects: map[string][]byte{"chunk": []byte("good")}}
			err := (&qcowChunkSource{stores: []durableDiskSnapshotStore{store, fallback}}).ReadChunk(context.Background(), types.DiskSnapshotChunk{ObjectKey: "chunk"}, make([]byte, 4))
			require.ErrorIs(t, err, context.Canceled, "cancellation must not fall through to another origin")
			require.Equal(t, 1, store.attempts)
			if phase == "body" {
				require.Equal(t, 1, store.closed)
			}
		})
	}
	t.Run("canceled during retry backoff", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store := &faultingQcowChunkStore{
			open:       func(context.Context, int) (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(nil)), nil },
			afterClose: func() { time.AfterFunc(10*time.Millisecond, cancel) },
		}
		err := (&qcowChunkSource{stores: []durableDiskSnapshotStore{store}}).ReadChunk(ctx, types.DiskSnapshotChunk{}, make([]byte, 4))
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, store.attempts)
		require.Equal(t, 1, store.closed)
	})
}

type faultingQcowChunkStore struct {
	fakeDurableDiskSnapshotStore
	open       func(context.Context, int) (io.ReadCloser, error)
	afterClose func()
	attempts   int
	closed     int
}

func (s *faultingQcowChunkStore) DownloadWithReader(ctx context.Context, _ string) (io.ReadCloser, error) {
	s.attempts++
	reader, err := s.open(ctx, s.attempts)
	if err != nil {
		return nil, err
	}
	return &qcowTrackedReader{ReadCloser: reader, close: func() {
		s.closed++
		if s.afterClose != nil {
			s.afterClose()
		}
	}}, nil
}

type qcowTrackedReader struct {
	io.ReadCloser
	close func()
}

func (r *qcowTrackedReader) Close() error {
	err := r.ReadCloser.Close()
	r.close()
	return err
}

type qcowFaultReader struct {
	data []byte
	err  error
}

func (r *qcowFaultReader) Read(dest []byte) (int, error) {
	n := copy(dest, r.data)
	r.data = r.data[n:]
	if len(r.data) == 0 {
		return n, r.err
	}
	return n, nil
}

func TestConcurrentQcowRestoresGoStraightToOriginOnLocalCacheMiss(t *testing.T) {
	content := []byte("durable qcow layer")
	store := &fakeDurableDiskSnapshotStore{objects: map[string][]byte{"layer": content}}
	// No network cache configuration: entering distributed cache discovery on
	// a local miss would fail instead of restoring from the durable origin.
	source := &qcowChunkSource{cacheReader: localSnapshotCacheReader{&cache.Client{}}, stores: []durableDiskSnapshotStore{store}}
	for range 100 {
		t.Run("restore", func(t *testing.T) {
			t.Parallel()
			dest := make([]byte, len(content))
			err := source.ReadChunk(context.Background(), types.DiskSnapshotChunk{ObjectKey: "layer", Digest: "sha256:missing"}, dest)
			require.NoError(t, err)
			require.Equal(t, content, dest)
		})
	}
}

type backloggedQcowVolume struct {
	pending    []disk.SealedLayer
	pendingErr error
	depth      int
	writes     int
	merges     bool
	events     []string
}

func (v *backloggedQcowVolume) Unpublished(context.Context) ([]disk.SealedLayer, error) {
	return v.pending, v.pendingErr
}
func (v *backloggedQcowVolume) Depth() int      { return v.depth }
func (v *backloggedQcowVolume) Journaled() bool { return !v.merges }
func (v *backloggedQcowVolume) Compact(context.Context) error {
	v.events = append(v.events, "compact")
	if len(v.pending) == 0 {
		v.depth = 2
	}
	return nil
}
func (v *backloggedQcowVolume) Seal(_ context.Context, force bool) ([]disk.SealedLayer, bool, error) {
	v.events = append(v.events, "seal")
	if v.depth >= disk.DefaultMaxChainDepth {
		return nil, false, disk.ErrMaxChainDepth
	}
	if !force && v.writes == 0 {
		return nil, true, nil
	}
	v.pending = append(v.pending, disk.SealedLayer{Path: fmt.Sprintf("head-%d", v.writes)})
	v.depth++
	v.writes = 0
	return v.pending, false, nil
}
func (v *backloggedQcowVolume) publish(layers []disk.SealedLayer) error {
	for _, layer := range layers {
		v.events = append(v.events, "publish "+layer.Path)
	}
	v.pending = nil
	return nil
}

func TestQcowSnapshotDrainsFullBacklogBeforeCapturingCurrentWrites(t *testing.T) {
	for _, mode := range []durableDiskSyncMode{durableDiskSyncExplicit, durableDiskSyncFinal, durableDiskSyncFailed} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			volume := &backloggedQcowVolume{pending: []disk.SealedLayer{{Path: "older"}, {Path: "newer"}}, depth: disk.DefaultMaxChainDepth, writes: 1}
			publish := func(layers []disk.SealedLayer) error {
				// Writes continue while the old sealed layers upload.
				if layers[0].Path == "older" {
					volume.writes++
				}
				return volume.publish(layers)
			}
			require.NoError(t, snapshotQcowVolume(context.Background(), volume, mode, false, publish))
			require.Equal(t, []string{"publish older", "publish newer", "compact", "seal", "publish head-2"}, volume.events)
			require.Empty(t, volume.pending)
		})
	}
}

func TestQcowSnapshotRetriesPublicationWithoutAnotherPivot(t *testing.T) {
	volume := &backloggedQcowVolume{pending: []disk.SealedLayer{{Path: "sealed"}}, depth: disk.DefaultMaxChainDepth, writes: 1}
	unavailable := errors.New("object storage unavailable")
	require.ErrorIs(t, snapshotQcowVolume(context.Background(), volume, durableDiskSyncExplicit, false, func([]disk.SealedLayer) error { return unavailable }), unavailable)
	require.Empty(t, volume.events, "failed publication must leave both the backlog and current head intact")
	require.Len(t, volume.pending, 1)
	require.Equal(t, 1, volume.writes)
	require.NoError(t, snapshotQcowVolume(context.Background(), volume, durableDiskSyncExplicit, false, volume.publish))
	require.Equal(t, []string{"publish sealed", "compact", "seal", "publish head-1"}, volume.events)
}

func TestQcowJournalCheckpointPublishesBacklogWithoutAnotherSeal(t *testing.T) {
	volume := &backloggedQcowVolume{pending: []disk.SealedLayer{{Path: "sealed"}}, depth: disk.DefaultMaxChainDepth, writes: 1}
	require.NoError(t, snapshotQcowVolume(context.Background(), volume, durableDiskSyncCheckpoint, false, volume.publish))
	require.Equal(t, []string{"publish sealed", "compact"}, volume.events)
	require.Equal(t, 1, volume.writes, "the journal retains writes beyond its published checkpoint")
}

func TestQcowSnapshotWithoutBacklogKeepsNormalSealBehavior(t *testing.T) {
	for _, tc := range []struct {
		writes int
		force  bool
		events []string
	}{
		{writes: 1, events: []string{"seal", "publish head-1"}},
		{events: []string{"seal"}},
		{force: true, events: []string{"seal", "publish head-0"}},
	} {
		volume := &backloggedQcowVolume{depth: 2, writes: tc.writes}
		require.NoError(t, snapshotQcowVolume(context.Background(), volume, durableDiskSyncExplicit, tc.force, volume.publish))
		require.Equal(t, tc.events, volume.events)
	}
}

func TestQcowSnapshotCompactsPublishedChainBeforeRetryingSeal(t *testing.T) {
	volume := &backloggedQcowVolume{depth: disk.DefaultMaxChainDepth, writes: 1}
	require.NoError(t, snapshotQcowVolume(context.Background(), volume, durableDiskSyncExplicit, false, volume.publish))
	require.Equal(t, []string{"seal", "compact", "seal", "publish head-1"}, volume.events)
}

// Merges commit a disk's local layers, so a disk that merges keeps them until
// a forced flatten has published its whole chain.
func TestQcowSnapshotCompactsAMergingDiskOnlyAfterAForcedFlatten(t *testing.T) {
	for _, tc := range []struct {
		depth  int
		events []string
	}{
		{depth: disk.DefaultFlattenDepth + 1, events: []string{"seal", "publish head-1"}},
		{depth: disk.DefaultMaxChainDepth / 2, events: []string{"seal", "publish head-1", "compact"}},
	} {
		volume := &backloggedQcowVolume{depth: tc.depth, writes: 1, merges: true}
		require.NoError(t, snapshotQcowVolume(context.Background(), volume, durableDiskSyncExplicit, false, volume.publish))
		require.Equal(t, tc.events, volume.events)
	}
}

func TestQcowSnapshotDoesNotForceAnEmptyHeadAfterFirstPublication(t *testing.T) {
	volume := &backloggedQcowVolume{pending: []disk.SealedLayer{{Path: "first"}}, depth: 2}
	require.NoError(t, snapshotQcowVolume(context.Background(), volume, durableDiskSyncExplicit, true, volume.publish))
	require.Equal(t, []string{"publish first", "seal"}, volume.events)
}

func TestSnapshotQcowCannotPublishAnUnresolvedPivot(t *testing.T) {
	uncertain := errors.New("live graph unavailable")
	volume := &backloggedQcowVolume{pending: []disk.SealedLayer{{Path: "possibly-writable"}}, pendingErr: uncertain}
	published := false
	err := snapshotQcowVolume(context.Background(), volume, durableDiskSyncExplicit, true, func([]disk.SealedLayer) error {
		published = true
		return nil
	})
	require.ErrorIs(t, err, uncertain)
	require.False(t, published)
	require.Empty(t, volume.events, "neither sealing nor compaction can proceed with an unknown head")
}
