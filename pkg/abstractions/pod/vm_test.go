package pod

import (
	"context"
	"errors"
	"testing"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

type vmDiskPlacementRepo struct {
	repository.BackendRepository
	latest            *types.DiskSnapshot
	err               error
	sourceErr         error
	requestedSnapshot string
}

func (r *vmDiskPlacementRepo) GetLatestDiskSnapshot(context.Context, uint, string) (*types.DiskSnapshot, error) {
	return r.latest, r.err
}

func (r *vmDiskPlacementRepo) GetDiskSnapshot(_ context.Context, _ uint, id string) (*types.DiskSnapshot, error) {
	r.requestedSnapshot = id
	return &types.DiskSnapshot{SourceWorkerId: "checkpoint-worker"}, r.sourceErr
}

func TestVMDiskPlacementUsesTheGenerationBeingRestored(t *testing.T) {
	for _, tc := range []struct {
		name         string
		warm         bool
		latestErr    error
		wantWorker   string
		wantSnapshot string
	}{
		{name: "cold uses latest", wantWorker: "latest-worker"},
		{name: "RAM uses paired generation", warm: true, wantWorker: "checkpoint-worker", wantSnapshot: "paired"},
		{name: "fork uses seed", latestErr: &types.ErrDiskSnapshotNotFound{SnapshotId: "latest"}, wantWorker: "checkpoint-worker", wantSnapshot: "paired"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &vmDiskPlacementRepo{latest: &types.DiskSnapshot{SourceWorkerId: "latest-worker"}, err: tc.latestErr}
			s := &GenericPodService{backendRepo: repo}
			worker, err := s.vmDiskWorker(context.Background(), 1, []*pb.DurableDisk{{Name: "root", MountPath: types.DurableDiskRootMountPath, SourceSnapshotId: "paired"}}, tc.warm)
			require.NoError(t, err)
			require.Equal(t, tc.wantWorker, worker)
			require.Equal(t, tc.wantSnapshot, repo.requestedSnapshot)
		})
	}
	repo := &vmDiskPlacementRepo{err: &types.ErrDiskSnapshotNotFound{SnapshotId: "latest"}}
	s := &GenericPodService{backendRepo: repo}
	disks := []*pb.DurableDisk{{Name: "fresh", MountPath: types.DurableDiskRootMountPath}}
	worker, err := s.vmDiskWorker(context.Background(), 1, disks, false)
	require.NoError(t, err)
	require.Empty(t, worker)
	repo.sourceErr = &types.ErrDiskSnapshotNotFound{SnapshotId: "paired"}
	disks[0].SourceSnapshotId = "paired"
	_, err = s.vmDiskWorker(context.Background(), 1, disks, false)
	require.ErrorIs(t, err, repo.sourceErr, "a missing fork source must not become a fresh empty disk")
	_, err = s.vmDiskWorker(context.Background(), 1, disks, true)
	require.ErrorIs(t, err, repo.sourceErr, "a missing RAM-paired disk must fail")
	repo.err = errors.New("database unavailable")
	_, err = s.vmDiskWorker(context.Background(), 1, disks, false)
	require.ErrorIs(t, err, repo.err)
}
