package abstractions

import (
	"context"
	"fmt"
	"testing"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

type durableDiskPlacementBackendRepo struct {
	repository.BackendRepository
	source       *types.DiskSnapshot
	latest       *types.DiskSnapshot
	sourceErr    error
	latestErr    error
	gotSourceID  string
	gotLatestFor string
}

func (r *durableDiskPlacementBackendRepo) GetDiskSnapshot(_ context.Context, _ uint, snapshotID string) (*types.DiskSnapshot, error) {
	r.gotSourceID = snapshotID
	return r.source, r.sourceErr
}

func (r *durableDiskPlacementBackendRepo) GetLatestDiskSnapshot(_ context.Context, _ uint, diskName string) (*types.DiskSnapshot, error) {
	r.gotLatestFor = diskName
	return r.latest, r.latestErr
}

func restorableDiskSnapshot(id, disk string) *types.DiskSnapshot {
	return &types.DiskSnapshot{
		ExternalId:  id,
		DiskName:    disk,
		Status:      types.DiskSnapshotStatusAvailable,
		Format:      types.DiskSnapshotFormatDirV1,
		ManifestKey: "snapshots/manifest.json",
	}
}

func TestDurableDiskFallbackHonorsExplicitSourceSnapshot(t *testing.T) {
	repo := &durableDiskPlacementBackendRepo{
		source:    restorableDiskSnapshot("source-snapshot", "source-disk"),
		latestErr: &types.ErrDiskSnapshotNotFound{SnapshotId: "new-destination-disk"},
	}
	disks := []*pb.DurableDisk{{Name: "new-destination-disk", SourceSnapshotId: "  source-snapshot  "}}

	snapshots, err := latestRestorableDurableDiskSnapshots(
		context.Background(),
		DurableDiskPlacementRepos{BackendRepo: repo},
		&types.Workspace{Id: 7},
		disks,
	)

	require.NoError(t, err)
	require.Equal(t, "new-destination-disk", repo.gotLatestFor)
	require.Equal(t, "source-snapshot", repo.gotSourceID)
	require.Equal(t, "source-snapshot", disks[0].SourceSnapshotId)
	require.Equal(t, "source-snapshot", snapshots[0].ExternalId)
}

func TestDurableDiskFallbackPrefersDestinationLatestGeneration(t *testing.T) {
	repo := &durableDiskPlacementBackendRepo{
		latest:    restorableDiskSnapshot("destination-latest", "fork-disk"),
		sourceErr: &types.ErrDiskSnapshotNotFound{SnapshotId: "pruned-seed"},
	}
	disks := []*pb.DurableDisk{{Name: "fork-disk", SourceSnapshotId: "pruned-seed"}}

	snapshots, err := latestRestorableDurableDiskSnapshots(
		context.Background(),
		DurableDiskPlacementRepos{BackendRepo: repo},
		&types.Workspace{Id: 7},
		disks,
	)

	require.NoError(t, err)
	require.Equal(t, "fork-disk", repo.gotLatestFor)
	require.Empty(t, repo.gotSourceID)
	require.Equal(t, "destination-latest", snapshots[0].ExternalId)
}

func TestDurableDiskFallbackDoesNotHideLatestGenerationLookupFailure(t *testing.T) {
	repo := &durableDiskPlacementBackendRepo{latestErr: fmt.Errorf("backend unavailable")}
	disks := []*pb.DurableDisk{{Name: "fork-disk", SourceSnapshotId: "source-snapshot"}}

	_, err := latestRestorableDurableDiskSnapshots(
		context.Background(),
		DurableDiskPlacementRepos{BackendRepo: repo},
		&types.Workspace{Id: 7},
		disks,
	)

	require.ErrorContains(t, err, "backend unavailable")
	require.Empty(t, repo.gotSourceID)
}

func TestDurableDiskFallbackUsesLatestGenerationWithoutASeed(t *testing.T) {
	repo := &durableDiskPlacementBackendRepo{latest: restorableDiskSnapshot("latest-snapshot", "existing-disk")}
	disks := []*pb.DurableDisk{{Name: "existing-disk"}}

	snapshots, err := latestRestorableDurableDiskSnapshots(
		context.Background(),
		DurableDiskPlacementRepos{BackendRepo: repo},
		&types.Workspace{Id: 7},
		disks,
	)

	require.NoError(t, err)
	require.Equal(t, "existing-disk", repo.gotLatestFor)
	require.Empty(t, repo.gotSourceID)
	require.Equal(t, "latest-snapshot", snapshots[0].ExternalId)
}
