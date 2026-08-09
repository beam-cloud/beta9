package abstractions

import (
	"context"
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
	gotSourceID  string
	gotLatestFor string
}

func (r *durableDiskPlacementBackendRepo) GetDiskSnapshot(_ context.Context, _ uint, snapshotID string) (*types.DiskSnapshot, error) {
	r.gotSourceID = snapshotID
	return r.source, nil
}

func (r *durableDiskPlacementBackendRepo) GetLatestDiskSnapshot(_ context.Context, _ uint, diskName string) (*types.DiskSnapshot, error) {
	r.gotLatestFor = diskName
	return r.latest, nil
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
	repo := &durableDiskPlacementBackendRepo{source: restorableDiskSnapshot("source-snapshot", "source-disk")}
	disks := []*pb.DurableDisk{{Name: "new-destination-disk", SourceSnapshotId: "source-snapshot"}}

	snapshots, err := latestRestorableDurableDiskSnapshots(
		context.Background(),
		DurableDiskPlacementRepos{BackendRepo: repo},
		&types.Workspace{Id: 7},
		disks,
	)

	require.NoError(t, err)
	require.Equal(t, "source-snapshot", repo.gotSourceID)
	require.Empty(t, repo.gotLatestFor, "a brand-new destination has no latest generation to restore")
	require.Equal(t, "source-snapshot", snapshots[0].ExternalId)
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
