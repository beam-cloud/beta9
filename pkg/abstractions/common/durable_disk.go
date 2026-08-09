package abstractions

import (
	"context"
	"fmt"
	"os"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type DurableDiskPlacementRepos struct {
	BackendRepo    repository.BackendRepository
	ComputeRepo    repository.ComputeRepository
	WorkerRepo     repository.WorkerRepository
	WorkerPoolRepo repository.WorkerPoolRepository
}

func ConfigureDurableDiskPlacement(ctx context.Context, repos DurableDiskPlacementRepos, workspace *types.Workspace, stubConfig *types.StubConfigV1) error {
	if stubConfig == nil || len(stubConfig.Disks) == 0 {
		return nil
	}
	if err := enforceDurableDiskSingleWriter(stubConfig); err != nil {
		return err
	}

	poolName := stubConfig.PoolSelector()
	fallback, err := durableDiskSnapshotFallbackNeeded(ctx, repos, workspace, poolName, stubConfig.Disks)
	if err != nil {
		return err
	}
	if fallback {
		return configureDurableDiskSnapshotFallback(ctx, repos, workspace, poolName, stubConfig)
	}

	if err := configureDurableDiskDrivers(stubConfig); err != nil {
		if fallbackErr := configureDurableDiskSnapshotFallback(ctx, repos, workspace, poolName, stubConfig); fallbackErr != nil {
			return fmt.Errorf("%w; snapshot fallback unavailable: %v", err, fallbackErr)
		}
	}

	return nil
}

func enforceDurableDiskSingleWriter(stubConfig *types.StubConfigV1) error {
	if durableDisksReadOnly(stubConfig.Disks) {
		return nil
	}

	maxContainers := uint(1)
	minContainers := uint(0)
	if stubConfig.Autoscaler != nil {
		minContainers = stubConfig.Autoscaler.MinContainers
		if stubConfig.Autoscaler.MaxContainers > 0 {
			maxContainers = stubConfig.Autoscaler.MaxContainers
		}
	}
	if minContainers > 1 || maxContainers > 1 {
		return fmt.Errorf("writable durable disks support one container; set max containers to 1 or mark the disk read_only")
	}
	return nil
}

func durableDisksReadOnly(disks []*pb.DurableDisk) bool {
	for _, disk := range disks {
		if disk != nil && !disk.ReadOnly {
			return false
		}
	}
	return true
}

func ConfigureUnavailablePrivatePoolFallback(ctx context.Context, repos DurableDiskPlacementRepos, workspace *types.Workspace, stubConfig *types.StubConfigV1) error {
	if stubConfig == nil {
		return nil
	}
	poolName := stubConfig.PoolSelector()
	if poolName == "" || privatePoolHasAvailableWorkers(ctx, repos, poolName) {
		return nil
	}
	if err := ensurePrivatePoolFallbackAllowed(ctx, repos, workspace, poolName); err != nil {
		return err
	}
	stubConfig.Pool = nil
	return nil
}

func configureDurableDiskDrivers(stubConfig *types.StubConfigV1) error {
	for _, disk := range stubConfig.Disks {
		if disk == nil {
			continue
		}

		driver := durableDiskGatewayDriver(disk.Driver)
		if driver == "" {
			driver = types.DurableDiskDriverSnapshot
		}
		disk.Driver = driver

		if driver != types.DurableDiskDriverSnapshot {
			return fmt.Errorf("durable disk %q requested unsupported driver %q", disk.Name, driver)
		}
	}

	return nil
}

func configureDurableDiskSnapshotFallback(ctx context.Context, repos DurableDiskPlacementRepos, workspace *types.Workspace, poolName string, stubConfig *types.StubConfigV1) error {
	if poolName == "" || !durableDiskSnapshotFallbackSupported(stubConfig.Disks) {
		return fmt.Errorf("durable disk snapshot fallback is not available for pool %q", poolName)
	}
	if _, err := latestRestorableDurableDiskSnapshots(ctx, repos, workspace, stubConfig.Disks); err != nil {
		return err
	}

	stubConfig.Pool = nil
	return configureDurableDiskDrivers(stubConfig)
}

func durableDiskSnapshotFallbackSupported(disks []*pb.DurableDisk) bool {
	for _, disk := range disks {
		if disk == nil {
			continue
		}
		driver := durableDiskGatewayDriver(disk.Driver)
		if driver == "" {
			driver = types.DurableDiskDriverSnapshot
		}
		if driver != types.DurableDiskDriverSnapshot {
			return false
		}
	}
	return true
}

func durableDiskSnapshotFallbackNeeded(ctx context.Context, repos DurableDiskPlacementRepos, workspace *types.Workspace, poolName string, disks []*pb.DurableDisk) (bool, error) {
	if poolName == "" || !durableDiskSnapshotFallbackSupported(disks) || privatePoolHasAvailableWorkers(ctx, repos, poolName) {
		return false, nil
	}
	_, err := latestRestorableDurableDiskSnapshots(ctx, repos, workspace, disks)
	return err == nil, err
}

func latestRestorableDurableDiskSnapshots(ctx context.Context, repos DurableDiskPlacementRepos, workspace *types.Workspace, disks []*pb.DurableDisk) ([]*types.DiskSnapshot, error) {
	if repos.BackendRepo == nil || workspace == nil {
		return nil, fmt.Errorf("durable disk snapshot fallback requires workspace metadata")
	}
	workspaceID, err := durableDiskSnapshotWorkspaceID(ctx, repos, workspace)
	if err != nil {
		return nil, err
	}
	snapshots := make([]*types.DiskSnapshot, 0, len(disks))
	for _, disk := range disks {
		if disk == nil || disk.Name == "" {
			continue
		}
		snapshot, err := repos.BackendRepo.GetLatestDiskSnapshot(ctx, workspaceID, disk.Name)
		if err != nil {
			return nil, fmt.Errorf("durable disk %q has no restorable snapshot: %w", disk.Name, err)
		}
		if snapshot == nil || snapshot.ManifestKey == "" || !types.IsDiskSnapshotFilesystemFormat(snapshot.Format) {
			return nil, fmt.Errorf("durable disk %q has no restorable filesystem snapshot", disk.Name)
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots, nil
}

func durableDiskSnapshotWorkspaceID(ctx context.Context, repos DurableDiskPlacementRepos, workspace *types.Workspace) (uint, error) {
	if workspace.Id != 0 {
		return workspace.Id, nil
	}
	if workspace.ExternalId == "" {
		return 0, fmt.Errorf("durable disk snapshot fallback requires workspace id")
	}
	resolved, err := repos.BackendRepo.GetWorkspaceByExternalId(ctx, workspace.ExternalId)
	if err != nil {
		return 0, err
	}
	if resolved.Id == 0 {
		return 0, fmt.Errorf("durable disk snapshot fallback requires workspace id")
	}
	return resolved.Id, nil
}

func ensurePrivatePoolFallbackAllowed(ctx context.Context, repos DurableDiskPlacementRepos, workspace *types.Workspace, poolName string) error {
	if repos.ComputeRepo == nil || workspace == nil {
		return nil
	}
	state, err := repos.ComputeRepo.GetPoolState(ctx, workspace.ExternalId, poolName)
	if err != nil {
		return err
	}
	if state != nil && state.Mode != "" && state.Mode != string(types.PoolModePrivate) {
		return fmt.Errorf("durable disk snapshot fallback is only enabled for private pools")
	}
	return nil
}

func privatePoolHasAvailableWorkers(ctx context.Context, repos DurableDiskPlacementRepos, poolName string) bool {
	if repos.WorkerRepo == nil || poolName == "" {
		return false
	}
	if repos.WorkerPoolRepo != nil {
		state, err := repos.WorkerPoolRepo.GetWorkerPoolState(ctx, poolName)
		if err == nil && state != nil {
			return state.ReadyMachines > 0
		}
	}
	workers, err := repos.WorkerRepo.GetAllWorkersInPool(poolName)
	if err != nil {
		return true
	}
	for _, worker := range workers {
		if worker != nil && worker.Status == types.WorkerStatusAvailable {
			return true
		}
	}
	return false
}

func durableDiskGatewayDriver(configured string) string {
	if driver := types.NormalizeDurableDiskDriver(configured); driver != "" {
		return driver
	}
	return types.NormalizeDurableDiskDriver(os.Getenv("BETA9_DURABLE_DISK_DRIVER"))
}
