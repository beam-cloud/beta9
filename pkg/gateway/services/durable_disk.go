package gatewayservices

import (
	"context"
	"fmt"
	"os"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

func (gws *GatewayService) configureDurableDiskPlacement(ctx context.Context, workspace *types.Workspace, stubConfig *types.StubConfigV1) error {
	if stubConfig == nil || len(stubConfig.Disks) == 0 {
		return nil
	}

	poolName := stubConfig.PoolSelector()
	fallback, err := gws.durableDiskSnapshotFallbackNeeded(ctx, workspace, poolName, stubConfig.Disks)
	if err != nil {
		return err
	}
	if fallback {
		return gws.configureDurableDiskSnapshotFallback(ctx, workspace, poolName, stubConfig)
	}

	if err := gws.configureDurableDiskDrivers(stubConfig); err != nil {
		if fallbackErr := gws.configureDurableDiskSnapshotFallback(ctx, workspace, poolName, stubConfig); fallbackErr != nil {
			return fmt.Errorf("%w; snapshot fallback unavailable: %v", err, fallbackErr)
		}
	}

	return nil
}

func (gws *GatewayService) configureDurableDiskDrivers(stubConfig *types.StubConfigV1) error {
	for _, disk := range stubConfig.Disks {
		if disk == nil {
			continue
		}

		driver := durableDiskGatewayDriver(disk.Driver)
		if driver == "" {
			driver = types.DurableDiskDriverDev
		}
		disk.Driver = driver

		if driver != types.DurableDiskDriverDev {
			return fmt.Errorf("durable disk %q requested unsupported driver %q", disk.Name, driver)
		}
	}

	return nil
}

func (gws *GatewayService) configureDurableDiskSnapshotFallback(ctx context.Context, workspace *types.Workspace, poolName string, stubConfig *types.StubConfigV1) error {
	if poolName == "" || !durableDiskSnapshotFallbackSupported(stubConfig.Disks) {
		return fmt.Errorf("durable disk snapshot fallback is not available for pool %q", poolName)
	}
	if err := gws.ensureDurableDiskSnapshotsAvailable(ctx, workspace, stubConfig.Disks); err != nil {
		return err
	}
	allowed, err := gws.privatePoolSnapshotFallbackAllowed(ctx, workspace, poolName)
	if err != nil {
		return err
	}
	if !allowed {
		return fmt.Errorf("durable disk snapshot fallback is only enabled for private pools")
	}

	stubConfig.Pool = nil
	return gws.configureDurableDiskDrivers(stubConfig)
}

func durableDiskSnapshotFallbackSupported(disks []*pb.DurableDisk) bool {
	for _, disk := range disks {
		if disk == nil {
			continue
		}
		driver := durableDiskGatewayDriver(disk.Driver)
		if driver == "" {
			driver = types.DurableDiskDriverDev
		}
		if driver != types.DurableDiskDriverDev {
			return false
		}
	}
	return true
}

func (gws *GatewayService) durableDiskSnapshotFallbackNeeded(ctx context.Context, workspace *types.Workspace, poolName string, disks []*pb.DurableDisk) (bool, error) {
	if poolName == "" || !durableDiskSnapshotFallbackSupported(disks) || gws.privatePoolHasAvailableWorkers(poolName) {
		return false, nil
	}
	allowed, err := gws.privatePoolSnapshotFallbackAllowed(ctx, workspace, poolName)
	return allowed, err
}

func (gws *GatewayService) privatePoolSnapshotFallbackAllowed(ctx context.Context, workspace *types.Workspace, poolName string) (bool, error) {
	if gws == nil || gws.computeRepo == nil || workspace == nil {
		return false, nil
	}
	state, err := gws.computeRepo.GetPoolState(ctx, workspace.ExternalId, poolName)
	if err != nil {
		return false, err
	}
	return state != nil && state.Mode == string(types.PoolModePrivate), nil
}

func (gws *GatewayService) ensureDurableDiskSnapshotsAvailable(ctx context.Context, workspace *types.Workspace, disks []*pb.DurableDisk) error {
	if gws == nil || gws.backendRepo == nil || workspace == nil {
		return fmt.Errorf("durable disk snapshot fallback requires workspace metadata")
	}
	workspaceID, err := gws.durableDiskSnapshotWorkspaceID(ctx, workspace)
	if err != nil {
		return err
	}
	for _, disk := range disks {
		if disk == nil || disk.Name == "" {
			continue
		}
		snapshot, err := gws.backendRepo.GetLatestDiskSnapshot(ctx, workspaceID, disk.Name)
		if err != nil {
			return fmt.Errorf("durable disk %q has no restorable snapshot: %w", disk.Name, err)
		}
		if snapshot == nil || snapshot.ManifestKey == "" || !types.IsDiskSnapshotFilesystemFormat(snapshot.Format) {
			return fmt.Errorf("durable disk %q has no restorable filesystem snapshot", disk.Name)
		}
	}
	return nil
}

func (gws *GatewayService) durableDiskSnapshotWorkspaceID(ctx context.Context, workspace *types.Workspace) (uint, error) {
	if workspace.Id != 0 {
		return workspace.Id, nil
	}
	if workspace.ExternalId == "" {
		return 0, fmt.Errorf("durable disk snapshot fallback requires workspace id")
	}
	resolved, err := gws.backendRepo.GetWorkspaceByExternalId(ctx, workspace.ExternalId)
	if err != nil {
		return 0, err
	}
	if resolved.Id == 0 {
		return 0, fmt.Errorf("durable disk snapshot fallback requires workspace id")
	}
	return resolved.Id, nil
}

func (gws *GatewayService) ensurePrivatePoolFallbackAllowed(ctx context.Context, workspace *types.Workspace, poolName string) error {
	if gws == nil || gws.computeRepo == nil || workspace == nil {
		return nil
	}
	state, err := gws.computeRepo.GetPoolState(ctx, workspace.ExternalId, poolName)
	if err != nil {
		return err
	}
	if state != nil && state.Mode != "" && state.Mode != string(types.PoolModePrivate) {
		return fmt.Errorf("durable disk snapshot fallback is only enabled for private pools")
	}
	return nil
}

func (gws *GatewayService) configureUnavailablePrivatePoolFallback(ctx context.Context, workspace *types.Workspace, stubConfig *types.StubConfigV1) error {
	poolName := stubConfig.PoolSelector()
	if poolName == "" || gws.privatePoolHasAvailableWorkers(poolName) {
		return nil
	}
	if err := gws.ensurePrivatePoolFallbackAllowed(ctx, workspace, poolName); err != nil {
		return err
	}
	stubConfig.Pool = nil
	return nil
}

func (gws *GatewayService) privatePoolHasAvailableWorkers(poolName string) bool {
	if gws == nil || gws.workerRepo == nil || poolName == "" {
		return false
	}
	workers, err := gws.workerRepo.GetAllWorkersInPool(poolName)
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
