package gatewayservices

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"slices"
	"strings"

	"github.com/beam-cloud/beta9/pkg/common"
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

	if err := gws.configureDurableDiskPlacementForPool(poolName, stubConfig); err != nil {
		if fallbackErr := gws.configureDurableDiskSnapshotFallback(ctx, workspace, poolName, stubConfig); fallbackErr != nil {
			return fmt.Errorf("%w; snapshot fallback unavailable: %v", err, fallbackErr)
		}
	}

	return nil
}

func (gws *GatewayService) configureDurableDiskPlacementForPool(poolName string, stubConfig *types.StubConfigV1) error {
	for _, disk := range stubConfig.Disks {
		if disk == nil {
			continue
		}

		driver := durableDiskGatewayDriver(disk.Driver)
		if driver == "" {
			driver = types.DurableDiskDriverDev
		}
		disk.Driver = driver

		switch driver {
		case types.DurableDiskDriverDev:
			if err := gws.configureSingleNodeDurableDiskPlacement(poolName, disk); err != nil {
				return err
			}
		case types.DurableDiskDriverDRBD:
			if err := gws.configureDRBDDurableDiskPlacement(poolName, disk); err != nil {
				return err
			}
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
	for _, disk := range stubConfig.Disks {
		if disk == nil {
			continue
		}
		disk.Replication = &pb.DiskReplication{}
	}
	return gws.configureDurableDiskPlacementForPool("", stubConfig)
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
		if snapshot == nil || snapshot.ManifestKey == "" || snapshot.Format != types.DiskSnapshotFormatTarV1 {
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

func (gws *GatewayService) configureSingleNodeDurableDiskPlacement(poolName string, disk *pb.DurableDisk) error {
	if disk.Replication == nil {
		disk.Replication = &pb.DiskReplication{}
	}

	nodes, err := gws.durableDiskCandidateStorageNodes(poolName)
	if err != nil {
		return err
	}

	primaryID := strings.TrimSpace(disk.Replication.PrimaryWorkerId)
	if primaryID != "" {
		canonicalID, ok := durableDiskCanonicalStorageNodeID(primaryID, nodes)
		if ok {
			primaryID = canonicalID
		} else {
			primaryID = ""
		}
	}
	if primaryID == "" {
		if len(nodes) == 0 {
			disk.Replication.PrimaryWorkerId = ""
			disk.Replication.ReplicaWorkerIds = nil
			return nil
		}
		primaryID = durableDiskPreferredStorageNodeID(disk.Name, nodes)
	}

	disk.Replication.PrimaryWorkerId = primaryID
	disk.Replication.ReplicaWorkerIds = []string{primaryID}
	return nil
}

func (gws *GatewayService) configureDRBDDurableDiskPlacement(poolName string, disk *pb.DurableDisk) error {
	if disk.Replication == nil {
		disk.Replication = &pb.DiskReplication{}
	}

	replication := disk.Replication
	if replication.Replicas == 0 {
		replication.Replicas = 3
	}
	if replication.Mode == "" {
		replication.Mode = types.DurableDiskReplicationModeSync
	}
	if replication.Quorum == "" {
		replication.Quorum = types.DurableDiskReplicationQuorumMajority
	}

	poolNodes, err := gws.durableDiskCandidateStorageNodes(poolName)
	if err != nil {
		return err
	}
	allNodes := poolNodes
	if poolName != "" {
		allNodes, err = gws.durableDiskCandidateStorageNodes("")
		if err != nil {
			return err
		}
	}

	if replication.PrimaryWorkerId != "" {
		primaryNodeID, ok := durableDiskCanonicalStorageNodeID(replication.PrimaryWorkerId, allNodes)
		if !ok {
			return fmt.Errorf("durable disk %q primary storage node %s is unavailable", disk.Name, replication.PrimaryWorkerId)
		}
		if poolName != "" && !durableDiskStorageNodeIDIn(primaryNodeID, poolNodes) {
			return fmt.Errorf("durable disk %q primary storage node %s is not available in pool %q", disk.Name, primaryNodeID, poolName)
		}
		replication.PrimaryWorkerId = primaryNodeID
	}

	replicaIDs := make([]string, 0, replication.Replicas)
	addReplicaID := func(storageNodeID string) error {
		storageNodeID = strings.TrimSpace(storageNodeID)
		if storageNodeID == "" {
			return nil
		}
		canonicalID, ok := durableDiskCanonicalStorageNodeID(storageNodeID, allNodes)
		if !ok {
			return fmt.Errorf("durable disk %q requested unavailable DRBD replica storage node %s in pool %q", disk.Name, storageNodeID, poolName)
		}
		if slices.Contains(replicaIDs, canonicalID) {
			return nil
		}
		replicaIDs = append(replicaIDs, canonicalID)
		return nil
	}

	if err := addReplicaID(replication.PrimaryWorkerId); err != nil {
		return err
	}
	for _, workerID := range replication.ReplicaWorkerIds {
		if err := addReplicaID(workerID); err != nil {
			return err
		}
	}
	for _, node := range poolNodes {
		if len(replicaIDs) >= int(replication.Replicas) {
			break
		}
		if err := addReplicaID(node.ID); err != nil {
			return err
		}
	}

	if len(replicaIDs) < int(replication.Replicas) {
		return fmt.Errorf("durable disk %q needs %d DRBD storage nodes, but only %d available node(s) matched pool %q", disk.Name, replication.Replicas, len(poolNodes), poolName)
	}
	replication.ReplicaWorkerIds = replicaIDs
	if replication.PrimaryWorkerId == "" {
		replication.PrimaryWorkerId = replication.ReplicaWorkerIds[0]
	}
	return nil
}

func (gws *GatewayService) dispatchDurableDiskPrepareCommands(workspaceName string, disks []*pb.DurableDisk) error {
	if gws == nil || gws.redisClient == nil {
		return nil
	}

	eventBus := common.NewEventBus(gws.redisClient)
	for _, disk := range disks {
		if disk == nil || disk.Replication == nil || durableDiskGatewayDriver(disk.Driver) != types.DurableDiskDriverDRBD {
			continue
		}

		mount := types.Mount{
			LocalPath:   path.Join(types.DefaultDurableDisksPath, workspaceName, types.SafeDurableDiskName(disk.Name)),
			MountPath:   disk.MountPath,
			ReadOnly:    disk.ReadOnly,
			MountType:   types.StorageModeDurableDisk,
			DurableDisk: types.NewDurableDiskMountConfigFromProto(disk),
		}
		for _, storageNodeID := range disk.Replication.ReplicaWorkerIds {
			nonce, err := common.GenerateObjectId()
			if err != nil {
				return fmt.Errorf("generate durable disk command nonce: %w", err)
			}
			args, err := types.DurableDiskCommandArgs{
				StorageNodeID: storageNodeID,
				Action:        types.DurableDiskCommandActionPrepare,
				Mount:         mount,
				Nonce:         nonce,
			}.ToMap()
			if err != nil {
				return err
			}
			if _, err := eventBus.Send(&common.Event{Type: common.EventTypeDurableDiskCommand, Retries: 3, LockAndDelete: false, Args: args}); err != nil {
				return fmt.Errorf("send durable disk prepare command for storage node %s: %w", storageNodeID, err)
			}
		}
	}
	return nil
}

type durableDiskStorageNode struct {
	ID         string
	WorkerID   string
	FreeCPU    int64
	FreeMemory int64
}

func (gws *GatewayService) durableDiskCandidateStorageNodes(poolName string) ([]durableDiskStorageNode, error) {
	if gws == nil || gws.workerRepo == nil {
		return nil, fmt.Errorf("durable disk DRBD placement requires a worker repository")
	}

	var (
		workers []*types.Worker
		err     error
	)
	if poolName != "" {
		workers, err = gws.workerRepo.GetAllWorkersInPool(poolName)
	} else {
		workers, err = gws.workerRepo.GetAllWorkers()
	}
	if err != nil {
		return nil, fmt.Errorf("list workers for DRBD durable disk placement: %w", err)
	}

	byID := map[string]durableDiskStorageNode{}
	for _, worker := range workers {
		if worker == nil || worker.Id == "" || worker.Status != types.WorkerStatusAvailable {
			continue
		}
		if poolName == "" && worker.RequiresPoolSelector {
			continue
		}
		storageNodeID := worker.StorageNodeID()
		if storageNodeID == "" {
			continue
		}
		node := durableDiskStorageNode{
			ID:         storageNodeID,
			WorkerID:   worker.Id,
			FreeCPU:    worker.FreeCpu,
			FreeMemory: worker.FreeMemory,
		}
		if existing, ok := byID[storageNodeID]; !ok || durableDiskStorageNodeLess(existing, node) {
			byID[storageNodeID] = node
		}
	}

	nodes := make([]durableDiskStorageNode, 0, len(byID))
	for _, node := range byID {
		nodes = append(nodes, node)
	}
	slices.SortFunc(nodes, func(a, b durableDiskStorageNode) int {
		return strings.Compare(a.ID, b.ID)
	})
	return nodes, nil
}

func durableDiskStorageNodeIDIn(id string, nodes []durableDiskStorageNode) bool {
	_, ok := durableDiskCanonicalStorageNodeID(id, nodes)
	return ok
}

func durableDiskPreferredStorageNodeID(diskName string, nodes []durableDiskStorageNode) string {
	if len(nodes) == 0 {
		return ""
	}
	sum := sha256.Sum256([]byte(diskName))
	start := int(binary.BigEndian.Uint64(sum[:8]) % uint64(len(nodes)))
	best := nodes[start]
	for offset := 1; offset < len(nodes); offset++ {
		node := nodes[(start+offset)%len(nodes)]
		if durableDiskStorageNodeLess(best, node) {
			best = node
		}
	}
	return best.ID
}

func durableDiskStorageNodeLess(a, b durableDiskStorageNode) bool {
	if a.FreeMemory != b.FreeMemory {
		return a.FreeMemory < b.FreeMemory
	}
	if a.FreeCPU != b.FreeCPU {
		return a.FreeCPU < b.FreeCPU
	}
	return false
}

func durableDiskCanonicalStorageNodeID(id string, nodes []durableDiskStorageNode) (string, bool) {
	id = strings.TrimSpace(id)
	if id == "" {
		return "", false
	}
	for _, node := range nodes {
		if id == node.ID || id == node.WorkerID {
			return node.ID, true
		}
	}
	return "", false
}

func durableDiskGatewayDriver(configured string) string {
	if driver := types.NormalizeDurableDiskDriver(configured); driver != "" {
		return driver
	}
	return types.NormalizeDurableDiskDriver(os.Getenv("BETA9_DURABLE_DISK_DRIVER"))
}
