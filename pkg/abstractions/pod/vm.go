package pod

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/labstack/echo/v4"
)

// RunVM reuses sandbox scheduling while preserving the VM service's reserved
// runtime identity. Never allow a VM to fall back to a container runtime.
func (s *GenericPodService) RunVM(ctx context.Context, info *auth.AuthInfo, stubID, containerID string, vmSpec types.VMSpec, checkpointID string, checkpointDisks map[string]string) error {
	stub, err := s.backendRepo.GetStubByExternalId(ctx, stubID)
	if err != nil {
		return err
	}

	if stub == nil {
		return fmt.Errorf("VM stub not found")
	}

	var spec types.StubConfigV1
	if err := json.Unmarshal([]byte(stub.Config), &spec); err != nil {
		return err
	}

	if !spec.IsPersistentVM() || spec.RequiresGPU() || !stub.Type.IsSandbox() {
		return fmt.Errorf("persistent VM requires a CPU microvm stub")
	}

	spec.Ports = vmSpec.RuntimePorts()
	spec.BlockNetwork = vmSpec.BlockNetwork
	spec.AllowList = vmSpec.AllowList
	var checkpoint *types.Checkpoint
	if checkpointID != "" {
		checkpoint, err = s.backendRepo.GetCheckpointById(ctx, checkpointID)
		if err != nil {
			return err
		}

		if checkpoint.WorkspaceId != info.Workspace.Id || checkpoint.StubId != stub.Id || checkpoint.Runtime != types.ContainerRuntimeMicroVM.String() || checkpoint.Status != string(types.CheckpointStatusAvailable) {
			return fmt.Errorf("VM memory checkpoint is unavailable or incompatible")
		}

		// These launch-only snapshot IDs already travel in durable-disk mounts
		// through worker RPCs. Warm VM mounts interpret them as exact heads.
		for _, disk := range spec.Disks {
			paired := checkpointDisks[disk.Name]
			if paired == "" {
				return fmt.Errorf("VM memory checkpoint has no paired disk %s", disk.Name)
			}

			disk.SourceSnapshotId = paired
		}
	}

	data, err := json.Marshal(spec)
	if err != nil {
		return err
	}

	stub.Config = string(data)
	workerID, err := s.vmDiskWorker(ctx, info.Workspace.Id, spec.Disks, checkpoint != nil)
	if err != nil {
		return err
	}
	_, err = s.run(ctx, info, stub, runOptions{containerId: containerID, checkpoint: checkpoint, preferredWorkerId: workerID})
	return err
}

// The publishing worker retains the local qcow layers. Prefer it when it
// still fits, rather than downloading the whole disk on another idle node.
func (s *GenericPodService) vmDiskWorker(ctx context.Context, workspaceID uint, disks []*pb.DurableDisk, restoringMemory bool) (string, error) {
	for _, disk := range disks {
		if disk.MountPath != types.DurableDiskRootMountPath {
			continue
		}
		var snapshot *types.DiskSnapshot
		var err error
		if restoringMemory {
			snapshot, err = s.backendRepo.GetDiskSnapshot(ctx, workspaceID, disk.SourceSnapshotId)
		} else {
			snapshot, err = s.backendRepo.GetLatestDiskSnapshot(ctx, workspaceID, disk.Name)
			var missing *types.ErrDiskSnapshotNotFound
			if errors.As(err, &missing) {
				if disk.SourceSnapshotId == "" {
					return "", nil
				}
				snapshot, err = s.backendRepo.GetDiskSnapshot(ctx, workspaceID, disk.SourceSnapshotId)
			}
		}
		if err != nil {
			return "", err
		}
		if snapshot != nil {
			return snapshot.SourceWorkerId, nil
		}
		return "", nil
	}
	return "", nil
}

// Reuse the proxy's actual backend route, including private worker transport.
func (s *GenericPodService) VMPortReady(ctx context.Context, stubID, containerID string, port uint32) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	instance, err := s.getOrCreatePodInstance(stubID)
	if err != nil {
		return false, err
	}

	return instance.buffer.primeContainerPort(containerID, int32(port), time.Second), nil
}

func (s *GenericPodService) ForwardVM(ctx echo.Context, stubID, containerID string) error {
	return s.forwardContainerRequest(ctx, stubID, containerID)
}
