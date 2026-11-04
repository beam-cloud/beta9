package vm

import (
	"context"
	"fmt"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/labstack/echo/v4"
)

// pause pairs RAM with the disk generations sealed while the hypervisor is
// paused. The terminal checkpoint owns shutdown; a normal systemd shutdown
// would change those disks underneath the saved memory image.
func (s *Service) pause(ctx context.Context, v *types.VM) error {
	defer common.StartupSpan(ctx, "vm.pause")()
	if v.Spec.GPU != "" {
		return echo.NewHTTPError(409, "GPU VMs cannot pause memory; stop to preserve saved files")
	}
	if v.DesiredState == types.VMStatePaused {
		if v.MemoryCheckpointID == "" {
			return echo.NewHTTPError(409, "memory pause did not complete; use start with cold=true to recover from disk")
		}

		return s.stop(ctx, v, false)
	}

	state, err := s.runtimeState(v)
	if err != nil {
		return err
	}

	if state == nil || state.Status != types.ContainerStatusRunning {
		return echo.NewHTTPError(409, "only a running VM can be paused")
	}

	v.DesiredState, v.Status = types.VMStatePaused, types.VMStatePausing
	v.MemoryCheckpointID, v.MemoryDiskSnapshots = "", nil
	endIntent := common.StartupSpan(ctx, "pause.save_intent")
	saveErr := s.repo.SaveVM(ctx, v)
	endIntent()
	if err := saveErr; err != nil {
		return err
	}

	endCapture := common.StartupSpan(ctx, "pause.capture_and_persist")
	resp, err := s.runtime.SandboxSnapshotMemory(ctx, &pb.PodSandboxSnapshotMemoryRequest{StubId: v.StubID, ContainerId: v.ContainerID, TerminateAfterCheckpoint: true})
	endCapture()
	if err != nil {
		return err
	}

	if !resp.Ok || resp.CheckpointId == "" || resp.Runtime != types.ContainerRuntimeMicroVM.String() {
		return fmt.Errorf("memory pause failed: %s", resp.ErrorMsg)
	}

	paired := make(map[string]string, len(resp.DiskSnapshots))
	for _, disk := range resp.DiskSnapshots {
		if disk != nil && disk.SnapshotId != "" {
			paired[disk.DiskName] = disk.SnapshotId
		}
	}

	if paired[rootDisk(v)] == "" {
		return fmt.Errorf("memory checkpoint did not contain this VM's durable root")
	}

	for _, disk := range v.Spec.Disks {
		if paired[disk.Name] == "" {
			return fmt.Errorf("memory checkpoint did not contain durable disk %s", disk.Name)
		}
	}

	v.MemoryCheckpointID, v.MemoryDiskSnapshots = resp.CheckpointId, paired
	v.RootSnapshotID = paired[rootDisk(v)]
	endPairing := common.StartupSpan(ctx, "pause.save_pairing")
	saveErr = s.repo.SaveVM(ctx, v)
	endPairing()
	if err := saveErr; err != nil {
		return err
	}

	defer common.StartupSpan(ctx, "pause.finalize")()
	return s.stop(ctx, v, false)
}

// An independently mounted disk may have changed while compute was released.
// Refuse stale RAM rather than presenting the guest with inconsistent storage.
func (s *Service) validateMemoryDisks(ctx context.Context, v *types.VM) error {
	if v.MemoryCheckpointID == "" {
		return nil
	}

	if v.MemoryDiskSnapshots[rootDisk(v)] == "" {
		return echo.NewHTTPError(409, "memory checkpoint has no paired root disk; use cold=true")
	}

	for name, snapshot := range v.MemoryDiskSnapshots {
		latest, err := s.backend.GetLatestDiskSnapshot(ctx, v.WorkspaceID, name)
		if err != nil {
			return fmt.Errorf("validate paused disk %s: %w", name, err)
		}

		if latest.ExternalId != snapshot {
			return echo.NewHTTPError(409, "disk "+name+" changed after memory pause; use cold=true to boot from disk")
		}
	}

	return nil
}
