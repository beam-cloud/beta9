package vm

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/labstack/echo/v4"
)

// pause pairs RAM with the disk generations sealed while the hypervisor is
// paused. The terminal checkpoint owns shutdown; a normal systemd shutdown
// would change those disks underneath the saved memory image.
func (s *Service) pause(ctx context.Context, v *types.VM) error {
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
	if err := s.repo.SaveVM(ctx, v); err != nil {
		return err
	}

	resp, err := s.snapshotMemory(ctx, v)
	if err != nil {
		return err
	}

	if !resp.Ok || resp.CheckpointId == "" || resp.Runtime != types.ContainerRuntimeMicroVM.String() {
		if !s.guestAnswers(ctx, v) {
			return fmt.Errorf("memory pause failed: %s", resp.ErrorMsg)
		}

		// A worker that cannot pause a guest either ends it or leaves it
		// as it was. This one is as it was: a process that is not a CUDA
		// program holds its GPU, for example.
		v.DesiredState, v.Status = types.VMStateRunning, types.VMStateRunning
		if err := s.repo.SaveVM(ctx, v); err != nil {
			return err
		}

		return errPauseRefused{reason: resp.ErrorMsg}
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
	if err := s.repo.SaveVM(ctx, v); err != nil {
		return err
	}

	return s.stop(ctx, v, false)
}

// pauseContainerPoll is how often a pause looks for the container it saves,
// and pauseContainerGrace how long it waits for the worker's answer once that
// container is gone: the worker of a pause that succeeds ends the container
// as it answers.
var (
	pauseContainerPoll  = time.Second
	pauseContainerGrace = 30 * time.Second
)

var errPauseContainerGone = errors.New("memory pause failed: the VM's container ended before its worker answered")

// snapshotMemory asks the VM's worker to save its memory and end it, and
// waits for the answer only while there can be one. A worker that restarts
// takes the guest with it and has nothing to say of it afterwards: the pause
// held the VM for its 10 minutes, and nothing could start or remove it.
func (s *Service) snapshotMemory(ctx context.Context, v *types.VM) (*pb.PodSandboxSnapshotMemoryResponse, error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	containerID, poll, grace := v.ContainerID, pauseContainerPoll, pauseContainerGrace
	go func() {
		tick := time.NewTicker(poll)
		defer tick.Stop()
		var gone time.Time
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}

			if _, err := s.containers.GetContainerState(containerID); !(&types.ErrContainerStateNotFound{}).From(err) {
				gone = time.Time{}
				continue
			}

			if gone.IsZero() {
				gone = time.Now()
			}

			if time.Since(gone) >= grace {
				cancel(errPauseContainerGone)
				return
			}
		}
	}()

	resp, err := s.runtime.SandboxSnapshotMemory(ctx, &pb.PodSandboxSnapshotMemoryRequest{StubId: v.StubID, ContainerId: containerID, TerminateAfterCheckpoint: true})
	if (err != nil || !resp.Ok) && context.Cause(ctx) == errPauseContainerGone {
		return nil, errPauseContainerGone
	}

	return resp, err
}

// errPauseRefused reports a VM that could not pause and is still running.
type errPauseRefused struct{ reason string }

func (e errPauseRefused) Error() string {
	return "VM cannot pause its memory and is still running: " + e.reason
}

// guestAnswers asks the guest itself, through its process manager. Container
// state trails a hypervisor that the failed pause has just ended.
func (s *Service) guestAnswers(ctx context.Context, v *types.VM) bool {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := s.runtime.SandboxStatFile(ctx, &pb.PodSandboxStatFileRequest{ContainerId: v.ContainerID, ContainerPath: "/"})
	return err == nil && resp.Ok
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
