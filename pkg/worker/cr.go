package worker

import (
	"context"
	_ "embed"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	types "github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/go-runc"
)

func (s *Worker) attemptCheckpointOrRestore(ctx context.Context, request *types.ContainerRequest, consoleWriter *ConsoleWriter, pidChan chan int, configPath string) error {
	state, createCheckpoint := s.shouldCreateCheckpoint(request)

	// If checkpointing is enabled, attempt to create a checkpoint
	if createCheckpoint {
		go s.createCheckpoint(ctx, request)
	} else if state.Status == types.CheckpointStatusAvailable {
		checkpointPath := filepath.Join(s.config.Worker.Checkpointing.Storage.MountPath, state.RemoteKey)
		processState, err := s.cedanaClient.Restore(ctx, state.ContainerId, checkpointPath, &runc.CreateOpts{
			Detach:        true,
			ConsoleSocket: consoleWriter,
			ConfigPath:    configPath,
			Started:       pidChan,
		})
		if err != nil {
			log.Printf("<%s> - failed to restore checkpoint: %v\n", request.ContainerId, err)

			// TODO: if restore fails, update checkpoint state to restore_failed

			return err
		} else {
			log.Printf("<%s> - checkpoint found and restored, process state: %+v\n", request.ContainerId, processState)

			err = s.wait(state.ContainerId)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Waits for the container to be ready to checkpoint at the desired point in execution, ie.
// after all processes within a container have reached a checkpointable state
func (s *Worker) createCheckpoint(ctx context.Context, request *types.ContainerRequest) error {
	os.MkdirAll(filepath.Join(s.config.Worker.Checkpointing.Storage.MountPath, request.Workspace.Name), os.ModePerm)

	timeout := defaultCheckpointDeadline
	managing := false
	gpuEnabled := request.Gpu != ""

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

waitForReady:
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("checkpoint deadline exceeded or container exited")
		case <-ticker.C:
			instance, exists := s.containerInstances.Get(request.ContainerId)
			if !exists {
				log.Printf("<%s> - container instance not found yet\n", request.ContainerId)
				continue
			}

			// Start managing the container with Cedana
			if !managing {
				err := s.cedanaClient.Manage(ctx, instance.Id, gpuEnabled)
				if err == nil {
					managing = true
				} else {
					log.Printf("<%s> - cedana manage failed, container may not be started yet: %+v\n", instance.Id, err)
				}
				continue
			}

			// Check if the endpoint is ready for checkpoint by verifying the existence of the file
			readyFilePath := fmt.Sprintf("/tmp/%s/cedana/READY_FOR_CHECKPOINT", instance.Id)
			if _, err := os.Stat(readyFilePath); err == nil {
				log.Printf("<%s> - endpoint is ready for checkpoint.\n", instance.Id)
				break waitForReady
			} else {
				log.Printf("<%s> - endpoint not ready for checkpoint.\n", instance.Id)
			}

		}
	}

	// Proceed to create the checkpoint
	checkpointPath, err := s.cedanaClient.Checkpoint(ctx, request.ContainerId)
	if err != nil {
		log.Printf("<%s> - cedana checkpoint failed: %+v\n", request.ContainerId, err)

		s.containerRepo.UpdateCheckpointState(request.Workspace.Name, request.StubId, &types.CheckpointState{
			Status:      types.CheckpointStatusCheckpointFailed,
			ContainerId: request.ContainerId,
			StubId:      request.StubId,
		})
		return err
	}

	// Move compressed checkpoint file to long-term storage directory
	remoteKey := filepath.Join(request.Workspace.Name, filepath.Base(checkpointPath))
	err = copyFile(checkpointPath, filepath.Join(s.config.Worker.Checkpointing.Storage.MountPath, remoteKey))
	if err != nil {
		log.Printf("<%s> - failed to copy checkpoint to storage: %v\n", request.ContainerId, err)
		return err
	}

	// TODO: Delete checkpoint files from local disk in /tmp

	log.Printf("<%s> - checkpoint created successfully\n", request.ContainerId)
	return s.containerRepo.UpdateCheckpointState(request.Workspace.Name, request.StubId, &types.CheckpointState{
		Status:      types.CheckpointStatusAvailable,
		ContainerId: request.ContainerId, // We store this as a reference to the container that we initially checkpointed
		StubId:      request.StubId,
		RemoteKey:   remoteKey,
	})
}

// shouldCreateCheckpoint checks if a checkpoint should be created for a given container
// NOTE: this currently only works for deployments since functions can run multiple containers
func (s *Worker) shouldCreateCheckpoint(request *types.ContainerRequest) (types.CheckpointState, bool) {
	if !s.checkpointingAvailable || !request.CheckpointEnabled {
		return types.CheckpointState{}, false
	}

	state, err := s.containerRepo.GetCheckpointState(request.Workspace.Name, request.StubId)
	if err != nil {
		if _, ok := err.(*types.ErrCheckpointNotFound); !ok {
			return types.CheckpointState{}, false
		}

		// If checkpoint not found, we can proceed to create one
		return types.CheckpointState{Status: types.CheckpointStatusNotFound}, true
	}

	return *state, false
}
