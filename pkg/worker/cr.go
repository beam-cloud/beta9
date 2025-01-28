package worker

import (
	"context"
	_ "embed"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	types "github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/go-runc"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/rs/zerolog/log"
)

const defaultCheckpointDeadline = 10 * time.Minute

func (s *Worker) attemptCheckpointOrRestore(ctx context.Context, request *types.ContainerRequest, outputWriter io.Writer, startedChan chan int, configPath string) (chan int, string, error) {
	state, createCheckpoint := s.shouldCreateCheckpoint(request)

	// If checkpointing is enabled, attempt to create a checkpoint
	if createCheckpoint {
		go func() {
			err := s.createCheckpoint(ctx, request)
			if err != nil {
				log.Error().Str("container_id", request.ContainerId).Msgf("failed to create checkpoint: %v", err)
			}
		}()
	} else if state.Status == types.CheckpointStatusAvailable {
		checkpointPath := filepath.Join(s.config.Worker.CRIU.Storage.MountPath, state.RemoteKey)

		os.Create(filepath.Join(checkpointSignalDir(request.ContainerId), checkpointCompleteFileName))

		exitCodeChan, err := s.cedanaClient.Restore(ctx, cedanaRestoreOpts{
			checkpointPath: checkpointPath,
			jobId:          state.ContainerId,
			containerId:    request.ContainerId,
			cacheFunc:      s.cacheCheckpoint,
		}, &runc.CreateOpts{
			Detach:        true,
			OutputWriter:  outputWriter,
			ConfigPath:    configPath,
			Started:       startedChan,
		})

		if err != nil {
			log.Error().Str("container_id", request.ContainerId).Msgf("failed to restore checkpoint: %v", err)

			s.containerRepo.UpdateCheckpointState(request.Workspace.Name, request.StubId, &types.CheckpointState{
				Status:      types.CheckpointStatusRestoreFailed,
				ContainerId: request.ContainerId,
				StubId:      request.StubId,
			})

			return nil, "", err
		} else {
			log.Info().Str("container_id", request.ContainerId).Msg("checkpoint found and restored")
			return exitCodeChan, request.ContainerId, nil
		}
	}

	return nil, "", nil
}

// Waits for the container to be ready to checkpoint at the desired point in execution, ie.
// after all processes within a container have reached a checkpointable state
func (s *Worker) createCheckpoint(ctx context.Context, request *types.ContainerRequest) error {
	os.MkdirAll(filepath.Join(s.config.Worker.CRIU.Storage.MountPath, request.Workspace.Name), os.ModePerm)

	timeout := defaultCheckpointDeadline

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
				log.Info().Str("container_id", request.ContainerId).Msg("container instance not found yet")
				continue
			}

			// Check if the container is ready for checkpoint by verifying the existence of a signal file
			readyFilePath := filepath.Join(checkpointSignalDir(instance.Id), checkpointSignalFileName)
			if _, err := os.Stat(readyFilePath); err == nil {
				log.Info().Str("container_id", instance.Id).Msg("container ready for checkpoint")
				break waitForReady
			} else {
				log.Info().Str("container_id", instance.Id).Msg("container not ready for checkpoint")
			}

		}
	}

	// Proceed to create the checkpoint
	checkpointPath, err := s.cedanaClient.Checkpoint(ctx, request.ContainerId)
	if err != nil {
		s.containerRepo.UpdateCheckpointState(request.Workspace.Name, request.StubId, &types.CheckpointState{
			Status:      types.CheckpointStatusCheckpointFailed,
			ContainerId: request.ContainerId,
			StubId:      request.StubId,
		})
		return err
	}

	// Create a file accessible to the container to indicate that the checkpoint has been captured
	os.Create(filepath.Join(checkpointSignalDir(request.ContainerId), checkpointCompleteFileName))

	// Move compressed checkpoint file to long-term storage directory
	remoteKey := filepath.Join(request.Workspace.Name, filepath.Base(checkpointPath))
	err = copyFile(checkpointPath, filepath.Join(s.config.Worker.CRIU.Storage.MountPath, remoteKey))
	if err != nil {
		return err
	}

	err = os.Remove(checkpointPath)
	if err != nil {
		log.Error().Str("container_id", request.ContainerId).Msgf("failed to delete temporary checkpoint file: %v", err)
	}

	log.Info().Str("container_id", request.ContainerId).Msg("checkpoint created successfully")

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
	if !s.IsCRIUAvailable() || !request.CheckpointEnabled {
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

// Wait for a restored container to exit
func (s *Worker) waitForRestoredContainer(ctx context.Context, containerId string, startedChan chan int, outputLogger *slog.Logger, request *types.ContainerRequest, spec *specs.Spec) int {
	pid := <-startedChan

	// Clean up runc container state and send final output message
	cleanup := func(exitCode int, err error) int {
		log.Info().Str("container_id", containerId).Msgf("container has exited with code: %d", exitCode)

		outputLogger.Info("", "done", true, "success", exitCode == 0)

		err = s.runcHandle.Delete(s.ctx, containerId, &runc.DeleteOpts{Force: true})
		if err != nil {
			log.Error().Str("container_id", containerId).Msgf("failed to delete container: %v", err)
		}

		return exitCode
	}

	go s.collectAndSendContainerMetrics(ctx, request, spec, pid) // Capture resource usage (cpu/mem/gpu)
	go s.watchOOMEvents(ctx, request, outputLogger)              // Watch for OOM events

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return cleanup(0, nil)
		case <-ticker.C:
			state, err := s.runcHandle.State(ctx, containerId)
			if err != nil {
				return cleanup(-1, err)
			}

			if state.Status != types.RuncContainerStatusRunning && state.Status != types.RuncContainerStatusPaused {
				return cleanup(0, nil)
			}
		}
	}
}

// Caches a checkpoint nearby if the file cache is available
func (s *Worker) cacheCheckpoint(containerId, checkpointPath string) (string, error) {
	cachedCheckpointPath := filepath.Join(baseFileCachePath, checkpointPath)

	if s.fileCacheManager.CacheAvailable() {

		// If the checkpoint is already cached, we can use that path without the extra grpc call
		if _, err := os.Stat(cachedCheckpointPath); err == nil {
			return cachedCheckpointPath, nil
		}

		log.Info().Str("container_id", containerId).Msgf("caching checkpoint nearby: %s", checkpointPath)
		client := s.fileCacheManager.GetClient()

		// Remove the leading "/" from the checkpoint path
		_, err := client.StoreContentFromSource(checkpointPath[1:], 0)
		if err != nil {
			return "", err
		}

		checkpointPath = cachedCheckpointPath
	}

	return checkpointPath, nil
}

func (s *Worker) IsCRIUAvailable() bool {
	if s.cedanaClient == nil {
		return false
	}

	poolName := os.Getenv("WORKER_POOL_NAME")
	if poolName == "" {
		return false
	}

	pool, ok := s.config.Worker.Pools[poolName]
	if !ok {
		return false
	}

	return pool.CRIUEnabled
}
