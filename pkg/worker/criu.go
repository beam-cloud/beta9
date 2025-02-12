package worker

import (
	"context"
	_ "embed"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	storage "github.com/beam-cloud/beta9/pkg/storage"
	types "github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/beam-cloud/go-runc"
	"github.com/rs/zerolog/log"
)

type CRIUManager interface {
	Available() bool
	Run(ctx context.Context, containerId string, bundle string, gpuEnabled bool, runcOpts *runc.CreateOpts) (chan int, error)
	CreateCheckpoint(ctx context.Context, request *types.ContainerRequest) (string, error)
	RestoreCheckpoint(ctx context.Context, request *types.ContainerRequest, state types.CheckpointState, runcOpts *runc.CreateOpts) (chan int, error)
}

const defaultCheckpointDeadline = 10 * time.Minute

// InitializeCRIUManager initializes a new CRIU manager that can be used to checkpoint and restore containers
// -- depending on the mode, it will use either cedana or nvidia cuda checkpoint under the hood
func InitializeCRIUManager(ctx context.Context, config types.CRIUConfig) (CRIUManager, error) {
	var criuManager CRIUManager = nil
	var err error = nil

	switch config.Mode {
	case types.CRIUConfigModeCedana:
		criuManager, err = InitializeCedanaCRIU(ctx, config.Cedana)
	case types.CRIUConfigModeNvidia:
		criuManager, err = InitializeNvidiaCRIU(ctx, config.Nvidia)
	default:
		return nil, fmt.Errorf("invalid CRIU mode: %s", config.Mode)
	}

	if err != nil {
		return nil, err
	}

	os.MkdirAll(config.Storage.MountPath, os.ModePerm)

	// If storage mode is S3, mount the checkpoint storage as a FUSE filesystem
	if config.Storage.Mode == string(types.CheckpointStorageModeS3) {
		checkpointStorage, _ := storage.NewMountPointStorage(types.MountPointConfig{
			BucketName:  config.Storage.ObjectStore.BucketName,
			AccessKey:   config.Storage.ObjectStore.AccessKey,
			SecretKey:   config.Storage.ObjectStore.SecretKey,
			EndpointURL: config.Storage.ObjectStore.EndpointURL,
			Region:      config.Storage.ObjectStore.Region,
			ReadOnly:    false,
		})

		err := checkpointStorage.Mount(config.Storage.MountPath)
		if err != nil {
			log.Warn().Msgf("C/R unavailable, unable to mount checkpoint storage: %v", err)
			return nil, err
		}
	}

	return criuManager, nil
}

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
		os.Create(filepath.Join(checkpointSignalDir(request.ContainerId), checkpointCompleteFileName))

		exitCodeChan, err := s.criuManager.RestoreCheckpoint(ctx, request, state, &runc.CreateOpts{
			Detach:       true,
			OutputWriter: outputWriter,
			ConfigPath:   configPath,
			Started:      startedChan,
		})

		if err != nil {
			log.Error().Str("container_id", request.ContainerId).Msgf("failed to restore checkpoint: %v", err)

			s.containerRepoClient.UpdateCheckpointState(ctx, &pb.UpdateCheckpointStateRequest{
				ContainerId:   request.ContainerId,
				CheckpointId:  request.StubId,
				WorkspaceName: request.Workspace.Name,
				CheckpointState: &pb.CheckpointState{
					Status:      string(types.CheckpointStatusRestoreFailed),
					ContainerId: request.ContainerId,
					StubId:      request.StubId,
				},
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
	log.Info().Str("container_id", request.ContainerId).Msg("creating checkpoint")
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
	checkpointPath, err := s.criuManager.CreateCheckpoint(ctx, request)
	if err != nil {
		s.containerRepoClient.UpdateCheckpointState(ctx, &pb.UpdateCheckpointStateRequest{
			ContainerId:   request.ContainerId,
			CheckpointId:  request.StubId,
			WorkspaceName: request.Workspace.Name,
			CheckpointState: &pb.CheckpointState{
				Status:      string(types.CheckpointStatusCheckpointFailed),
				ContainerId: request.ContainerId,
				StubId:      request.StubId,
			},
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

	_, err = handleGRPCResponse(s.containerRepoClient.UpdateCheckpointState(ctx, &pb.UpdateCheckpointStateRequest{
		ContainerId:   request.ContainerId,
		CheckpointId:  request.StubId,
		WorkspaceName: request.Workspace.Name,
		CheckpointState: &pb.CheckpointState{
			Status:      string(types.CheckpointStatusAvailable),
			ContainerId: request.ContainerId, // We store this as a reference to the container that we initially checkpointed
			StubId:      request.StubId,
			RemoteKey:   remoteKey,
		},
	}))
	if err != nil {
		log.Error().Str("container_id", request.ContainerId).Msgf("failed to update checkpoint state: %v", err)
	}

	return nil
}

// shouldCreateCheckpoint checks if a checkpoint should be created for a given container
// NOTE: this currently only works for deployments since functions can run multiple containers
func (s *Worker) shouldCreateCheckpoint(request *types.ContainerRequest) (types.CheckpointState, bool) {
	if !s.IsCRIUAvailable() || !request.CheckpointEnabled {
		return types.CheckpointState{}, false
	}

	response, err := handleGRPCResponse(s.containerRepoClient.GetCheckpointState(context.Background(), &pb.GetCheckpointStateRequest{
		WorkspaceName: request.Workspace.Name,
		CheckpointId:  request.StubId,
	}))
	if err != nil {
		notFoundErr := &types.ErrCheckpointNotFound{
			CheckpointId: request.StubId,
		}
		if err.Error() == notFoundErr.Error() {
			log.Info().Str("container_id", request.ContainerId).Msg("checkpoint not found, creating checkpoint")
			return types.CheckpointState{Status: types.CheckpointStatusNotFound}, true
		}

		return types.CheckpointState{}, false
	}

	state := types.CheckpointState{
		Status:      types.CheckpointStatus(response.CheckpointState.Status),
		ContainerId: response.CheckpointState.ContainerId,
		StubId:      response.CheckpointState.StubId,
		RemoteKey:   response.CheckpointState.RemoteKey,
	}

	return state, false
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
	if s.criuManager == nil {
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
