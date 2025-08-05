package worker

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	storage "github.com/beam-cloud/beta9/pkg/storage"
	types "github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/beam-cloud/go-runc"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	gpuCntEnvKey              = "GPU_COUNT"
	defaultCheckpointDeadline = 10 * time.Minute
	readyLogRate              = 10
)

type RestoreOpts struct {
	request    *types.ContainerRequest
	state      types.CheckpointState
	runcOpts   *runc.CreateOpts
	configPath string
}

type CRIUManager interface {
	Available() bool
	Run(ctx context.Context, request *types.ContainerRequest, bundlePath string, runcOpts *runc.CreateOpts) (int, error)
	CreateCheckpoint(ctx context.Context, request *types.ContainerRequest) (string, error)
	RestoreCheckpoint(ctx context.Context, opts *RestoreOpts) (int, error)
}

// InitializeCRIUManager initializes a new CRIU manager that can be used to checkpoint and restore containers
// -- depending on the mode, it will use either cedana or nvidia cuda checkpoint under the hood
func InitializeCRIUManager(ctx context.Context, config types.CRIUConfig) (CRIUManager, error) {
	var criuManager CRIUManager = nil
	var err error = nil

	switch config.Mode {
	case types.CRIUConfigModeCedana:
		criuManager, err = InitializeCedanaCRIU(ctx, config.Cedana)
	case types.CRIUConfigModeNvidia:
		criuManager, err = InitializeNvidiaCRIU(ctx, config)
	default:
		return nil, fmt.Errorf("invalid CRIU mode: %s", config.Mode)
	}

	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(config.Storage.MountPath, os.ModePerm); err != nil {
		return nil, err
	}

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

func (s *Worker) attemptCheckpointOrRestore(ctx context.Context, request *types.ContainerRequest, outputLogger *slog.Logger, outputWriter io.Writer, startedChan chan int, checkpointPIDChan chan int, configPath string) (int, string, error) {
	state, createCheckpoint := s.shouldCreateCheckpoint(request)

	// If checkpointing is enabled, attempt to create a checkpoint
	if createCheckpoint {
		outputLogger.Info("Attempting to create container checkpoint...")

		log.Info().Str("container_id", request.ContainerId).Msg("attempting to create checkpoint")

		exitCode, err := s.createCheckpoint(ctx, request, outputWriter, outputLogger, startedChan, checkpointPIDChan, configPath)
		if err != nil {
			return -1, "", err
		}

		return exitCode, request.ContainerId, nil
	}

	if state.Status == types.CheckpointStatusAvailable {
		err := s.waitForSyncFile(request, outputLogger)
		if err != nil {
			log.Error().Str("container_id", request.ContainerId).Msgf("failed to wait for sync file: %v", err)
			return -1, "", err
		}

		log.Info().Str("container_id", request.ContainerId).Msg("attempting to restore checkpoint")
		outputLogger.Info("Attempting to restore container checkpoint...")

		f, err := os.Create(filepath.Join(checkpointSignalDir(request.ContainerId), checkpointCompleteFileName))
		if err != nil {
			return -1, "", fmt.Errorf("failed to create checkpoint signal directory: %v", err)
		}
		defer f.Close()

		exitCode, err := s.criuManager.RestoreCheckpoint(ctx, &RestoreOpts{
			request: request,
			state:   state,
			runcOpts: &runc.CreateOpts{
				OutputWriter: outputWriter,
				Started:      startedChan,
			},
			configPath: configPath,
		})
		if err != nil {
			updateStateErr := s.updateCheckpointState(request, types.CheckpointStatusRestoreFailed)
			if updateStateErr != nil {
				log.Error().Str("container_id", request.ContainerId).Msgf("failed to update checkpoint state: %v", updateStateErr)
			}
			return exitCode, "", err
		}

		log.Info().Str("container_id", request.ContainerId).Msg("checkpoint found and restored")
		return exitCode, request.ContainerId, nil
	}

	// If a checkpoint exists but is not available (previously failed), run the container normally
	bundlePath := filepath.Dir(configPath)

	exitCode, err := s.runcHandle.Run(s.ctx, request.ContainerId, bundlePath, &runc.CreateOpts{
		OutputWriter: outputWriter,
		Started:      startedChan,
	})

	return exitCode, request.ContainerId, err
}

// Waits for the container to be ready to checkpoint at the desired point in execution, ie.
// after all processes within a container have reached a checkpointable state
func (s *Worker) createCheckpoint(ctx context.Context, request *types.ContainerRequest, outputWriter io.Writer, outputLogger *slog.Logger, startedChan chan int, checkpointPIDChan chan int, configPath string) (int, error) {
	bundlePath := filepath.Dir(configPath)

	go func() {
		pid := <-checkpointPIDChan
		log.Info().Str("container_id", request.ContainerId).Int("pid", pid).Msg("creating checkpoint")
		os.MkdirAll(filepath.Join(s.config.Worker.CRIU.Storage.MountPath, request.Workspace.Name), os.ModePerm)

		timeout := defaultCheckpointDeadline

		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		sampledLogger := log.Sample(&zerolog.BasicSampler{N: readyLogRate})
	waitForReady:
		for {
			select {
			case <-ctx.Done():
				log.Info().Str("container_id", request.ContainerId).Msg("checkpoint deadline exceeded or container exited")
				return
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
					outputLogger.Info("Container ready for checkpoint")

					break waitForReady
				} else {
					sampledLogger.Info().Str("container_id", instance.Id).Msg("container not ready for checkpoint")
				}
			}
		}

		// Proceed to create the checkpoint
		checkpointPath, err := s.criuManager.CreateCheckpoint(ctx, request)
		if err != nil {
			updateStateErr := s.updateCheckpointState(request, types.CheckpointStatusCheckpointFailed)
			if updateStateErr != nil {
				log.Error().Str("container_id", request.ContainerId).Msgf("failed to update checkpoint state: %v", updateStateErr)
			}
			log.Error().Str("container_id", request.ContainerId).Msgf("failed to create checkpoint: %v", err)
			return
		}

		err = s.createSyncFile(request, checkpointPath)
		if err != nil {
			log.Error().Str("container_id", request.ContainerId).Msgf("failed to create sync file: %v", err)
		}

		// Create a file accessible to the container to indicate that the checkpoint has been captured
		os.Create(filepath.Join(checkpointSignalDir(request.ContainerId), checkpointCompleteFileName))

		log.Info().Str("container_id", request.ContainerId).Str("checkpoint_path", checkpointPath).Msg("checkpoint created successfully")
		outputLogger.Info("Checkpoint created successfully")

		updateStateErr := s.updateCheckpointState(request, types.CheckpointStatusAvailable)
		if updateStateErr != nil {
			log.Error().Str("container_id", request.ContainerId).Msgf("failed to update checkpoint state: %v", updateStateErr)
		}
	}()

	exitCode, err := s.criuManager.Run(ctx, request, bundlePath, &runc.CreateOpts{
		OutputWriter: outputWriter,
		Started:      startedChan,
	})

	return exitCode, err
}

type syncFile struct {
	Path string `json:"path"`
}

const (
	syncFileExtension    = "crsync"
	syncFileTimeout      = 600 * time.Second
	syncFilePollInterval = 1 * time.Second
)

func (s *Worker) createSyncFile(request *types.ContainerRequest, checkpointPath string) error {
	syncFile := syncFile{
		Path: filepath.Base(checkpointPath),
	}
	syncFileBytes, err := json.Marshal(syncFile)
	if err != nil {
		log.Error().Str("container_id", request.ContainerId).Msgf("failed to marshal sync file: %v", err)
	}

	err = os.WriteFile(filepath.Join(s.config.Worker.CRIU.Storage.MountPath, fmt.Sprintf("%s.%s", request.StubId, syncFileExtension)), syncFileBytes, 0644)
	if err != nil {
		log.Error().Str("container_id", request.ContainerId).Msgf("failed to write sync file: %v", err)
	}

	return nil
}

func (s *Worker) waitForSyncFile(request *types.ContainerRequest, outputLogger *slog.Logger) error {
	timeout := syncFileTimeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	syncFilePath := filepath.Join(s.config.Worker.CRIU.Storage.MountPath, fmt.Sprintf("%s.%s", request.StubId, syncFileExtension))

	_, err := os.Stat(syncFilePath)
	if err == nil {
		return nil
	}

	outputLogger.Info("Waiting for checkpoint to sync")
	for {
		_, err := os.Stat(syncFilePath)
		if err == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			outputLogger.Info("Timeout waiting for checkpoint to sync")
			return fmt.Errorf("timeout waiting for sync file")
		default:
		}

		time.Sleep(syncFilePollInterval)
	}
}

// shouldCreateCheckpoint checks if a checkpoint should be created for a given container
// NOTE: this currently only works for deployments since functions can run multiple containers
func (s *Worker) shouldCreateCheckpoint(request *types.ContainerRequest) (types.CheckpointState, bool) {
	if !s.IsCRIUAvailable(request.GpuCount) || !request.CheckpointEnabled {
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

func (s *Worker) IsCRIUAvailable(gpuCount uint32) bool {
	if s.criuManager == nil {
		log.Info().Msg("manager not initialized")
		return false
	}

	if !s.criuManager.Available() {
		log.Info().Msg("manager not available")
		return false
	}

	poolName := os.Getenv("WORKER_POOL_NAME")
	if poolName == "" {
		log.Info().Msg("pool name not set")
		return false
	}

	pool, ok := s.config.Worker.Pools[poolName]
	if !ok {
		log.Info().Msg("pool not found")
		return false
	}

	return pool.CRIUEnabled
}

func (s *Worker) updateCheckpointState(request *types.ContainerRequest, status types.CheckpointStatus) error {
	_, err := handleGRPCResponse(s.containerRepoClient.UpdateCheckpointState(context.Background(), &pb.UpdateCheckpointStateRequest{
		ContainerId:   request.ContainerId,
		CheckpointId:  request.StubId,
		WorkspaceName: request.Workspace.Name,
		CheckpointState: &pb.CheckpointState{
			Status:      string(status),
			ContainerId: request.ContainerId,
			StubId:      request.StubId,
		},
	}))

	return err
}
