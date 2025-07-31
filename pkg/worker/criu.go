package worker

import (
	"context"
	_ "embed"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
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
	CacheCheckpoint(containerId, checkpointPath string) (string, error)
}

// InitializeCRIUManager initializes a new CRIU manager that can be used to checkpoint and restore containers
// -- depending on the mode, it will use either cedana or nvidia cuda checkpoint under the hood
func InitializeCRIUManager(ctx context.Context, criuConfig types.CRIUConfig, storageConfig types.StorageConfig, fileCacheManager *FileCacheManager) (CRIUManager, error) {
	var criuManager CRIUManager = nil
	var err error = nil

	switch criuConfig.Mode {
	case types.CRIUConfigModeCedana:
		criuManager, err = InitializeCedanaCRIU(ctx, criuConfig.Cedana, fileCacheManager)
	case types.CRIUConfigModeNvidia:
		criuManager, err = InitializeNvidiaCRIU(ctx, criuConfig, fileCacheManager)
	default:
		return nil, fmt.Errorf("invalid CRIU mode: %s", criuConfig.Mode)
	}

	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(criuConfig.Storage.MountPath, os.ModePerm); err != nil {
		return nil, err
	}

	// If storage mode is S3, mount the checkpoint storage bucket
	if criuConfig.Storage.Mode == string(types.CheckpointStorageModeS3) {
		checkpointStorage, _ := storage.NewAlluxioStorage(types.AlluxioConfig{
			ImageUrl:            storageConfig.Alluxio.ImageUrl,
			License:             storageConfig.Alluxio.License,
			EtcdEndpoint:        storageConfig.Alluxio.EtcdEndpoint,
			EtcdUsername:        storageConfig.Alluxio.EtcdUsername,
			EtcdPassword:        storageConfig.Alluxio.EtcdPassword,
			EtcdTlsEnabled:      storageConfig.Alluxio.EtcdTlsEnabled,
			CoordinatorHostname: storageConfig.Alluxio.CoordinatorHostname,
			BucketName:          storageConfig.Alluxio.BucketName,
			AccessKey:           storageConfig.Alluxio.AccessKey,
			SecretKey:           storageConfig.Alluxio.SecretKey,
			EndpointURL:         storageConfig.Alluxio.EndpointURL,
			Region:              storageConfig.Alluxio.Region,
			ReadOnly:            storageConfig.Alluxio.ReadOnly,
			ForcePathStyle:      storageConfig.Alluxio.ForcePathStyle,
		})

		err := checkpointStorage.Mount(criuConfig.Storage.MountPath)
		if err != nil {
			log.Warn().Msgf("C/R unavailable, unable to mount checkpoint storage: %v", err)
			return nil, err
		}
	}

	return criuManager, nil
}

func (s *Worker) attemptCheckpointOrRestore(ctx context.Context, request *types.ContainerRequest, outputWriter io.Writer, startedChan chan int, checkpointPIDChan chan int, configPath string) (int, string, error) {
	state, createCheckpoint := s.shouldCreateCheckpoint(request)

	// If checkpointing is enabled, attempt to create a checkpoint
	if createCheckpoint {
		log.Info().Str("container_id", request.ContainerId).Msg("attempting to create checkpoint")
		exitCode, err := s.createCheckpoint(ctx, request, outputWriter, startedChan, checkpointPIDChan, configPath)
		if err != nil {
			return -1, "", err
		}

		return exitCode, request.ContainerId, nil
	}

	if state.Status == types.CheckpointStatusAvailable {
		log.Info().Str("container_id", request.ContainerId).Msg("attempting to restore checkpoint")
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
func (s *Worker) createCheckpoint(ctx context.Context, request *types.ContainerRequest, outputWriter io.Writer, startedChan chan int, checkpointPIDChan chan int, configPath string) (int, error) {
	wg := sync.WaitGroup{}
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

		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.criuManager.CacheCheckpoint(request.ContainerId, checkpointPath)
			if err != nil {
				log.Error().Str("container_id", request.ContainerId).Msgf("failed to cache checkpoint: %v", err)
			}
		}()

		// Create a file accessible to the container to indicate that the checkpoint has been captured
		os.Create(filepath.Join(checkpointSignalDir(request.ContainerId), checkpointCompleteFileName))
		log.Info().Str("container_id", request.ContainerId).Msg("checkpoint created successfully")

		updateStateErr := s.updateCheckpointState(request, types.CheckpointStatusAvailable)
		if updateStateErr != nil {
			log.Error().Str("container_id", request.ContainerId).Msgf("failed to update checkpoint state: %v", updateStateErr)
		}
	}()

	exitCode, err := s.criuManager.Run(ctx, request, bundlePath, &runc.CreateOpts{
		OutputWriter: outputWriter,
		Started:      startedChan,
	})

	wg.Wait()
	return exitCode, err
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
