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
	"strings"
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
	state      *types.Checkpoint
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

func (s *Worker) attemptAutoCheckpoint(ctx context.Context, request *types.ContainerRequest, outputLogger *slog.Logger, outputWriter io.Writer, startedChan chan int, checkpointPIDChan chan int) {
	checkpointId := request.StubId

	// If checkpointing is enabled and there is no existing checkpoint, attempt to create a checkpoint
	if s.shouldCreateCheckpoint(request) {
		outputLogger.Info("Attempting to create container checkpoint...")

		err := s.createCheckpoint(ctx, &CreateCheckpointOpts{
			Request:           request,
			CheckpointId:      checkpointId,
			OutputWriter:      outputWriter,
			OutputLogger:      outputLogger,
			StartedChan:       startedChan,
			CheckpointPIDChan: checkpointPIDChan,
			WaitForSignal:     true,
		})
		if err != nil {
			log.Error().Str("container_id", request.ContainerId).Msgf("failed to create checkpoint: %v", err)
			return
		}
	}
}

type CreateCheckpointOpts struct {
	Request           *types.ContainerRequest
	CheckpointId      string
	ContainerIp       string
	OutputWriter      io.Writer
	OutputLogger      *slog.Logger
	StartedChan       chan int
	CheckpointPIDChan chan int
	WaitForSignal     bool
}

// Waits for the container to be ready to checkpoint at the desired point in execution, ie.
// after all processes within a container have reached a checkpointable state
func (s *Worker) createCheckpoint(ctx context.Context, opts *CreateCheckpointOpts) error {
	pid := <-opts.CheckpointPIDChan

	log.Info().Str("container_id", opts.Request.ContainerId).Str("checkpoint_id", opts.CheckpointId).Int("pid", pid).Msg("creating checkpoint")

	if opts.WaitForSignal {
		err := s.waitForCheckpointSignal(ctx, opts.Request, opts.OutputLogger)
		if err != nil {
			log.Error().Str("container_id", opts.Request.ContainerId).Str("checkpoint_id", opts.CheckpointId).Msgf("failed to wait for checkpoint signal: %v", err)
			return err
		}
	}

	// Proceed to create the checkpoint
	checkpointPath, err := s.criuManager.CreateCheckpoint(ctx, opts.Request)
	if err != nil {
		err := s.createCheckpointState(opts.CheckpointId, opts.Request, types.CheckpointStatusCheckpointFailed, opts.ContainerIp, []uint32{})
		if err != nil {
			log.Error().Str("container_id", opts.Request.ContainerId).Str("checkpoint_id", opts.CheckpointId).Msgf("failed to create checkpoint state: %v", err)
		}

		opts.OutputLogger.Error("Failed to create checkpoint")
		return err
	}

	err = s.createSyncFile(opts.Request, opts.CheckpointId, checkpointPath)
	if err != nil {
		log.Error().Str("container_id", opts.Request.ContainerId).Str("checkpoint_id", opts.CheckpointId).Msgf("failed to create sync file: %v", err)
		return err
	}

	if opts.WaitForSignal {
		// Create a file accessible to the container to indicate that the checkpoint has been captured
		_, err = os.Create(filepath.Join(checkpointSignalDir(opts.Request.ContainerId), checkpointCompleteFileName))
		if err != nil {
			log.Error().Str("container_id", opts.Request.ContainerId).Str("checkpoint_id", opts.CheckpointId).Msgf("failed to create checkpoint complete file: %v", err)
			return err
		}
	}

	err = s.createCheckpointState(opts.CheckpointId, opts.Request, types.CheckpointStatusAvailable, opts.ContainerIp, []uint32{})
	if err != nil {
		log.Error().Str("container_id", opts.Request.ContainerId).Str("checkpoint_id", opts.CheckpointId).Msgf("failed to update checkpoint state: %v", err)
		return err
	}

	opts.OutputLogger.Info("Checkpoint created successfully")

	return nil
}

type syncFile struct {
	Path string `json:"path"`
	Type string `json:"type"`
}

const (
	syncFileExtension    = "crsync" // TODO: this should be in config or consolidated into a centralized checkpoint manager
	syncFileTimeout      = 600 * time.Second
	syncFilePollInterval = 1 * time.Second
)

func (s *Worker) createSyncFile(request *types.ContainerRequest, checkpointId string, checkpointPath string) error {
	cacheType := "CPU"
	if request.Gpu != "" {
		cacheType = strings.ToUpper(request.Gpu)
	}

	syncFile := syncFile{
		Path: filepath.Base(checkpointPath),
		Type: cacheType,
	}
	syncFileBytes, err := json.Marshal(syncFile)
	if err != nil {
		log.Error().Str("container_id", request.ContainerId).Msgf("failed to marshal sync file: %v", err)
		return err
	}

	err = os.WriteFile(filepath.Join(s.config.Worker.CRIU.Storage.MountPath, fmt.Sprintf("%s.%s", checkpointId, syncFileExtension)), syncFileBytes, 0644)
	if err != nil {
		log.Error().Str("container_id", request.ContainerId).Msgf("failed to write sync file: %v", err)
		return err
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
func (s *Worker) shouldCreateCheckpoint(request *types.ContainerRequest) bool {
	if !s.IsCRIUAvailable(request.GpuCount) || !request.CheckpointEnabled {
		return false
	}

	if request.Checkpoint != nil {
		return false
	}

	return true
}

func (s *Worker) IsCRIUAvailable(gpuCount uint32) bool {
	if s.criuManager == nil {
		log.Warn().Msg("criu manager not initialized")
		return false
	}

	if !s.criuManager.Available() {
		log.Warn().Msg("criu manager not available")
		return false
	}

	poolName := os.Getenv("WORKER_POOL_NAME")
	if poolName == "" {
		log.Warn().Msg("pool name not set")
		return false
	}

	pool, ok := s.config.Worker.Pools[poolName]
	if !ok {
		log.Warn().Msg("pool not found")
		return false
	}

	return pool.CRIUEnabled
}

func (s *Worker) createCheckpointState(checkpointId string, request *types.ContainerRequest, status types.CheckpointStatus, containerIp string, exposedPorts []uint32) error {
	_, err := handleGRPCResponse(s.backendRepoClient.CreateCheckpoint(context.Background(), &pb.CreateCheckpointRequest{
		CheckpointId:      checkpointId,
		SourceContainerId: request.ContainerId,
		ContainerIp:       containerIp,
		Status:            string(status),
		RemoteKey:         checkpointId,
		StubId:            request.Stub.ExternalId,
		ExposedPorts:      request.Ports,
	}))

	return err
}

func (s *Worker) updateCheckpointState(checkpointId string, request *types.ContainerRequest, status types.CheckpointStatus, containerIp string, exposedPorts []uint32) error {
	_, err := handleGRPCResponse(s.backendRepoClient.UpdateCheckpoint(context.Background(), &pb.UpdateCheckpointRequest{
		CheckpointId: checkpointId,
		ContainerIp:  containerIp,
		Status:       string(status),
		ExposedPorts: exposedPorts,
	}))

	return err
}

func (s *Worker) waitForCheckpointSignal(ctx context.Context, request *types.ContainerRequest, outputLogger *slog.Logger) error {
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
			log.Warn().Str("container_id", request.ContainerId).Msg("checkpoint deadline exceeded or container exited")
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
				outputLogger.Info("Container ready for checkpoint")
				break waitForReady
			} else {
				sampledLogger.Info().Str("container_id", instance.Id).Msg("container not ready for checkpoint")
			}
		}
	}

	return nil
}
