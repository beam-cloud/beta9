package worker

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/runtime"
	types "github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	gpuCntEnvKey               = types.WorkerGPUCountEnv
	defaultCheckpointDeadline  = 10 * time.Minute
	readyLogRate               = 10
	checkpointFsDir            = "filesystem"
	checkpointArchiveExtension = ".tar"
	checkpointOriginPrefix     = "checkpoints"
)

var errCRIUManagerUnavailable = errors.New("checkpoint/restore unavailable: CRIU manager is not initialized")

type checkpointCacheMetadata struct {
	hash        string
	sizeBytes   int64
	originKey   string
	locality    string
	accelerator string
}

type RestoreOpts struct {
	request      *types.ContainerRequest
	checkpoint   *types.Checkpoint
	outputWriter io.Writer
	started      chan int
	configPath   string
}

type CRIUManager interface {
	Available() bool
	CreateCheckpoint(ctx context.Context, runtime runtime.Runtime, checkpointId string, request *types.ContainerRequest) (string, error)
	RestoreCheckpoint(ctx context.Context, runtime runtime.Runtime, opts *RestoreOpts) (int, error)
}

type restoreCheckpointResult struct {
	exitCode int
	err      error
}

// InitializeCRIUManager initializes a new CRIU manager that can be used to checkpoint and restore containers.
func InitializeCRIUManager(ctx context.Context, config types.CRIUConfig, checkpointRoot string) (CRIUManager, error) {
	var criuManager CRIUManager = nil
	var err error = nil
	if checkpointRoot == "" {
		return nil, fmt.Errorf("checkpoint root is required")
	}

	switch config.Mode {
	case types.CRIUConfigModeNvidia:
		criuManager, err = InitializeNvidiaCRIU(ctx, config, checkpointRoot)
	default:
		return nil, fmt.Errorf("unsupported CRIU mode: %s", config.Mode)
	}

	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(checkpointRoot, os.ModePerm); err != nil {
		return nil, err
	}

	return criuManager, nil
}

func (s *Worker) attemptAutoCheckpoint(ctx context.Context, request *types.ContainerRequest, outputLogger *slog.Logger, outputWriter io.Writer, startedChan chan int, checkpointPIDChan chan int) {
	checkpointId := uuid.New().String()

	// If checkpointing is enabled and there is no existing checkpoint, attempt to create a checkpoint
	if s.shouldCreateCheckpoint(request) {
		outputLogger.Info("Attempting to create container checkpoint...")

		containerIp := ""
		containerInstance, exists := s.containerInstances.Get(request.ContainerId)
		if exists {
			containerIp = containerInstance.ContainerIp
		}

		err := s.createCheckpoint(ctx, &CreateCheckpointOpts{
			Request:           request,
			CheckpointId:      checkpointId,
			OutputLogger:      outputLogger,
			CheckpointPIDChan: checkpointPIDChan,
			WaitForSignal:     true,
			ContainerIp:       containerIp,
		})
		if err != nil {
			log.Error().Str("container_id", request.ContainerId).Msgf("failed to create checkpoint: %v", err)
			return
		}
	}
}

func (s *Worker) attemptRestoreCheckpoint(ctx context.Context, request *types.ContainerRequest, outputLogger *slog.Logger, outputWriter io.Writer, startedChan chan int, checkpointPIDChan chan int) (exitCode int, restored bool, started bool, err error) {
	checkpoint := request.Checkpoint
	if checkpoint.Status != string(types.CheckpointStatusAvailable) {
		return -1, false, false, fmt.Errorf("checkpoint not available")
	}

	if err := s.requireCRIUManager(); err != nil {
		return -1, false, false, err
	}

	outputLogger.Info("Attempting to restore container from checkpoint...")
	signalDir := checkpointSignalDir(request.ContainerId)
	if err := os.MkdirAll(signalDir, 0755); err != nil {
		log.Error().Str("container_id", request.ContainerId).Str("checkpoint_id", checkpoint.CheckpointId).Msgf("failed to create checkpoint signal dir: %v", err)
		return -1, false, false, err
	}

	f, err := os.Create(filepath.Join(signalDir, checkpointCompleteFileName))
	if err != nil {
		log.Error().Str("container_id", request.ContainerId).Str("checkpoint_id", checkpoint.CheckpointId).Msgf("failed to create checkpoint complete file: %v", err)
		return -1, false, false, err
	}
	defer f.Close()

	instance, exists := s.containerInstances.Get(request.ContainerId)
	if !exists {
		return -1, false, false, fmt.Errorf("container instance not found")
	}

	restoreStarted := make(chan int, 1)
	restoreDone := make(chan restoreCheckpointResult, 1)
	go func() {
		exitCode, err := s.criuManager.RestoreCheckpoint(ctx, instance.Runtime, &RestoreOpts{
			request:      request,
			checkpoint:   checkpoint,
			outputWriter: outputWriter,
			started:      restoreStarted,
			configPath:   request.ConfigPath,
		})
		restoreDone <- restoreCheckpointResult{exitCode: exitCode, err: err}
	}()

	restoreStartedChan := (<-chan int)(restoreStarted)
	for restoreDone != nil {
		select {
		case pid := <-restoreStartedChan:
			started = true
			restoreStartedChan = nil
			if err := forwardRestoreStarted(ctx, startedChan, pid); err != nil {
				return -1, false, started, err
			}
		case result := <-restoreDone:
			exitCode, err = result.exitCode, result.err
			restoreDone = nil
			if !started {
				if pid, ok := restoreStartedPID(restoreStarted); ok {
					started = true
					if err == nil {
						if forwardErr := forwardRestoreStarted(ctx, startedChan, pid); forwardErr != nil {
							return -1, false, started, forwardErr
						}
					}
				}
			}
		case <-ctx.Done():
			return -1, false, started, ctx.Err()
		}
	}

	if err != nil {
		log.Error().Str("container_id", request.ContainerId).Str("checkpoint_id", checkpoint.CheckpointId).Msgf("failed to restore checkpoint: %v", err)

		outputLogger.Info("Failed to restore checkpoint")
		if cleanupErr := deleteFailedRestoreRuntimeContainer(ctx, instance.Runtime, request.ContainerId); cleanupErr != nil {
			log.Warn().
				Err(cleanupErr).
				Str("container_id", request.ContainerId).
				Str("checkpoint_id", checkpoint.CheckpointId).
				Msg("failed to clean up runtime container after checkpoint restore failure")
		}
		s.markCheckpointRestoreFailed(request, checkpoint)

		return exitCode, false, started, err
	}

	if !started {
		err := fmt.Errorf("checkpoint restore completed without runtime start")
		log.Error().Str("container_id", request.ContainerId).Str("checkpoint_id", checkpoint.CheckpointId).Msg(err.Error())
		s.markCheckpointRestoreFailed(request, checkpoint)
		return -1, false, false, err
	}

	return exitCode, true, started, nil
}

func forwardRestoreStarted(ctx context.Context, startedChan chan int, pid int) error {
	if startedChan == nil {
		return nil
	}

	select {
	case startedChan <- pid:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func restoreStartedPID(started <-chan int) (int, bool) {
	select {
	case pid := <-started:
		return pid, true
	default:
		return 0, false
	}
}

func deleteFailedRestoreRuntimeContainer(ctx context.Context, rt runtime.Runtime, containerId string) error {
	if rt == nil || containerId == "" {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), runtimeDeleteTimeout)
	defer cancel()

	err := rt.Delete(cleanupCtx, containerId, &runtime.DeleteOpts{Force: true})
	if err != nil && !runtimeContainerNotFound(err) {
		return err
	}
	return nil
}

func (s *Worker) prepareRestoreFallback(request *types.ContainerRequest, config []byte) error {
	if request == nil || request.ConfigPath == "" || len(config) == 0 {
		return nil
	}

	instance, exists := s.containerInstances.Get(request.ContainerId)
	if exists && instance.Overlay != nil {
		upperDir := filepath.Join(filepath.Dir(instance.Overlay.TopLayerPath()), "upper")
		entries, err := os.ReadDir(upperDir)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		for _, entry := range entries {
			if err := os.RemoveAll(filepath.Join(upperDir, entry.Name())); err != nil {
				return err
			}
		}
		for _, dir := range []string{"workspace", "volumes"} {
			if err := os.MkdirAll(filepath.Join(upperDir, dir), 0755); err != nil {
				return err
			}
		}
	}

	return os.WriteFile(request.ConfigPath, config, 0644)
}

func runtimeContainerNotFound(err error) bool {
	if err == nil {
		return false
	}

	var notFoundValue runtime.ErrContainerNotFound
	if errors.As(err, &notFoundValue) {
		return true
	}

	var notFound *runtime.ErrContainerNotFound
	if errors.As(err, &notFound) {
		return true
	}

	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "does not exist") ||
		strings.Contains(msg, "not found") ||
		strings.Contains(msg, "no such container")
}

func (s *Worker) markCheckpointRestoreFailed(request *types.ContainerRequest, checkpoint *types.Checkpoint) {
	if request == nil || checkpoint == nil || checkpoint.CheckpointId == "" {
		return
	}

	if err := s.updateCheckpointState(checkpoint.CheckpointId, request, types.CheckpointStatusRestoreFailed); err != nil {
		log.Error().
			Err(err).
			Str("container_id", request.ContainerId).
			Str("checkpoint_id", checkpoint.CheckpointId).
			Msg("failed to update checkpoint state")
	}
}

func (s *Worker) signalRestoredSandboxProcessManager(ctx context.Context, request *types.ContainerRequest, rt runtime.Runtime) {
	if request.Stub.Type.Kind() != types.StubTypeSandbox || rt == nil {
		return
	}

	signalCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if err := rt.Kill(signalCtx, request.ContainerId, syscall.SIGWINCH, &runtime.KillOpts{}); err != nil {
		log.Debug().
			Err(err).
			Str("container_id", request.ContainerId).
			Msg("failed to signal restored sandbox process manager")
	}
}

type CreateCheckpointOpts struct {
	Request           *types.ContainerRequest
	CheckpointId      string
	ContainerIp       string
	OutputLogger      *slog.Logger
	CheckpointPIDChan chan int
	WaitForSignal     bool
}

// Waits for the container to be ready to checkpoint at the desired point in execution, ie.
// after all processes within a container have reached a checkpointable state
func (s *Worker) createCheckpoint(ctx context.Context, opts *CreateCheckpointOpts) error {
	instance, exists := s.containerInstances.Get(opts.Request.ContainerId)
	if !exists {
		return fmt.Errorf("container instance not found")
	}

	if err := s.requireCRIUManager(); err != nil {
		return err
	}

	if opts.CheckpointPIDChan != nil {
		<-opts.CheckpointPIDChan
	}

	log.Info().Str("container_id", opts.Request.ContainerId).Str("checkpoint_id", opts.CheckpointId).Msg("creating checkpoint")

	if opts.WaitForSignal {
		err := s.waitForCheckpointSignal(ctx, opts.Request, opts.OutputLogger)
		if err != nil {
			log.Error().Str("container_id", opts.Request.ContainerId).Str("checkpoint_id", opts.CheckpointId).Msgf("failed to wait for checkpoint signal: %v", err)
			return err
		}
	}

	// Proceed to create the checkpoint
	checkpointPath, err := s.criuManager.CreateCheckpoint(ctx, instance.Runtime, opts.CheckpointId, opts.Request)
	if err != nil {
		if stateErr := s.createCheckpointState(opts.CheckpointId, opts.Request, types.CheckpointStatusCheckpointFailed, opts.ContainerIp); stateErr != nil {
			log.Error().Str("container_id", opts.Request.ContainerId).Str("checkpoint_id", opts.CheckpointId).Msgf("failed to create checkpoint state: %v", stateErr)
		}

		if opts.OutputLogger != nil {
			opts.OutputLogger.Error("Failed to create checkpoint")
		}

		return err
	}

	parentDir := filepath.Dir(instance.Overlay.TopLayerPath())
	upperDir := path.Join(parentDir, "upper")

	err = copyDirectory(upperDir, path.Join(checkpointPath, checkpointFsDir), []string{"config.json", "outputs", "snapshot"})
	if err != nil {
		log.Error().Str("container_id", opts.Request.ContainerId).Str("checkpoint_id", opts.CheckpointId).Msgf("failed to copy upper directory: %v", err)
		return err
	}
	if !checkpointMaterialized(checkpointPath) {
		return fmt.Errorf("checkpoint missing runtime or filesystem payload")
	}

	metadata, err := s.persistCheckpoint(ctx, opts.Request, opts.CheckpointId, checkpointPath)
	if err != nil {
		_ = s.createCheckpointState(opts.CheckpointId, opts.Request, types.CheckpointStatusCheckpointFailed, opts.ContainerIp)
		log.Error().Str("container_id", opts.Request.ContainerId).Str("checkpoint_id", opts.CheckpointId).Msgf("failed to persist checkpoint: %v", err)
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

	err = s.createAvailableCheckpointState(opts.CheckpointId, opts.Request, opts.ContainerIp, metadata)
	if err != nil {
		log.Error().Str("container_id", opts.Request.ContainerId).Str("checkpoint_id", opts.CheckpointId).Msgf("failed to update checkpoint state: %v", err)
		return err
	}
	s.reportCheckpointRequiredContent(opts.Request, opts.CheckpointId, metadata)

	if opts.OutputLogger != nil {
		opts.OutputLogger.Info("Checkpoint created successfully")
	}

	log.Info().Str("container_id", opts.Request.ContainerId).Str("checkpoint_id", opts.CheckpointId).Msg("checkpoint created successfully")
	return nil
}

func (s *Worker) checkpointPath(checkpointId string) string {
	if s.cacheManager == nil {
		return ""
	}
	return filepath.Join(s.cacheManager.CheckpointRoot(), checkpointId)
}

func (s *Worker) checkpointArchivePath(checkpointId string) string {
	if s.cacheManager == nil {
		return ""
	}
	return filepath.Join(s.cacheManager.CheckpointRoot(), checkpointId+checkpointArchiveExtension)
}

func checkpointOriginKey(checkpointId string) string {
	return path.Join(checkpointOriginPrefix, checkpointId+checkpointArchiveExtension)
}

func checkpointAccelerator(request *types.ContainerRequest) string {
	if request != nil && request.Gpu != "" {
		return strings.ToUpper(request.Gpu)
	}
	return "CPU"
}

func (s *Worker) persistCheckpoint(ctx context.Context, request *types.ContainerRequest, checkpointId, checkpointPath string) (*checkpointCacheMetadata, error) {
	if s.cacheManager == nil || s.cacheManager.client == nil {
		return nil, fmt.Errorf("cache is required for checkpoint persistence")
	}
	if request == nil || !request.StorageAvailable() {
		return nil, fmt.Errorf("workspace storage is required for checkpoint persistence")
	}

	archivePath := s.checkpointArchivePath(checkpointId)
	if archivePath == "" {
		return nil, fmt.Errorf("checkpoint archive path is unavailable")
	}
	_ = os.Remove(archivePath)
	defer os.Remove(archivePath)

	if err := createTar(checkpointPath, archivePath); err != nil {
		return nil, err
	}
	hash, size, err := fileSHA256(archivePath)
	if err != nil {
		return nil, err
	}

	originKey := checkpointOriginKey(checkpointId)
	storageClient, err := clients.NewWorkspaceStorageClient(ctx, request.Workspace.Name, request.Workspace.Storage)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(archivePath)
	if err != nil {
		return nil, err
	}
	if err := storageClient.UploadWithReader(ctx, originKey, f); err != nil {
		_ = f.Close()
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}

	if _, err := s.cacheManager.client.StoreContentFromLocalFile(cache.LocalContentSource{
		Path:      archivePath,
		CachePath: originKey,
	}, cache.StoreContentOptions{RoutingKey: hash, Lock: true}); err != nil {
		log.Warn().Err(err).Str("checkpoint_id", checkpointId).Str("hash", hash).Msg("failed to store checkpoint archive in cache")
	}

	return &checkpointCacheMetadata{
		hash:        hash,
		sizeBytes:   size,
		originKey:   originKey,
		locality:    s.cacheManager.locality,
		accelerator: checkpointAccelerator(request),
	}, nil
}

func (s *Worker) ensureCheckpointMaterialized(ctx context.Context, request *types.ContainerRequest, checkpoint *types.Checkpoint) (string, error) {
	if checkpoint == nil {
		return "", fmt.Errorf("checkpoint is required")
	}

	checkpointPath := s.checkpointPath(checkpoint.CheckpointId)
	if checkpointPath == "" {
		return "", fmt.Errorf("checkpoint path is unavailable")
	}
	metadataComplete := checkpoint.CacheHash != "" && checkpoint.CacheSizeBytes > 0 && checkpoint.OriginKey != ""
	if metadataComplete {
		s.reportCheckpointRequiredContent(request, checkpoint.CheckpointId, checkpointCacheMetadataFromRecord(request, checkpoint))
	}
	if checkpointMaterialized(checkpointPath) {
		return checkpointPath, nil
	}
	if !metadataComplete {
		return "", fmt.Errorf("checkpoint cache metadata is incomplete")
	}
	s.cacheManager.requestReconcile()

	archivePath := s.checkpointArchivePath(checkpoint.CheckpointId)
	if archivePath == "" {
		return "", fmt.Errorf("checkpoint archive path is unavailable")
	}
	if err := s.writeCheckpointArchiveFromCache(ctx, archivePath, checkpoint); err != nil {
		if err := s.downloadCheckpointArchive(ctx, request, archivePath, checkpoint); err != nil {
			return "", err
		}
	}
	defer os.Remove(archivePath)

	if err := materializeCheckpointArchive(archivePath, checkpointPath, checkpoint.CheckpointId); err != nil {
		return "", err
	}
	return checkpointPath, nil
}

func materializeCheckpointArchive(archivePath, checkpointPath, checkpointID string) error {
	tmpRoot := filepath.Join(filepath.Dir(checkpointPath), "."+checkpointID+".extract")
	_ = os.RemoveAll(tmpRoot)
	if err := os.MkdirAll(tmpRoot, 0755); err != nil {
		return err
	}
	defer os.RemoveAll(tmpRoot)

	if err := untarTar(archivePath, tmpRoot); err != nil {
		return err
	}
	extractedPath := filepath.Join(tmpRoot, checkpointID)
	if !checkpointMaterialized(extractedPath) {
		return fmt.Errorf("checkpoint archive missing runtime or filesystem payload")
	}
	_ = os.RemoveAll(checkpointPath)
	if err := os.Rename(extractedPath, checkpointPath); err != nil {
		return err
	}
	return nil
}

func checkpointMaterialized(checkpointPath string) bool {
	info, err := os.Stat(filepath.Join(checkpointPath, checkpointFsDir))
	if err != nil || !info.IsDir() {
		return false
	}

	return checkpointHasRuntimePayload(checkpointPath)
}

func checkpointHasRuntimePayload(checkpointPath string) bool {
	entries, err := os.ReadDir(checkpointPath)
	if err != nil {
		return false
	}

	for _, entry := range entries {
		if entry.Name() == checkpointFsDir {
			continue
		}

		entryPath := filepath.Join(checkpointPath, entry.Name())
		if entry.Type().IsRegular() {
			return true
		}
		if entry.IsDir() && checkpointDirHasRegularFile(entryPath) {
			return true
		}
	}

	return false
}

func checkpointDirHasRegularFile(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}

	for _, entry := range entries {
		entryPath := filepath.Join(dir, entry.Name())
		if entry.Type().IsRegular() {
			return true
		}
		if entry.IsDir() && checkpointDirHasRegularFile(entryPath) {
			return true
		}
	}

	return false
}

func (s *Worker) writeCheckpointArchiveFromCache(ctx context.Context, archivePath string, checkpoint *types.Checkpoint) error {
	if s.cacheManager == nil || s.cacheManager.client == nil {
		return fmt.Errorf("cache is unavailable")
	}
	return writeCacheContentFile(ctx, s.cacheManager.client, archivePath, checkpoint.CacheHash, checkpoint.CacheSizeBytes, checkpoint.CacheHash)
}

func writeCacheContentFile(ctx context.Context, client *cache.Client, filePath, hash string, size int64, routingKey string) error {
	if client == nil {
		return fmt.Errorf("cache is unavailable")
	}
	return writeCacheContentFileWithReader(ctx, filePath, hash, size, func(ctx context.Context, hash string, offset int64, dst []byte) (int64, error) {
		return client.ReadContentInto(ctx, hash, offset, dst, cache.ClientOptions{RoutingKey: routingKey})
	})
}

func writeLocalCacheContentFile(ctx context.Context, server *cache.Server, filePath, hash string, size int64) error {
	if server == nil {
		return fmt.Errorf("cache server is unavailable")
	}
	return writeCacheContentFileWithReader(ctx, filePath, hash, size, server.ReadContentInto)
}

func writeCacheContentFileWithReader(ctx context.Context, filePath, hash string, size int64, read func(context.Context, string, int64, []byte) (int64, error)) error {
	tmpPath := filePath + ".tmp"
	_ = os.Remove(tmpPath)

	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return err
	}

	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	hasher := sha256.New()
	buf := make([]byte, 4*1024*1024)
	for offset := int64(0); offset < size; {
		length := min(int64(len(buf)), size-offset)
		n, err := read(ctx, hash, offset, buf[:length])
		if err != nil {
			_ = f.Close()
			_ = os.Remove(tmpPath)
			return err
		}
		if n != length {
			_ = f.Close()
			_ = os.Remove(tmpPath)
			return fmt.Errorf("short checkpoint cache read: expected %d bytes, got %d", length, n)
		}
		if _, err := f.Write(buf[:n]); err != nil {
			_ = f.Close()
			_ = os.Remove(tmpPath)
			return err
		}
		if _, err := hasher.Write(buf[:n]); err != nil {
			_ = f.Close()
			_ = os.Remove(tmpPath)
			return err
		}
		offset += n
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if actual := hex.EncodeToString(hasher.Sum(nil)); actual != hash {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("cache hash mismatch: expected %s, got %s", hash, actual)
	}
	return os.Rename(tmpPath, filePath)
}

func (s *Worker) downloadCheckpointArchive(ctx context.Context, request *types.ContainerRequest, archivePath string, checkpoint *types.Checkpoint) error {
	if request == nil || !request.StorageAvailable() {
		return fmt.Errorf("workspace storage is required for checkpoint restore")
	}
	storageClient, err := clients.NewWorkspaceStorageClient(ctx, request.Workspace.Name, request.Workspace.Storage)
	if err != nil {
		return err
	}
	reader, err := storageClient.DownloadWithReader(ctx, checkpoint.OriginKey)
	if err != nil {
		return err
	}
	defer reader.Close()

	tmpPath := archivePath + ".tmp"
	_ = os.Remove(tmpPath)
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	hasher := sha256.New()
	size, err := io.Copy(f, io.TeeReader(reader, hasher))
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if size != checkpoint.CacheSizeBytes {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("checkpoint origin size mismatch: expected %d, got %d", checkpoint.CacheSizeBytes, size)
	}
	if actual := hex.EncodeToString(hasher.Sum(nil)); actual != checkpoint.CacheHash {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("checkpoint origin hash mismatch: expected %s, got %s", checkpoint.CacheHash, actual)
	}
	if err := os.Rename(tmpPath, archivePath); err != nil {
		return err
	}
	if s.cacheManager != nil && s.cacheManager.client != nil {
		if _, err := s.cacheManager.client.StoreContentFromLocalFile(cache.LocalContentSource{
			Path:      archivePath,
			CachePath: checkpoint.OriginKey,
		}, cache.StoreContentOptions{RoutingKey: checkpoint.CacheHash, Lock: true}); err != nil {
			log.Warn().Err(err).Str("checkpoint_id", checkpoint.CheckpointId).Msg("failed to cache restored checkpoint archive")
		}
	}
	return nil
}

func fileSHA256(filePath string) (string, int64, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()

	hasher := sha256.New()
	size, err := io.Copy(hasher, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(hasher.Sum(nil)), size, nil
}

func checkpointCacheMetadataFromRecord(request *types.ContainerRequest, checkpoint *types.Checkpoint) *checkpointCacheMetadata {
	accelerator := checkpoint.Accelerator
	if accelerator == "" {
		accelerator = checkpointAccelerator(request)
	}
	return &checkpointCacheMetadata{
		hash:        checkpoint.CacheHash,
		sizeBytes:   checkpoint.CacheSizeBytes,
		originKey:   checkpoint.OriginKey,
		locality:    checkpoint.Locality,
		accelerator: accelerator,
	}
}

func (s *Worker) reportCheckpointRequiredContent(request *types.ContainerRequest, checkpointId string, metadata *checkpointCacheMetadata) {
	if s.cacheManager == nil || metadata == nil {
		return
	}
	reporter := s.cacheManager.ContentReporter()
	if reporter == nil {
		return
	}
	reporter.reportItems(cacheRequestWorkspaceID(request), cacheRequestStubID(request), types.CacheContentKindCheckpoint, []types.CacheRequiredContentItem{{
		Hash:         metadata.hash,
		RoutingKey:   metadata.hash,
		SizeBytes:    metadata.sizeBytes,
		ExpectedHash: metadata.hash,
		Source:       metadata.originKey,
		Kind:         types.CacheContentKindCheckpoint,
		CheckpointID: checkpointId,
		Accelerator:  metadata.accelerator,
	}})
	reporter.flush()
}

// shouldCreateCheckpoint checks if a checkpoint should be created for a given container
// NOTE: this currently only works for deployments since functions can run multiple containers
func (s *Worker) shouldCreateCheckpoint(request *types.ContainerRequest) bool {
	if !s.IsCRIUAvailable(request.GpuCount) || !request.CheckpointEnabled {
		return false
	}

	if request.Checkpoint != nil && request.Checkpoint.Status == string(types.CheckpointStatusAvailable) {
		return false
	}

	return true
}

func (s *Worker) IsCRIUAvailable(gpuCount uint32) bool {
	if err := s.requireCRIUManager(); err != nil {
		log.Warn().Err(err).Msg("C/R unavailable")
		return false
	}

	poolName := os.Getenv(types.WorkerPoolEnv)
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

func (s *Worker) requireCRIUManager() error {
	if s.criuManager == nil || !s.criuManager.Available() {
		return errCRIUManagerUnavailable
	}
	return nil
}

func (s *Worker) createCheckpointState(checkpointId string, request *types.ContainerRequest, status types.CheckpointStatus, containerIp string) error {
	req := checkpointStateRequest(checkpointId, request, status, containerIp)
	_, err := handleGRPCResponse(s.backendRepoClient.CreateCheckpoint(context.Background(), req))

	return err
}

func (s *Worker) createAvailableCheckpointState(checkpointId string, request *types.ContainerRequest, containerIp string, metadata *checkpointCacheMetadata) error {
	req := checkpointStateRequest(checkpointId, request, types.CheckpointStatusAvailable, containerIp)
	req.CacheHash = metadata.hash
	req.CacheSizeBytes = metadata.sizeBytes
	req.OriginKey = metadata.originKey
	req.Locality = metadata.locality
	req.Accelerator = metadata.accelerator
	_, err := handleGRPCResponse(s.backendRepoClient.CreateCheckpoint(context.Background(), req))

	return err
}

func checkpointStateRequest(checkpointId string, request *types.ContainerRequest, status types.CheckpointStatus, containerIp string) *pb.CreateCheckpointRequest {
	return &pb.CreateCheckpointRequest{
		CheckpointId:      checkpointId,
		SourceContainerId: request.ContainerId,
		ContainerIp:       containerIp,
		Status:            string(status),
		RemoteKey:         checkpointId,
		StubId:            request.Stub.ExternalId,
		ExposedPorts:      request.Ports,
	}
}

func (s *Worker) updateCheckpointState(checkpointId string, request *types.ContainerRequest, status types.CheckpointStatus) error {
	_, err := handleGRPCResponse(s.backendRepoClient.UpdateCheckpoint(context.Background(), &pb.UpdateCheckpointRequest{
		CheckpointId: checkpointId,
		Status:       string(status),
	}))

	return err
}

func (s *Worker) updateCheckpointRestored(checkpointId string) error {
	_, err := handleGRPCResponse(s.backendRepoClient.UpdateCheckpoint(context.Background(), &pb.UpdateCheckpointRequest{
		CheckpointId:   checkpointId,
		LastRestoredAt: timestamppb.Now(),
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
				sampledLogger.Info().Str("container_id", request.ContainerId).Msg("container instance not found yet")
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
