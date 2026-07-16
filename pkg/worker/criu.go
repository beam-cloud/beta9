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
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/runtime"
	types "github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	gpuCntEnvKey               = types.WorkerGPUCountEnv
	defaultCheckpointDeadline  = 10 * time.Minute
	defaultCheckpointCreateTTL = 10 * time.Minute
	readyLogRate               = 10
	checkpointFsDir            = "filesystem"
	checkpointArchiveExtension = ".tar"
	checkpointOriginPrefix     = "checkpoints"
	checkpointTriggerHTTP      = "http"
	checkpointProgressInterval = 10 * time.Second
)

var checkpointRuntimeEnvOverrides = []string{
	"UV_USE_IO_URING=0",
	"TORCHINDUCTOR_QUIESCE_ASYNC_COMPILE_POOL=1",
}

var checkpointServiceLoopbackEnvOverrides = []string{
	"MASTER_ADDR=127.0.0.1",
	"NCCL_SOCKET_IFNAME=lo",
	"GLOO_SOCKET_IFNAME=lo",
}

var checkpointDisabledIOUringSyscalls = []string{
	"io_uring_setup",
	"io_uring_enter",
	"io_uring_register",
}

var errCRIUManagerUnavailable = errors.New("checkpoint/restore unavailable: CRIU manager is not initialized")

func applyCheckpointRuntimeEnvironmentOverrides(env []string, request *types.ContainerRequest, processArgs []string) []string {
	if request == nil || !request.CheckpointEnabled {
		return env
	}
	env = upsertEnvVars(env, checkpointRuntimeEnvOverrides)
	if shouldUseLoopbackForPodServiceCheckpoint(request, processArgs, env) {
		env = upsertEnvVars(env, checkpointServiceLoopbackEnvOverrides)
	}
	return env
}

func shouldUseLoopbackForPodServiceCheckpoint(request *types.ContainerRequest, processArgs, env []string) bool {
	if request == nil || !request.RequiresGPU() {
		return false
	}
	if !isPodServiceRequest(request) {
		return false
	}
	return hasLoopbackSensitiveGPUBackend(processArgs, env)
}

func isPodRequest(request *types.ContainerRequest) bool {
	return request != nil && request.Stub.Type.Kind() == types.StubTypePod
}

func isPodServiceRequest(request *types.ContainerRequest) bool {
	if !isPodRequest(request) {
		return false
	}
	config, err := request.Stub.UnmarshalConfig()
	if err != nil || config == nil {
		return false
	}
	return config.IsService
}

func hasLoopbackSensitiveGPUBackend(processArgs, env []string) bool {
	for _, value := range append(append([]string{}, processArgs...), env...) {
		name, _, hasValue := strings.Cut(value, "=")
		if hasValue && strings.HasPrefix(strings.ToUpper(name), "VLLM_") {
			return true
		}
		lower := strings.ToLower(value)
		if lower == "vllm" ||
			strings.Contains(lower, "/vllm") ||
			strings.Contains(lower, "vllm.") ||
			strings.Contains(lower, "vllm_") ||
			strings.Contains(lower, "vllm-") {
			return true
		}
	}
	return false
}

func disableIOUringForCheckpoint(spec *specs.Spec) {
	if spec == nil {
		return
	}
	if spec.Linux == nil {
		spec.Linux = &specs.Linux{}
	}
	if spec.Linux.Seccomp == nil {
		spec.Linux.Seccomp = &specs.LinuxSeccomp{DefaultAction: specs.ActAllow}
	}
	if spec.Linux.Seccomp.DefaultAction == "" {
		spec.Linux.Seccomp.DefaultAction = specs.ActAllow
	}

	blocked := make(map[string]struct{}, len(checkpointDisabledIOUringSyscalls))
	for _, name := range checkpointDisabledIOUringSyscalls {
		blocked[name] = struct{}{}
	}

	syscalls := spec.Linux.Seccomp.Syscalls[:0]
	for _, syscallRule := range spec.Linux.Seccomp.Syscalls {
		names := syscallRule.Names[:0]
		for _, name := range syscallRule.Names {
			if _, ok := blocked[name]; !ok {
				names = append(names, name)
			}
		}
		if len(names) == 0 {
			continue
		}
		syscallRule.Names = names
		syscalls = append(syscalls, syscallRule)
	}

	errno := uint(syscall.ENOSYS)
	spec.Linux.Seccomp.Syscalls = append([]specs.LinuxSyscall{{
		Names:    append([]string(nil), checkpointDisabledIOUringSyscalls...),
		Action:   specs.ActErrno,
		ErrnoRet: &errno,
	}}, syscalls...)
}

type checkpointCacheMetadata struct {
	hash        string
	sizeBytes   int64
	originKey   string
	locality    string
	accelerator string
}

type checkpointPersistenceProgress struct {
	mu           sync.Mutex
	outputLogger *slog.Logger
	phase        string
	total        int64
	started      time.Time
	lastLog      time.Time
}

func newCheckpointPersistenceProgress(outputLogger *slog.Logger, phase string, total int64) *checkpointPersistenceProgress {
	now := time.Now()
	return &checkpointPersistenceProgress{
		outputLogger: outputLogger,
		phase:        phase,
		total:        total,
		started:      now,
		lastLog:      now,
	}
}

func (p *checkpointPersistenceProgress) update(completed int64) {
	p.report(completed, false)
}

func (p *checkpointPersistenceProgress) finish(completed int64) {
	p.report(completed, true)
}

func (p *checkpointPersistenceProgress) report(completed int64, complete bool) {
	if p == nil || p.outputLogger == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(p.started)
	if !complete && now.Sub(p.lastLog) < checkpointProgressInterval {
		return
	}
	if p.total > 0 && completed > p.total {
		completed = p.total
	}
	elapsed = elapsed.Round(100 * time.Millisecond)

	if complete {
		rate := int64(0)
		if elapsed > 0 {
			rate = int64(float64(completed) / elapsed.Seconds())
		}
		p.outputLogger.Info(fmt.Sprintf("Checkpoint %s complete: %s in %s (%s/s)\n", p.phase, formatImageBytes(completed), elapsed, formatImageBytes(rate)))
	} else if p.total > 0 {
		p.outputLogger.Info(fmt.Sprintf("Checkpoint %s: %s / %s (%d%%, %s elapsed)\n", p.phase, formatImageBytes(completed), formatImageBytes(p.total), min(int64(100), completed*100/p.total), elapsed))
	} else {
		p.outputLogger.Info(fmt.Sprintf("Checkpoint %s: %s (%s elapsed)\n", p.phase, formatImageBytes(completed), elapsed))
	}
	p.lastLog = now
}

type checkpointUploadReader struct {
	file        *os.File
	total       int64
	transferred atomic.Int64
	progress    func(int64)
}

var _ interface {
	io.Reader
	io.ReaderAt
	io.Seeker
} = (*checkpointUploadReader)(nil)

func (r *checkpointUploadReader) Read(p []byte) (int, error) {
	n, err := r.file.Read(p)
	r.report(n)
	return n, err
}

func (r *checkpointUploadReader) ReadAt(p []byte, offset int64) (int, error) {
	n, err := r.file.ReadAt(p, offset)
	r.report(n)
	return n, err
}

func (r *checkpointUploadReader) Seek(offset int64, whence int) (int64, error) {
	return r.file.Seek(offset, whence)
}

func (r *checkpointUploadReader) report(n int) {
	if n <= 0 || r.progress == nil {
		return
	}
	completed := r.transferred.Add(int64(n))
	if r.total > 0 && completed > r.total {
		completed = r.total
	}
	r.progress(completed)
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
		if !s.acquireCheckpointCreateLock(request) {
			log.Info().Str("container_id", request.ContainerId).Str("stub_id", request.StubId).Msg("checkpoint creation already in progress")
			return
		}
		defer s.releaseCheckpointCreateLock(request)

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

	if err := writeCheckpointCompleteMarker(request.ContainerId); err != nil {
		log.Error().Str("container_id", request.ContainerId).Str("checkpoint_id", checkpoint.CheckpointId).Msgf("failed to create checkpoint complete file: %v", err)
		return -1, false, false, err
	}

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
func (s *Worker) createCheckpoint(ctx context.Context, opts *CreateCheckpointOpts) (err error) {
	if opts == nil || opts.Request == nil {
		return errors.New("checkpoint request is required")
	}

	availableStateCreated := false
	var persistedMetadata *checkpointCacheMetadata
	defer func() {
		if err != nil && !availableStateCreated {
			s.markCheckpointFailed(opts, persistedMetadata)
		}
	}()

	instance, exists := s.containerInstances.Get(opts.Request.ContainerId)
	if !exists || instance == nil {
		return fmt.Errorf("container instance not found")
	}
	if instance.Runtime == nil {
		return fmt.Errorf("container runtime is unavailable")
	}

	if err := s.requireCRIUManager(); err != nil {
		return err
	}

	if opts.CheckpointPIDChan != nil {
		if opts.OutputLogger != nil {
			opts.OutputLogger.Info("Waiting for container runtime to start before checkpoint")
		}
		failBeforeRuntimeStart := func(err error) error {
			log.Error().Str("container_id", opts.Request.ContainerId).Str("checkpoint_id", opts.CheckpointId).Err(err).Msg("checkpoint failed before container runtime started")
			if opts.OutputLogger != nil {
				opts.OutputLogger.Error(fmt.Sprintf("Failed to start checkpoint: %v", err))
			}
			return err
		}
		select {
		case pid, ok := <-opts.CheckpointPIDChan:
			if !ok {
				return failBeforeRuntimeStart(fmt.Errorf("container runtime exited before checkpoint could start"))
			}
			if pid <= 0 {
				return failBeforeRuntimeStart(fmt.Errorf("container runtime reported invalid PID %d before checkpoint", pid))
			}
			log.Info().Str("container_id", opts.Request.ContainerId).Str("checkpoint_id", opts.CheckpointId).Int("pid", pid).Msg("container runtime started for checkpoint")
		case <-ctx.Done():
			return failBeforeRuntimeStart(ctx.Err())
		}
	}

	log.Info().Str("container_id", opts.Request.ContainerId).Str("checkpoint_id", opts.CheckpointId).Msg("creating checkpoint")

	if opts.WaitForSignal {
		err := s.waitForCheckpointTrigger(ctx, opts.Request, opts.OutputLogger)
		if err != nil {
			log.Error().Str("container_id", opts.Request.ContainerId).Str("checkpoint_id", opts.CheckpointId).Msgf("failed to wait for checkpoint signal: %v", err)
			if opts.OutputLogger != nil {
				opts.OutputLogger.Error(fmt.Sprintf("Failed to wait for checkpoint readiness: %v", err))
			}
			return err
		}
	}
	if instance.Overlay == nil {
		return fmt.Errorf("container overlay is unavailable")
	}

	// Proceed to create the checkpoint
	if opts.OutputLogger != nil {
		opts.OutputLogger.Info("Creating container checkpoint snapshot")
	}
	checkpointCtx, checkpointCancel := context.WithTimeout(ctx, defaultCheckpointCreateTTL)
	defer checkpointCancel()
	checkpointPath, err := s.criuManager.CreateCheckpoint(checkpointCtx, instance.Runtime, opts.CheckpointId, opts.Request)
	if err != nil {
		if errors.Is(checkpointCtx.Err(), context.DeadlineExceeded) {
			err = fmt.Errorf("checkpoint snapshot timed out after %s: %w", defaultCheckpointCreateTTL, err)
		}
		if opts.OutputLogger != nil {
			opts.OutputLogger.Error(fmt.Sprintf("Failed to create checkpoint: %v", err))
		}

		return err
	}
	parentDir := filepath.Dir(instance.Overlay.TopLayerPath())
	upperDir := path.Join(parentDir, "upper")

	err = copyDirectory(upperDir, path.Join(checkpointPath, checkpointFsDir), []string{"config.json", "outputs", "snapshot"})
	if err != nil {
		log.Error().Str("container_id", opts.Request.ContainerId).Str("checkpoint_id", opts.CheckpointId).Msgf("failed to copy upper directory: %v", err)
		if opts.OutputLogger != nil {
			opts.OutputLogger.Error(fmt.Sprintf("Failed to copy checkpoint filesystem state: %v", err))
		}
		return err
	}
	if !checkpointMaterialized(checkpointPath) {
		return fmt.Errorf("checkpoint missing runtime or filesystem payload")
	}

	if opts.OutputLogger != nil {
		opts.OutputLogger.Info("Persisting container checkpoint")
	}
	metadata, err := s.persistCheckpoint(ctx, opts.Request, opts.CheckpointId, checkpointPath, opts.OutputLogger)
	if err != nil {
		log.Error().Str("container_id", opts.Request.ContainerId).Str("checkpoint_id", opts.CheckpointId).Msgf("failed to persist checkpoint: %v", err)
		if opts.OutputLogger != nil {
			opts.OutputLogger.Error(fmt.Sprintf("Failed to persist checkpoint: %v", err))
		}
		return err
	}
	persistedMetadata = metadata

	if opts.WaitForSignal {
		// Create a file accessible to the container to indicate that the checkpoint has been captured
		if err = writeCheckpointCompleteMarker(opts.Request.ContainerId); err != nil {
			log.Error().Str("container_id", opts.Request.ContainerId).Str("checkpoint_id", opts.CheckpointId).Msgf("failed to create checkpoint complete file: %v", err)
			return err
		}
	}

	err = s.createCheckpointState(opts.CheckpointId, opts.Request, types.CheckpointStatusAvailable, opts.ContainerIp, metadata)
	if err != nil {
		log.Error().Str("container_id", opts.Request.ContainerId).Str("checkpoint_id", opts.CheckpointId).Msgf("failed to update checkpoint state: %v", err)
		return err
	}
	availableStateCreated = true
	s.reportCheckpointRequiredContent(opts.Request, opts.CheckpointId, metadata)

	if opts.OutputLogger != nil {
		opts.OutputLogger.Info("Checkpoint created successfully")
	} else {
		log.Info().Str("container_id", opts.Request.ContainerId).Str("checkpoint_id", opts.CheckpointId).Msg("checkpoint created successfully")
	}
	return nil
}

func (s *Worker) markCheckpointFailed(opts *CreateCheckpointOpts, metadata *checkpointCacheMetadata) {
	if s == nil || s.backendRepoClient == nil || opts == nil || opts.Request == nil {
		return
	}
	if stateErr := s.createCheckpointState(opts.CheckpointId, opts.Request, types.CheckpointStatusCheckpointFailed, opts.ContainerIp, metadata); stateErr != nil {
		log.Error().
			Str("container_id", opts.Request.ContainerId).
			Str("checkpoint_id", opts.CheckpointId).
			Msgf("failed to create checkpoint state: %v", stateErr)
	}
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

func (s *Worker) persistCheckpoint(ctx context.Context, request *types.ContainerRequest, checkpointId, checkpointPath string, outputLogger *slog.Logger) (*checkpointCacheMetadata, error) {
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

	if outputLogger != nil {
		outputLogger.Info("Creating checkpoint archive...\n")
	}
	archiveProgress := newCheckpointPersistenceProgress(outputLogger, "archive", 0)
	archiveStarted := time.Now()
	hash, size, err := createTarWithSHA256Progress(ctx, checkpointPath, archivePath, archiveProgress.update)
	if err != nil {
		_ = os.Remove(archivePath)
		return nil, err
	}
	archiveProgress.finish(size)
	log.Info().Str("checkpoint_id", checkpointId).Int64("bytes", size).Dur("duration", time.Since(archiveStarted)).Msg("checkpoint archive created")

	originKey := checkpointOriginKey(checkpointId)
	storageClient, err := clients.NewWorkspaceStorageClient(ctx, request.Workspace.Name, request.Workspace.Storage)
	if err != nil {
		_ = os.Remove(archivePath)
		return nil, err
	}
	f, err := os.Open(archivePath)
	if err != nil {
		_ = os.Remove(archivePath)
		return nil, err
	}
	if outputLogger != nil {
		outputLogger.Info("Uploading checkpoint archive...\n")
	}
	uploadProgress := newCheckpointPersistenceProgress(outputLogger, "upload", size)
	uploadStarted := time.Now()
	uploadReader := &checkpointUploadReader{file: f, total: size, progress: uploadProgress.update}
	if err := storageClient.UploadWithReader(ctx, originKey, uploadReader); err != nil {
		_ = f.Close()
		_ = os.Remove(archivePath)
		return nil, err
	}
	uploadProgress.finish(size)
	log.Info().Str("checkpoint_id", checkpointId).Int64("bytes", size).Dur("duration", time.Since(uploadStarted)).Msg("checkpoint archive uploaded")
	if err := f.Close(); err != nil {
		log.Warn().Err(err).Str("checkpoint_id", checkpointId).Msg("failed to close uploaded checkpoint archive")
	}

	metadata := &checkpointCacheMetadata{
		hash:        hash,
		sizeBytes:   size,
		originKey:   originKey,
		locality:    s.cacheManager.locality,
		accelerator: checkpointAccelerator(request),
	}
	s.cacheCheckpointArchiveAsync(checkpointId, archivePath, originKey, hash)

	return metadata, nil
}

// cacheCheckpointArchiveAsync takes ownership of archivePath and removes it
// after the cache store completes or when no cache client is available.
func (s *Worker) cacheCheckpointArchiveAsync(checkpointId, archivePath, originKey, hash string) {
	if s.cacheManager == nil || s.cacheManager.client == nil {
		_ = os.Remove(archivePath)
		return
	}
	client := s.cacheManager.client

	go func() {
		defer os.Remove(archivePath)
		if _, err := client.StoreContentFromLocalFile(cache.LocalContentSource{
			Path:      archivePath,
			CachePath: originKey,
		}, cache.StoreContentOptions{RoutingKey: hash, Lock: true}); err != nil {
			log.Warn().Err(err).Str("checkpoint_id", checkpointId).Str("hash", hash).Msg("failed to store checkpoint archive in cache")
		}
	}()
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
	release, err := s.cacheManager.acquireCheckpointMaterialization(ctx, checkpoint.CheckpointId)
	if err != nil {
		return "", err
	}
	defer release()
	if checkpointMaterialized(checkpointPath) {
		return checkpointPath, nil
	}
	s.cacheManager.requestReconcile()

	archivePath := s.checkpointArchivePath(checkpoint.CheckpointId)
	if archivePath == "" {
		return "", fmt.Errorf("checkpoint archive path is unavailable")
	}
	downloadedFromOrigin := false
	if err := s.writeCheckpointArchiveFromCache(ctx, archivePath, checkpoint); err != nil {
		if err := s.downloadCheckpointArchive(ctx, request, archivePath, checkpoint); err != nil {
			return "", err
		}
		downloadedFromOrigin = true
	}
	removeArchive := true
	defer func() {
		if removeArchive {
			_ = os.Remove(archivePath)
		}
	}()

	if err := materializeCheckpointArchive(archivePath, checkpointPath, checkpoint.CheckpointId); err != nil {
		return "", err
	}
	if downloadedFromOrigin {
		removeArchive = false
		s.cacheCheckpointArchiveAsync(checkpoint.CheckpointId, archivePath, checkpoint.OriginKey, checkpoint.CacheHash)
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
	return nil
}

func writeCheckpointCompleteMarker(containerID string) error {
	return os.WriteFile(filepath.Join(checkpointSignalDir(containerID), checkpointCompleteFileName), nil, 0644)
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

func (s *Worker) acquireCheckpointCreateLock(request *types.ContainerRequest) bool {
	if request == nil {
		return false
	}
	key := fmt.Sprintf("%s:%s", request.WorkspaceId, request.StubId)
	_, loaded := s.checkpointCreateLocks.LoadOrStore(key, struct{}{})
	return !loaded
}

func (s *Worker) releaseCheckpointCreateLock(request *types.ContainerRequest) {
	if request == nil {
		return
	}
	key := fmt.Sprintf("%s:%s", request.WorkspaceId, request.StubId)
	s.checkpointCreateLocks.Delete(key)
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

func (s *Worker) createCheckpointState(checkpointId string, request *types.ContainerRequest, status types.CheckpointStatus, containerIp string, metadata *checkpointCacheMetadata) error {
	req := checkpointStateRequest(checkpointId, request, status, containerIp)
	if metadata != nil {
		req.CacheHash = metadata.hash
		req.CacheSizeBytes = metadata.sizeBytes
		req.OriginKey = metadata.originKey
		req.Locality = metadata.locality
		req.Accelerator = metadata.accelerator
	}
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

func (s *Worker) waitForCheckpointTrigger(ctx context.Context, request *types.ContainerRequest, outputLogger *slog.Logger) error {
	trigger := request.CheckpointTrigger
	if trigger != nil && strings.EqualFold(trigger.Type, checkpointTriggerHTTP) && trigger.HttpPath != "" {
		return s.waitForCheckpointHTTPReadiness(ctx, request, trigger, outputLogger)
	}

	return s.waitForCheckpointSignal(ctx, request, outputLogger)
}

func (s *Worker) waitForCheckpointHTTPReadiness(ctx context.Context, request *types.ContainerRequest, trigger *types.CheckpointTrigger, outputLogger *slog.Logger) error {
	timeout := time.Duration(trigger.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = defaultCheckpointDeadline
	}
	interval := time.Duration(trigger.IntervalSeconds) * time.Second
	if interval <= 0 {
		interval = time.Second
	}

	port := int32(trigger.HttpPort)
	if port == 0 && len(request.Ports) > 0 {
		port = int32(request.Ports[0])
	}
	if port == 0 {
		return fmt.Errorf("checkpoint HTTP readiness port is required")
	}

	readinessPath := trigger.HttpPath
	if !strings.HasPrefix(readinessPath, "/") {
		readinessPath = "/" + readinessPath
	}
	if outputLogger != nil {
		outputLogger.Info(fmt.Sprintf("Waiting for container HTTP readiness before checkpoint (port %d, path %s)", port, readinessPath))
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	client := &http.Client{Timeout: min(interval, 5*time.Second)}
	sampledLogger := log.Sample(&zerolog.BasicSampler{N: readyLogRate})

	for {
		if err := s.checkCheckpointHTTPReady(ctx, client, request, port, readinessPath); err == nil {
			if outputLogger != nil {
				outputLogger.Info("Container HTTP readiness reached for checkpoint")
			}
			return nil
		} else {
			sampledLogger.Info().Str("container_id", request.ContainerId).Int32("port", port).Str("path", readinessPath).Err(err).Msg("container not ready for checkpoint")
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("checkpoint HTTP readiness deadline exceeded")
		case <-ticker.C:
		}
	}
}

func (s *Worker) checkCheckpointHTTPReady(ctx context.Context, client *http.Client, request *types.ContainerRequest, port int32, readinessPath string) error {
	instance, exists := s.containerInstances.Get(request.ContainerId)
	if !exists {
		return fmt.Errorf("container instance not found yet")
	}

	addresses := checkpointHTTPReadinessAddresses(instance, port)
	if len(addresses) == 0 {
		return fmt.Errorf("container address for port %d not registered yet", port)
	}

	if !strings.HasPrefix(readinessPath, "/") {
		readinessPath = "/" + readinessPath
	}

	var firstErr error
	for _, address := range addresses {
		if err := checkCheckpointHTTPReadyAt(ctx, client, address, readinessPath); err == nil {
			return nil
		} else if firstErr == nil {
			firstErr = fmt.Errorf("%s: %w", address, err)
		}
	}

	return fmt.Errorf("checkpoint HTTP readiness failed at %w", firstErr)
}

func checkCheckpointHTTPReadyAt(ctx context.Context, client *http.Client, address, readinessPath string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+readinessPath, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("checkpoint HTTP readiness returned %d", resp.StatusCode)
	}
	return nil
}

func checkpointHTTPReadinessAddresses(instance *ContainerInstance, port int32) []string {
	if instance == nil {
		return nil
	}
	portText := strconv.Itoa(int(port))
	addresses := []string{}
	addAddress := func(address string) {
		if address == "" {
			return
		}
		for _, existing := range addresses {
			if existing == address {
				return
			}
		}
		addresses = append(addresses, address)
	}
	addHost := func(host string) {
		if host != "" {
			addAddress(net.JoinHostPort(host, portText))
		}
	}

	addHost(instance.ContainerIp)
	addHost(checkpointContainerIPv6(instance.ContainerIp))
	addAddress(instance.containerAddress(port))
	return addresses
}

func checkpointContainerIPv6(containerIP string) string {
	ip := net.ParseIP(containerIP)
	if ip == nil || ip.To4() == nil {
		return ""
	}
	_, ipv6Net, err := net.ParseCIDR(containerSubnetIPv6)
	if err != nil {
		return ""
	}
	ipv6Address, err := containerIPv6Address(ip, ipv6Net)
	if err != nil {
		return ""
	}
	return ipv6Address.String()
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
