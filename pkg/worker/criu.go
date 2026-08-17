package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
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
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/rs/zerolog/log"
)

const (
	gpuCntEnvKey                    = types.WorkerGPUCountEnv
	defaultCheckpointOperationTTL   = 10 * time.Minute
	checkpointArchiveExtension      = ".tar"
	checkpointOriginPrefix          = "checkpoints"
	checkpointProgressInterval      = 10 * time.Second
	checkpointForcedRuncProfileFile = "beam-forced-runc-profile"
	checkpointForcedRuncProfileV1   = "v1\n"
	checkpointListenerProofFile     = "beam-listeners.v1.json"
	restoreReadinessTimeout         = 15 * time.Second
	restoreReadinessInterval        = 100 * time.Millisecond
	restoreReadinessStableFor       = 250 * time.Millisecond
)

type checkpointListenerProof struct {
	Version int      `json:"version"`
	Ports   []uint32 `json:"ports"`
}

func (s *Worker) checkpointProcFilesystemRoot() string {
	if s != nil && s.checkpointProcRoot != "" {
		return s.checkpointProcRoot
	}
	return "/proc"
}

func checkpointProcessState(procRoot string, pid int) (byte, error) {
	if pid <= 0 {
		return 0, fmt.Errorf("runtime pid %d is invalid", pid)
	}
	data, err := os.ReadFile(filepath.Join(procRoot, strconv.Itoa(pid), "stat"))
	if err != nil {
		return 0, err
	}
	closing := bytes.LastIndexByte(data, ')')
	if closing < 0 || closing+2 >= len(data) {
		return 0, fmt.Errorf("runtime pid %d has malformed proc stat", pid)
	}
	state := data[closing+2]
	if state == 'Z' {
		return state, fmt.Errorf("runtime pid %d is a zombie", pid)
	}
	return state, nil
}

func checkpointListeningTCPPorts(procRoot string, pid int) (map[uint32]struct{}, error) {
	ports := make(map[uint32]struct{})
	for _, name := range []string{"tcp", "tcp6"} {
		data, err := os.ReadFile(filepath.Join(procRoot, strconv.Itoa(pid), "net", name))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for lineIndex, line := range strings.Split(string(data), "\n") {
			if lineIndex == 0 || strings.TrimSpace(line) == "" {
				continue
			}
			fields := strings.Fields(line)
			if len(fields) < 4 || fields[3] != "0A" {
				continue
			}
			address := strings.Split(fields[1], ":")
			if len(address) != 2 {
				return nil, fmt.Errorf("malformed listening socket address %q", fields[1])
			}
			port, err := strconv.ParseUint(address[1], 16, 16)
			if err != nil || port == 0 {
				return nil, fmt.Errorf("malformed listening socket port %q", address[1])
			}
			ports[uint32(port)] = struct{}{}
		}
	}
	return ports, nil
}

func (s *Worker) captureCheckpointListenerProof(ctx context.Context, request *types.ContainerRequest, rt runtime.Runtime) (checkpointListenerProof, error) {
	proof := checkpointListenerProof{Version: 1, Ports: []uint32{}}
	if request == nil || len(request.Ports) == 0 {
		return proof, nil
	}
	state, err := rt.State(ctx, request.ContainerId)
	if err != nil {
		return proof, fmt.Errorf("inspect runtime before checkpoint listener capture: %w", err)
	}
	if state.Status != types.RuncContainerStatusRunning || state.Pid <= 0 {
		return proof, fmt.Errorf("runtime is not running before checkpoint listener capture: status=%q pid=%d", state.Status, state.Pid)
	}
	if _, err := checkpointProcessState(s.checkpointProcFilesystemRoot(), state.Pid); err != nil {
		return proof, err
	}
	listening, err := checkpointListeningTCPPorts(s.checkpointProcFilesystemRoot(), state.Pid)
	if err != nil {
		return proof, fmt.Errorf("inspect runtime listeners before checkpoint: %w", err)
	}
	seen := make(map[uint32]struct{}, len(request.Ports))
	for _, port := range request.Ports {
		if _, duplicate := seen[port]; duplicate {
			continue
		}
		seen[port] = struct{}{}
		if _, ok := listening[port]; ok {
			proof.Ports = append(proof.Ports, port)
		}
	}
	slices.Sort(proof.Ports)
	return proof, nil
}

func writeCheckpointListenerProof(checkpointPath string, proof checkpointListenerProof) error {
	if proof.Version != 1 {
		return fmt.Errorf("unsupported checkpoint listener proof version %d", proof.Version)
	}
	data, err := json.Marshal(proof)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(checkpointPath, checkpointListenerProofFile), data, 0600)
}

func readCheckpointListenerProof(checkpointPath string) (checkpointListenerProof, error) {
	var proof checkpointListenerProof
	data, err := os.ReadFile(filepath.Join(checkpointPath, checkpointListenerProofFile))
	if err != nil {
		return proof, err
	}
	if err := json.Unmarshal(data, &proof); err != nil {
		return proof, err
	}
	if proof.Version != 1 {
		return proof, fmt.Errorf("unsupported checkpoint listener proof version %d", proof.Version)
	}
	seen := make(map[uint32]struct{}, len(proof.Ports))
	for _, port := range proof.Ports {
		if port == 0 {
			return proof, fmt.Errorf("checkpoint listener proof contains port zero")
		}
		if _, duplicate := seen[port]; duplicate {
			return proof, fmt.Errorf("checkpoint listener proof repeats port %d", port)
		}
		seen[port] = struct{}{}
	}
	return proof, nil
}

var checkpointRuntimeEnvOverrides = []string{
	"UV_USE_IO_URING=0",
	"TORCHINDUCTOR_QUIESCE_ASYNC_COMPILE_POOL=1",
}

var checkpointServiceLoopbackEnvOverrides = []string{
	"MASTER_ADDR=127.0.0.1",
	"NCCL_SOCKET_IFNAME=lo",
	"GLOO_SOCKET_IFNAME=lo",
	"VLLM_HOST_IP=127.0.0.1",
	"VLLM_LOOPBACK_IP=127.0.0.1",
}

var checkpointDisabledIOUringSyscalls = []string{
	"io_uring_setup",
	"io_uring_enter",
	"io_uring_register",
}

var errCRIUManagerUnavailable = errors.New("checkpoint/restore unavailable: CRIU manager is not initialized")

type checkpointDurableMountValidationError struct {
	mountPath string
	err       error
}

func (e *checkpointDurableMountValidationError) Error() string {
	if e.mountPath == "" {
		return fmt.Sprintf("validate restored durable disk mounts: %v", e.err)
	}
	return fmt.Sprintf("validate restored durable disk mount %q: %v", e.mountPath, e.err)
}

func (e *checkpointDurableMountValidationError) Unwrap() error {
	return e.err
}

type checkpointRestoreCleanupError struct {
	err error
}

func (e *checkpointRestoreCleanupError) Error() string {
	return fmt.Sprintf("clean up failed checkpoint restore: %v", e.err)
}

func (e *checkpointRestoreCleanupError) Unwrap() error {
	return e.err
}

func applyCheckpointRuntimeEnvironmentOverrides(env []string, request *types.ContainerRequest, processArgs []string) []string {
	if request == nil || !requestHasStateVolumes(request) {
		return env
	}
	env = upsertEnvVars(env, checkpointRuntimeEnvOverrides)
	if shouldUseLoopbackForPodCheckpoint(request, processArgs, env) {
		env = upsertEnvVars(env, checkpointServiceLoopbackEnvOverrides)
	}
	return env
}

func shouldUseLoopbackForPodCheckpoint(request *types.ContainerRequest, processArgs, env []string) bool {
	if request == nil || !request.RequiresGPU() {
		return false
	}
	if !isPodRequest(request) {
		return false
	}
	return hasLoopbackSensitiveGPUBackend(processArgs, env)
}

func isPodRequest(request *types.ContainerRequest) bool {
	return request != nil && request.Stub.Type.Kind() == types.StubTypePod
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

// StateMemoryCheckpoint is the private CRIU payload bound to one exact
// terminal StateSnapshot. It never exists as a standalone public resource.
type StateMemoryCheckpoint struct {
	ID          string
	Digest      string
	CacheHash   string
	SizeBytes   int64
	OriginKey   string
	Locality    string
	Accelerator string
	Runtime     string
	ContainerIP string
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
	checkpoint   *StateMemoryCheckpoint
	containerIP  string
	outputWriter io.Writer
	started      chan int
	configPath   string
	validate     func(context.Context, runtime.Runtime) error
}

type CRIUManager interface {
	Available() bool
	CreateCheckpoint(ctx context.Context, runtime runtime.Runtime, checkpointId string, request *types.ContainerRequest, terminateAfterCheckpoint bool) (string, error)
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

func (s *Worker) attemptRestoreCheckpoint(ctx context.Context, request *types.ContainerRequest, outputLogger *slog.Logger, outputWriter io.Writer, startedChan chan int) (exitCode int, restored bool, started bool, err error) {
	instance, exists := s.containerInstances.Get(request.ContainerId)
	if !exists {
		return -1, false, false, fmt.Errorf("container instance not found")
	}
	if instance.Runtime == nil {
		return -1, false, false, fmt.Errorf("container runtime not found")
	}
	checkpoint := instance.StateMemoryCheckpoint
	if checkpoint == nil || checkpoint.ID == "" || request.StateSnapshotId == "" {
		return -1, false, false, fmt.Errorf("terminal state snapshot has no bound memory checkpoint")
	}
	if err := s.requireCRIUManager(); err != nil {
		return -1, false, false, err
	}
	if checkpointPath := s.checkpointPath(checkpoint.ID); checkpointPath != "" {
		if err := validateCheckpointRuntimePayload(checkpointPath, instance.Runtime.Name()); err != nil {
			log.Warn().Err(err).
				Str("container_id", request.ContainerId).
				Str("checkpoint_id", checkpoint.ID).
				Str("runtime", instance.Runtime.Name()).
				Msg("checkpoint runtime payload is incompatible; starting container normally")
			if outputLogger != nil {
				outputLogger.Info("Checkpoint was created by an incompatible container runtime; starting container normally")
			}
			return -1, false, false, err
		}
	}

	if outputLogger != nil {
		outputLogger.Info("Attempting to restore container from checkpoint...")
	}

	restoreStarted := make(chan int, 1)
	restoreDone := make(chan restoreCheckpointResult, 1)
	go func() {
		exitCode, err := s.criuManager.RestoreCheckpoint(ctx, instance.Runtime, &RestoreOpts{
			request:      request,
			checkpoint:   checkpoint,
			containerIP:  instance.ContainerIp,
			outputWriter: outputWriter,
			started:      restoreStarted,
			configPath:   request.ConfigPath,
			validate:     s.checkpointRestoreValidator(request),
		})
		restoreDone <- restoreCheckpointResult{exitCode: exitCode, err: err}
	}()

	restoreStartedChan := (<-chan int)(restoreStarted)
	forwardStarted := func(pid int) error {
		if err := s.applyDeferredCheckpointRestoreCPUAffinity(ctx, request); err != nil {
			log.Error().Err(err).
				Str("container_id", request.ContainerId).
				Str("checkpoint_id", checkpoint.ID).
				Msg("failed to apply CPU affinity after checkpoint restore")
			cleanupErr := deleteFailedRestoreRuntimeContainer(ctx, instance.Runtime, request.ContainerId)
			if cleanupErr != nil {
				err = errors.Join(err, fmt.Errorf("clean up restore after CPU affinity failure: %w", cleanupErr))
			}
			return err
		}
		return forwardRestoreStarted(ctx, startedChan, pid)
	}
	for restoreDone != nil {
		select {
		case pid := <-restoreStartedChan:
			started = true
			restoreStartedChan = nil
			if err := forwardStarted(pid); err != nil {
				return -1, false, started, err
			}
		case result := <-restoreDone:
			exitCode, err = result.exitCode, result.err
			restoreDone = nil
			if !started {
				if pid, ok := restoreStartedPID(restoreStarted); ok {
					started = true
					if err == nil {
						if forwardErr := forwardStarted(pid); forwardErr != nil {
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
		log.Error().Str("container_id", request.ContainerId).Str("checkpoint_id", checkpoint.ID).Msgf("failed to restore checkpoint: %v", err)

		hostIncompatible := IsCheckpointHostIncompatible(err)
		if hostIncompatible {
			outputLogger.Info("Checkpoint was created on an incompatible CPU; starting container normally")
		} else {
			outputLogger.Error(fmt.Sprintf("Failed to restore checkpoint: %v", err))
		}
		if cleanupErr := deleteFailedRestoreRuntimeContainer(ctx, instance.Runtime, request.ContainerId); cleanupErr != nil {
			log.Warn().
				Err(cleanupErr).
				Str("container_id", request.ContainerId).
				Str("checkpoint_id", checkpoint.ID).
				Msg("failed to clean up runtime container after checkpoint restore failure")
			err = errors.Join(err, &checkpointRestoreCleanupError{err: cleanupErr})
		}
		return exitCode, false, started, err
	}

	if !started {
		err := fmt.Errorf("checkpoint restore completed without runtime start")
		log.Error().Str("container_id", request.ContainerId).Str("checkpoint_id", checkpoint.ID).Msg(err.Error())
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

func (s *Worker) prepareRestoreFallback(ctx context.Context, request *types.ContainerRequest, config []byte) error {
	if request != nil {
		if instance, exists := s.containerInstances.Get(request.ContainerId); exists {
			instance.RestoreCPUAffinityDeferred = false
			instance.resetRuntimeStarted()
			s.containerInstances.Set(request.ContainerId, instance)
		}
	}
	if request == nil || request.ConfigPath == "" || len(config) == 0 {
		return nil
	}

	instance, exists := s.containerInstances.Get(request.ContainerId)
	if exists && instance.Overlay != nil {
		if request.StateSnapshotId != "" {
			resetCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
			defer cancel()
			if err := instance.Overlay.Cleanup(); err != nil {
				return fmt.Errorf("unmount failed state restore overlay: %w", err)
			}
			if s.stateVolumeManager == nil {
				return fmt.Errorf("reset state restore: state volume manager is unavailable")
			}
			if err := s.stateVolumeManager.QuarantineWritable(resetCtx, request.ContainerId); err != nil {
				return fmt.Errorf("quarantine failed state restore volumes: %w", err)
			}
			instance.StateVolumes = nil
			handle, err := s.restoreStateVolumes(resetCtx, request, instance, request.StateSnapshotId)
			if err != nil {
				s.containerInstances.Set(request.ContainerId, instance)
				return fmt.Errorf("rematerialize exact state snapshot: %w", err)
			}
			instance.StateVolumes = handle
			if upper, work, ok := handle.PersistentOverlayPaths(); ok {
				err = instance.Overlay.SetupWithWritable(upper, work)
			} else {
				err = instance.Overlay.Setup()
			}
			if err != nil {
				_ = s.stateVolumeManager.QuarantineWritable(resetCtx, request.ContainerId)
				instance.StateVolumes = nil
				s.containerInstances.Set(request.ContainerId, instance)
				return fmt.Errorf("mount exact cold restore overlay: %w", err)
			}
			s.containerInstances.Set(request.ContainerId, instance)
		} else if err := instance.Overlay.Reset(); err != nil {
			return fmt.Errorf("reset container overlay: %w", err)
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
	Request                  *types.ContainerRequest
	CheckpointId             string
	ContainerIp              string
	OutputLogger             *slog.Logger
	TerminateAfterCheckpoint bool
	CheckpointRuntime        string
	CheckpointMetadata       *checkpointCacheMetadata
	RequireListenerProof     bool
}

func forcedRuncCheckpointProfileRequired(request *types.ContainerRequest, rt runtime.Runtime) bool {
	return requestForcesResourceLimits(request) && rt != nil && rt.Name() == types.ContainerRuntimeRunc.String()
}

func validateForcedRuncCheckpointProfile(configPath string) error {
	contents, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("read container config: %w", err)
	}

	var spec specs.Spec
	if err := json.Unmarshal(contents, &spec); err != nil {
		return fmt.Errorf("decode container config: %w", err)
	}
	if spec.Linux == nil || !slices.Contains(spec.Linux.ReadonlyPaths, "/proc/sys/vm/drop_caches") {
		return errors.New("container config does not protect /proc/sys/vm/drop_caches")
	}
	return nil
}

func writeForcedRuncCheckpointProfile(checkpointPath string, request *types.ContainerRequest, rt runtime.Runtime) error {
	if !forcedRuncCheckpointProfileRequired(request, rt) {
		return nil
	}
	markerPath := filepath.Join(checkpointPath, checkpointForcedRuncProfileFile)
	if err := os.Remove(markerPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("reset forced runc checkpoint profile marker: %w", err)
	}
	if request.DockerEnabled {
		return errors.New("forced runc profile does not support Docker-enabled containers")
	}
	if err := validateForcedRuncCheckpointProfile(request.ConfigPath); err != nil {
		return fmt.Errorf("validate forced runc checkpoint profile: %w", err)
	}

	if err := os.WriteFile(markerPath, []byte(checkpointForcedRuncProfileV1), 0644); err != nil {
		_ = os.Remove(markerPath)
		return fmt.Errorf("write forced runc checkpoint profile marker: %w", err)
	}
	return nil
}

// Waits for the container to be ready to checkpoint at the desired point in execution, ie.
// after all processes within a container have reached a checkpointable state
func (s *Worker) createCheckpoint(ctx context.Context, opts *CreateCheckpointOpts) (err error) {
	if opts == nil || opts.Request == nil {
		return errors.New("checkpoint request is required")
	}
	if !opts.TerminateAfterCheckpoint || !opts.RequireListenerProof {
		return errors.New("CRIU capture is only allowed for a terminal state snapshot")
	}

	runtimeName := ""
	var persistedMetadata *checkpointCacheMetadata

	instance, exists := s.containerInstances.Get(opts.Request.ContainerId)
	if !exists || instance == nil {
		return fmt.Errorf("container instance not found")
	}
	if instance.Runtime == nil {
		return fmt.Errorf("container runtime is unavailable")
	}
	runtimeName = instance.Runtime.Name()
	opts.CheckpointRuntime = runtimeName
	if err := s.requireCRIUManager(); err != nil {
		return err
	}

	if opts.TerminateAfterCheckpoint {
		if !supportsTerminalCheckpoint(instance.Runtime) {
			return fmt.Errorf("runtime %s does not support terminal checkpointing", runtimeName)
		}
	}

	log.Info().Str("container_id", opts.Request.ContainerId).Str("checkpoint_id", opts.CheckpointId).Msg("creating checkpoint")
	// Proceed to create the checkpoint
	if opts.OutputLogger != nil {
		opts.OutputLogger.Info("Creating container checkpoint snapshot")
	}
	// Once terminal CRIU capture is selected, caller cancellation cannot safely
	// undo the runtime stop, so the remaining work is worker-owned.
	checkpointDeadline := time.Now().Add(defaultCheckpointOperationTTL)
	terminateRuntime := true
	checkpointParent := ctx
	if terminateRuntime {
		checkpointParent = context.WithoutCancel(ctx)
	}
	checkpointCtx, checkpointCancel := context.WithDeadline(checkpointParent, checkpointDeadline)
	defer func() { checkpointCancel() }()
	resumeOOMWatcher := func() {}
	if terminateRuntime {
		resumeOOMWatcher = instance.suspendOOMWatcher()
		if oomTerminationAttempted, oomErr := instance.oomTerminationResult(); oomTerminationAttempted {
			if oomErr != nil {
				resumeOOMWatcher()
				return fmt.Errorf("cannot create terminal checkpoint because OOM termination did not complete: %w", oomErr)
			}
			return errors.New("cannot create terminal checkpoint because the container was terminated after OOM")
		}
		log.Info().Str("container_id", opts.Request.ContainerId).Str("checkpoint_id", opts.CheckpointId).Msg("creating terminal checkpoint")
	}

	listenerProof := checkpointListenerProof{Version: 1, Ports: []uint32{}}
	if opts.RequireListenerProof {
		listenerProof, err = s.captureCheckpointListenerProof(checkpointCtx, opts.Request, instance.Runtime)
		if err != nil {
			return fmt.Errorf("capture checkpoint listener proof: %w", err)
		}
	}
	checkpointPath, err := s.criuManager.CreateCheckpoint(checkpointCtx, instance.Runtime, opts.CheckpointId, opts.Request, terminateRuntime)
	if err != nil {
		if terminateRuntime && checkpointRuntimeHasStopped(checkpointCtx, instance.Runtime, opts.Request.ContainerId) {
			s.markTerminalStateCheckpointStopped(opts.Request, instance)
		} else {
			resumeOOMWatcher()
		}
		if errors.Is(checkpointCtx.Err(), context.DeadlineExceeded) {
			err = fmt.Errorf("checkpoint snapshot timed out after %s: %w", defaultCheckpointOperationTTL, err)
		}
		if opts.OutputLogger != nil {
			opts.OutputLogger.Error(fmt.Sprintf("Failed to create checkpoint: %v", err))
		}
		return err
	}
	if terminateRuntime {
		s.markTerminalStateCheckpointStopped(opts.Request, instance)
	}
	if opts.RequireListenerProof {
		if err = writeCheckpointListenerProof(checkpointPath, listenerProof); err != nil {
			return fmt.Errorf("persist checkpoint listener proof: %w", err)
		}
	}
	if err = writeForcedRuncCheckpointProfile(checkpointPath, opts.Request, instance.Runtime); err != nil {
		return err
	}

	if opts.OutputLogger != nil {
		opts.OutputLogger.Info("Persisting container checkpoint")
	}
	persistedMetadata, err = s.persistCheckpoint(checkpointCtx, opts.Request, opts.CheckpointId, checkpointPath, opts.OutputLogger)
	if err != nil {
		if errors.Is(checkpointCtx.Err(), context.DeadlineExceeded) {
			err = fmt.Errorf("checkpoint persistence timed out after %s: %w", defaultCheckpointOperationTTL, err)
		}
		log.Error().Str("container_id", opts.Request.ContainerId).Str("checkpoint_id", opts.CheckpointId).Msgf("failed to persist checkpoint: %v", err)
		if opts.OutputLogger != nil {
			opts.OutputLogger.Error(fmt.Sprintf("Failed to persist checkpoint: %v", err))
		}
		return err
	}

	opts.CheckpointMetadata = persistedMetadata

	if opts.OutputLogger != nil {
		opts.OutputLogger.Info("Checkpoint created successfully")
	} else {
		log.Info().Str("container_id", opts.Request.ContainerId).Str("checkpoint_id", opts.CheckpointId).Msg("checkpoint created successfully")
	}
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

func (s *Worker) ensureCheckpointMaterialized(ctx context.Context, request *types.ContainerRequest, checkpoint *StateMemoryCheckpoint) (string, error) {
	return s.ensureCheckpointMaterializedWithLogger(ctx, request, checkpoint, nil)
}

func (s *Worker) ensureCheckpointMaterializedWithLogger(ctx context.Context, request *types.ContainerRequest, checkpoint *StateMemoryCheckpoint, outputLogger *slog.Logger) (string, error) {
	if checkpoint == nil {
		return "", fmt.Errorf("checkpoint is required")
	}

	checkpointPath := s.checkpointPath(checkpoint.ID)
	if checkpointPath == "" {
		return "", fmt.Errorf("checkpoint path is unavailable")
	}
	metadataComplete := checkpoint.CacheHash != "" && checkpoint.SizeBytes > 0 && checkpoint.OriginKey != ""
	if checkpointMaterialized(checkpointPath) {
		return checkpointPath, validateCheckpointKind(checkpointPath)
	}
	if !metadataComplete {
		return "", fmt.Errorf("checkpoint cache metadata is incomplete")
	}
	if s.cacheManager == nil {
		return "", fmt.Errorf("checkpoint cache manager is unavailable")
	}
	release, err := s.cacheManager.acquireCheckpointMaterialization(ctx, checkpoint.ID)
	if err != nil {
		return "", err
	}
	defer release()
	if checkpointMaterialized(checkpointPath) {
		return checkpointPath, validateCheckpointKind(checkpointPath)
	}
	s.cacheManager.requestReconcile()

	started := time.Now()
	if outputLogger != nil {
		outputLogger.Info(fmt.Sprintf("Materializing checkpoint from cache (%s)...\n", formatImageBytes(checkpoint.SizeBytes)))
	}
	err = s.materializeCheckpointFromCache(ctx, checkpointPath, checkpoint)
	if err == nil {
		if err := validateCheckpointKind(checkpointPath); err != nil {
			return "", err
		}
		s.logCheckpointMaterialized(outputLogger, checkpoint, "cache", started)
		return checkpointPath, nil
	}
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	log.Debug().Err(err).Str("checkpoint_id", checkpoint.ID).Msg("checkpoint cache materialization failed; falling back to origin")

	started = time.Now()
	if outputLogger != nil {
		outputLogger.Info(fmt.Sprintf("Checkpoint cache miss; downloading from workspace storage (%s)...\n", formatImageBytes(checkpoint.SizeBytes)))
	}
	if err := s.materializeCheckpointFromOrigin(ctx, request, checkpointPath, checkpoint); err != nil {
		return "", err
	}
	if err := validateCheckpointKind(checkpointPath); err != nil {
		return "", err
	}
	s.logCheckpointMaterialized(outputLogger, checkpoint, "origin", started)
	return checkpointPath, nil
}

func (s *Worker) logCheckpointMaterialized(outputLogger *slog.Logger, checkpoint *StateMemoryCheckpoint, source string, started time.Time) {
	duration := time.Since(started)
	log.Info().
		Str("checkpoint_id", checkpoint.ID).
		Str("source", source).
		Int64("bytes", checkpoint.SizeBytes).
		Dur("duration", duration).
		Msg("checkpoint materialized")
	if outputLogger != nil {
		outputLogger.Info(fmt.Sprintf("Checkpoint materialized from %s in %s\n", source, duration.Round(time.Millisecond)))
	}
}

func (s *Worker) materializeCheckpointFromCache(ctx context.Context, checkpointPath string, checkpoint *StateMemoryCheckpoint) error {
	if s.cacheManager == nil || s.cacheManager.client == nil {
		return fmt.Errorf("cache is unavailable")
	}
	reader := newCheckpointCacheReader(ctx, checkpoint.CacheHash, checkpoint.SizeBytes, func(ctx context.Context, hash string, offset int64, dst []byte) (int64, error) {
		return s.cacheManager.client.ReadContentInto(ctx, hash, offset, dst, cache.ClientOptions{RoutingKey: checkpoint.CacheHash})
	})
	return materializeCheckpointReader(ctx, reader, checkpoint.CacheHash, checkpoint.SizeBytes, checkpointPath, checkpoint.ID, nil)
}

func (s *Worker) materializeCheckpointFromOrigin(ctx context.Context, request *types.ContainerRequest, checkpointPath string, checkpoint *StateMemoryCheckpoint) error {
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

	var archive *os.File
	if s.cacheManager != nil && s.cacheManager.client != nil {
		if err := os.MkdirAll(filepath.Dir(checkpointPath), 0755); err != nil {
			return err
		}
		archive, err = os.CreateTemp(filepath.Dir(checkpointPath), "."+checkpoint.ID+".origin-*.tar")
		if err != nil {
			log.Warn().Err(err).Str("checkpoint_id", checkpoint.ID).Msg("failed to create checkpoint cache staging file")
		}
	}

	var archiveWriter io.Writer
	var cacheWriter *bestEffortCheckpointCacheWriter
	if archive != nil {
		cacheWriter = &bestEffortCheckpointCacheWriter{writer: archive}
		archiveWriter = cacheWriter
	}
	err = materializeCheckpointReader(ctx, reader, checkpoint.CacheHash, checkpoint.SizeBytes, checkpointPath, checkpoint.ID, archiveWriter)
	if archive == nil {
		return err
	}
	archivePath := archive.Name()
	closeErr := archive.Close()
	if err != nil {
		_ = os.Remove(archivePath)
		return err
	}
	if cacheWriter.err != nil || closeErr != nil {
		_ = os.Remove(archivePath)
		log.Warn().Err(errors.Join(cacheWriter.err, closeErr)).Str("checkpoint_id", checkpoint.ID).Msg("failed to stage restored checkpoint archive for cache")
		return nil
	}
	s.cacheCheckpointArchiveAsync(checkpoint.ID, archivePath, checkpoint.OriginKey, checkpoint.CacheHash)
	return nil
}

func materializeCheckpointArchive(archivePath, checkpointPath, checkpointID string) error {
	archive, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	defer archive.Close()
	info, err := archive.Stat()
	if err != nil {
		return err
	}
	return materializeCheckpointReader(context.Background(), archive, "", info.Size(), checkpointPath, checkpointID, nil)
}

func materializeCheckpointReader(ctx context.Context, reader io.Reader, expectedHash string, expectedSize int64, checkpointPath, checkpointID string, archiveWriter io.Writer) error {
	if reader == nil {
		return fmt.Errorf("checkpoint archive reader is required")
	}
	if expectedSize <= 0 {
		return fmt.Errorf("checkpoint archive size must be positive")
	}
	if err := os.MkdirAll(filepath.Dir(checkpointPath), 0755); err != nil {
		return err
	}
	tmpRoot, err := os.MkdirTemp(filepath.Dir(checkpointPath), "."+checkpointID+".extract-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpRoot)

	hasher := sha256.New()
	counter := &countingWriter{}
	writers := []io.Writer{hasher, counter}
	if archiveWriter != nil {
		writers = append(writers, archiveWriter)
	}
	verifiedReader := io.TeeReader(io.LimitReader(reader, expectedSize+1), io.MultiWriter(writers...))
	if err := untarCheckpointReader(ctx, verifiedReader, tmpRoot); err != nil {
		return err
	}
	if _, err := io.Copy(io.Discard, verifiedReader); err != nil {
		return err
	}
	if counter.n != expectedSize {
		return fmt.Errorf("checkpoint archive size mismatch: expected %d, got %d", expectedSize, counter.n)
	}
	if expectedHash != "" {
		actualHash := hex.EncodeToString(hasher.Sum(nil))
		if actualHash != expectedHash {
			return fmt.Errorf("checkpoint archive hash mismatch: expected %s, got %s", expectedHash, actualHash)
		}
	}

	extractedPath := filepath.Join(tmpRoot, checkpointID)
	if !checkpointMaterialized(extractedPath) {
		return fmt.Errorf("checkpoint archive missing runtime payload")
	}
	if checkpointMaterialized(checkpointPath) {
		return nil
	}
	if err := os.RemoveAll(checkpointPath); err != nil {
		return err
	}
	return os.Rename(extractedPath, checkpointPath)
}

type bestEffortCheckpointCacheWriter struct {
	writer io.Writer
	err    error
}

func (w *bestEffortCheckpointCacheWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return len(p), nil
	}
	n, err := w.writer.Write(p)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	if err != nil {
		w.err = err
		return len(p), nil
	}
	return n, nil
}

func untarCheckpointReader(ctx context.Context, reader io.Reader, destDir string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	cmd := exec.CommandContext(ctx, "tar", append(tarXattrArgs(), "-xf", "-", "-C", destDir)...)
	var stderr bytes.Buffer
	cmd.Stdin = reader
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return tarCommandError(fmt.Sprintf("extract checkpoint to %s", destDir), err, stderr)
	}
	return nil
}

const checkpointCacheReadBufferSize = 4 * 1024 * 1024

type checkpointCacheReader struct {
	ctx        context.Context
	hash       string
	size       int64
	nextOffset int64
	buffer     []byte
	bufferPos  int
	readAt     func(context.Context, string, int64, []byte) (int64, error)
}

func newCheckpointCacheReader(ctx context.Context, hash string, size int64, readAt func(context.Context, string, int64, []byte) (int64, error)) io.Reader {
	if ctx == nil {
		ctx = context.Background()
	}
	return &checkpointCacheReader{ctx: ctx, hash: hash, size: size, readAt: readAt}
}

func (r *checkpointCacheReader) Read(dst []byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	if r.bufferPos >= len(r.buffer) {
		if r.nextOffset >= r.size {
			return 0, io.EOF
		}
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}
		length := min(int64(checkpointCacheReadBufferSize), r.size-r.nextOffset)
		if cap(r.buffer) < int(length) {
			r.buffer = make([]byte, length)
		} else {
			r.buffer = r.buffer[:length]
		}
		n, err := r.readAt(r.ctx, r.hash, r.nextOffset, r.buffer)
		if err != nil {
			return 0, err
		}
		if n != length {
			return 0, fmt.Errorf("short checkpoint cache read at %d: expected %d bytes, got %d", r.nextOffset, length, n)
		}
		r.nextOffset += n
		r.bufferPos = 0
	}

	n := copy(dst, r.buffer[r.bufferPos:])
	r.bufferPos += n
	return n, nil
}

func checkpointMaterialized(checkpointPath string) bool {
	return checkpointHasRuntimePayload(checkpointPath)
}

func validateCheckpointKind(checkpointPath string) error {
	if !checkpointHasRuntimePayload(checkpointPath) {
		return errors.New("memory checkpoint payload is invalid")
	}
	return nil
}

type ErrCheckpointRuntimeIncompatible struct {
	RuntimeName string
	PayloadName string
	Err         error
}

func (e *ErrCheckpointRuntimeIncompatible) Error() string {
	return fmt.Sprintf("checkpoint is incompatible with %s: required runtime payload %s: %v", e.RuntimeName, e.PayloadName, e.Err)
}

func (e *ErrCheckpointRuntimeIncompatible) Unwrap() error {
	return e.Err
}

func validateCheckpointRuntimePayload(checkpointPath, runtimeName string) error {
	payloadName := ""
	switch runtimeName {
	case types.ContainerRuntimeRunc.String():
		payloadName = "inventory.img"
	case types.ContainerRuntimeGvisor.String():
		payloadName = "checkpoint.img"
	default:
		if checkpointHasRuntimePayload(checkpointPath) {
			return nil
		}
		return fmt.Errorf("checkpoint has no runtime payload for %s", runtimeName)
	}

	payloadPath := filepath.Join(checkpointPath, payloadName)
	info, err := os.Stat(payloadPath)
	if err != nil {
		return &ErrCheckpointRuntimeIncompatible{RuntimeName: runtimeName, PayloadName: payloadName, Err: err}
	}
	if !info.Mode().IsRegular() || info.Size() == 0 {
		return &ErrCheckpointRuntimeIncompatible{
			RuntimeName: runtimeName,
			PayloadName: payloadName,
			Err:         errors.New("payload is empty or invalid"),
		}
	}
	return nil
}

func checkpointHasRuntimePayload(checkpointPath string) bool {
	entries, err := os.ReadDir(checkpointPath)
	if err != nil {
		return false
	}

	for _, entry := range entries {
		if entry.Name() == checkpointForcedRuncProfileFile || entry.Name() == checkpointListenerProofFile {
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

func supportsTerminalCheckpoint(rt runtime.Runtime) bool {
	if rt == nil {
		return false
	}
	return rt.Name() == types.ContainerRuntimeRunc.String() ||
		rt.Name() == types.ContainerRuntimeGvisor.String()
}

func checkpointRuntimeHasStopped(ctx context.Context, rt runtime.Runtime, containerID string) bool {
	if rt == nil {
		return false
	}
	stateCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	state, err := rt.State(stateCtx, containerID)
	if err != nil {
		return runtimeContainerNotFound(err)
	}
	return state.Status == types.RuncContainerStatusStopped
}

func (s *Worker) markTerminalStateCheckpointStopped(request *types.ContainerRequest, instance *ContainerInstance) {
	if instance != nil {
		instance.stopOOMWatcher()
		instance.setStopReason(types.StopContainerReasonUser)
		s.containerInstances.Set(request.ContainerId, instance)
	}
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

func (s *Worker) checkpointRestoreValidator(request *types.ContainerRequest) func(context.Context, runtime.Runtime) error {
	instance := s.mustContainerInstance(request.ContainerId)
	validateStateMemory := request.StateSnapshotId != "" && instance != nil && instance.StateMemoryCheckpoint != nil
	validateDurableMounts := false
	if requestForcesResourceLimits(request) {
		for _, mount := range request.Mounts {
			if mount.DurableDisk != nil {
				validateDurableMounts = true
				break
			}
		}
	}
	if !validateDurableMounts && !validateStateMemory {
		return nil
	}

	return func(ctx context.Context, rt runtime.Runtime) error {
		if validateStateMemory {
			checkpointPath := s.checkpointPath(instance.StateMemoryCheckpoint.ID)
			if checkpointPath == "" {
				return fmt.Errorf("state checkpoint listener proof path is unavailable")
			}
			if err := s.waitForRestoredCheckpointListenerProof(ctx, request, rt, checkpointPath); err != nil {
				return err
			}
		}
		if validateDurableMounts {
			if err := validateRestoredDurableDiskMounts(ctx, request, rt); err != nil {
				return err
			}
		}
		return nil
	}
}

func (s *Worker) waitForRestoredCheckpointListenerProof(ctx context.Context, request *types.ContainerRequest, rt runtime.Runtime, checkpointPath string) error {
	if request == nil || rt == nil {
		return fmt.Errorf("restored checkpoint listener validation context is incomplete")
	}
	proof, err := readCheckpointListenerProof(checkpointPath)
	if err != nil {
		return fmt.Errorf("read restored checkpoint listener proof: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, restoreReadinessTimeout)
	defer cancel()
	ticker := time.NewTicker(restoreReadinessInterval)
	defer ticker.Stop()
	var firstHealthyAt time.Time
	var lastErr error
	for {
		state, stateErr := rt.State(ctx, request.ContainerId)
		if stateErr != nil {
			lastErr = fmt.Errorf("restored runtime state: %w", stateErr)
		} else if state.Status != types.RuncContainerStatusRunning || state.Pid <= 0 {
			lastErr = fmt.Errorf("restored runtime status %q with pid %d", state.Status, state.Pid)
		} else if _, stateErr = checkpointProcessState(s.checkpointProcFilesystemRoot(), state.Pid); stateErr != nil {
			lastErr = stateErr
		} else {
			listening, listenErr := checkpointListeningTCPPorts(s.checkpointProcFilesystemRoot(), state.Pid)
			if listenErr != nil {
				lastErr = listenErr
			} else {
				lastErr = nil
				for _, port := range proof.Ports {
					if _, ok := listening[port]; !ok {
						lastErr = fmt.Errorf("restored runtime is not listening on checkpoint port %d", port)
						break
					}
					if err := dialRestoredCheckpointPort(ctx, stateCheckpointListenerAddresses(s.mustContainerInstance(request.ContainerId), int32(port))); err != nil {
						lastErr = fmt.Errorf("restored checkpoint port %d is unreachable: %w", port, err)
						break
					}
				}
			}
		}
		if lastErr == nil {
			if firstHealthyAt.IsZero() {
				firstHealthyAt = time.Now()
			} else if time.Since(firstHealthyAt) >= restoreReadinessStableFor {
				return nil
			}
		} else {
			firstHealthyAt = time.Time{}
		}
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return fmt.Errorf("restored checkpoint runtime failed listener/zombie readiness: %w", lastErr)
			}
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Worker) mustContainerInstance(containerID string) *ContainerInstance {
	if s == nil || s.containerInstances == nil {
		return nil
	}
	instance, _ := s.containerInstances.Get(containerID)
	return instance
}

func dialRestoredCheckpointPort(ctx context.Context, addresses []string) error {
	if len(addresses) == 0 {
		return fmt.Errorf("container has no routable address")
	}
	var firstErr error
	for _, address := range addresses {
		dialCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		connection, err := (&net.Dialer{}).DialContext(dialCtx, "tcp", address)
		cancel()
		if err == nil {
			return connection.Close()
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func validateRestoredDurableDiskMounts(ctx context.Context, request *types.ContainerRequest, rt runtime.Runtime) error {
	if request == nil || rt == nil || rt.Name() != types.ContainerRuntimeRunc.String() {
		return nil
	}

	state, err := rt.State(ctx, request.ContainerId)
	if err != nil {
		return &checkpointDurableMountValidationError{err: fmt.Errorf("get runtime state: %w", err)}
	}
	if state.Status != types.RuncContainerStatusRunning || state.Pid <= 0 {
		return &checkpointDurableMountValidationError{err: fmt.Errorf("runtime status %q with pid %d", state.Status, state.Pid)}
	}

	return validateRestoredDurableDiskMountsAtRoot(request.Mounts, filepath.Join("/proc", strconv.Itoa(state.Pid), "root"))
}

func validateRestoredDurableDiskMountsAtRoot(mounts []types.Mount, containerRoot string) error {
	for _, mount := range mounts {
		if mount.DurableDisk == nil {
			continue
		}
		if !filepath.IsAbs(mount.LocalPath) || !filepath.IsAbs(mount.MountPath) {
			return &checkpointDurableMountValidationError{
				mountPath: mount.MountPath,
				err:       fmt.Errorf("invalid source %q or target", mount.LocalPath),
			}
		}

		targetPath := filepath.Join(containerRoot, strings.TrimPrefix(filepath.Clean(mount.MountPath), string(filepath.Separator)))
		sourceLinkInfo, err := os.Lstat(mount.LocalPath)
		if err != nil {
			return &checkpointDurableMountValidationError{mountPath: mount.MountPath, err: fmt.Errorf("lstat source %q: %w", mount.LocalPath, err)}
		}
		targetLinkInfo, err := os.Lstat(targetPath)
		if err != nil {
			return &checkpointDurableMountValidationError{mountPath: mount.MountPath, err: fmt.Errorf("lstat restored target %q: %w", targetPath, err)}
		}
		if sourceLinkInfo.Mode()&os.ModeSymlink != 0 || targetLinkInfo.Mode()&os.ModeSymlink != 0 {
			return &checkpointDurableMountValidationError{
				mountPath: mount.MountPath,
				err:       fmt.Errorf("source %q or restored target %q is a symlink", mount.LocalPath, targetPath),
			}
		}
		sourceInfo, err := os.Stat(mount.LocalPath)
		if err != nil {
			return &checkpointDurableMountValidationError{mountPath: mount.MountPath, err: fmt.Errorf("stat source %q: %w", mount.LocalPath, err)}
		}
		targetInfo, err := os.Stat(targetPath)
		if err != nil {
			return &checkpointDurableMountValidationError{mountPath: mount.MountPath, err: fmt.Errorf("stat restored target %q: %w", targetPath, err)}
		}
		if !os.SameFile(sourceInfo, targetInfo) {
			return &checkpointDurableMountValidationError{
				mountPath: mount.MountPath,
				err:       fmt.Errorf("source %q does not back restored target %q", mount.LocalPath, targetPath),
			}
		}
	}
	return nil
}

func stateCheckpointListenerAddresses(instance *ContainerInstance, port int32) []string {
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
