package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/runtime"
	types "github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	minNvidiaDriverVersion      = 570
	gvisorCheckpointAttempts    = 4
	gvisorCheckpointRetryDelay  = 250 * time.Millisecond
	nvidiaRestoreAttempts       = 2
	nvidiaRestoreRetryDelay     = 250 * time.Millisecond
	maxRestoreStderrBytes       = 4096
	maxRestoreCaptureBytes      = 64 << 10
	checkpointNetworkMapFile    = "beam-network-map-v1"
	checkpointNetworkMapVersion = "v1"
)

type restoreOutputCapture struct {
	mu         sync.Mutex
	downstream io.Writer
	capturing  bool
	buf        strings.Builder
}

func newRestoreOutputCapture(downstream io.Writer) *restoreOutputCapture {
	return &restoreOutputCapture{downstream: downstream, capturing: true}
}

func (w *restoreOutputCapture) Write(p []byte) (int, error) {
	w.mu.Lock()
	if w.capturing && w.buf.Len() < maxRestoreCaptureBytes {
		remaining := maxRestoreCaptureBytes - w.buf.Len()
		if len(p) < remaining {
			remaining = len(p)
		}
		_, _ = w.buf.Write(p[:remaining])
	}
	w.mu.Unlock()

	if w.downstream != nil {
		return w.downstream.Write(p)
	}
	return len(p), nil
}

func (w *restoreOutputCapture) stop() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.capturing = false
	return w.buf.String()
}

type ErrCRIURestoreFailed struct {
	Stderr string
}

func (e *ErrCRIURestoreFailed) Error() string {
	return fmt.Sprintf("CRIU restore failed: %s", e.Stderr)
}

func IsCRIURestoreError(err error) bool {
	var criuErr *ErrCRIURestoreFailed
	return errors.As(err, &criuErr)
}

type NvidiaCRIUManager struct {
	checkpointRoot string
	gpuCnt         int
	available      bool
}

func InitializeNvidiaCRIU(ctx context.Context, config types.CRIUConfig, checkpointRoot string) (CRIUManager, error) {
	gpuCnt := 0
	var err error
	gpuCntEnv := os.Getenv(gpuCntEnvKey)
	if gpuCntEnv != "" {
		gpuCnt, err = strconv.Atoi(gpuCntEnv)
		if err != nil {
			return nil, fmt.Errorf("failed to parse GPU_COUNT: %v", err)
		}
	}

	available := crCompatible(gpuCnt)

	return &NvidiaCRIUManager{checkpointRoot: checkpointRoot, gpuCnt: gpuCnt, available: available}, nil
}

func (c *NvidiaCRIUManager) CreateCheckpoint(ctx context.Context, rt runtime.Runtime, checkpointId string, request *types.ContainerRequest, terminateAfterCheckpoint bool) (string, error) {
	checkpointPath := filepath.Join(c.checkpointRoot, checkpointId)
	workDir := filepath.Join(types.AgentTmpPath, checkpointId)
	terminateAfterCheckpoint = terminateAfterCheckpoint && rt.Name() == types.ContainerRuntimeRunc.String()

	// Setup work directory for checkpoint files
	if err := os.MkdirAll(workDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create checkpoint work directory: %w", err)
	}

	checkpointOpts := &runtime.CheckpointOpts{
		ImagePath:    checkpointPath,
		WorkDir:      workDir, // Required for checkpoint files (logs, cache, sockets)
		LeaveRunning: !terminateAfterCheckpoint,
		AllowOpenTCP: true, // Allow open TCP connections
		SkipInFlight: true, // Skip in-flight TCP packets
		LinkRemap:    true, // Enable link remapping for file descriptors
	}

	attempts := 1
	if rt.Name() == types.ContainerRuntimeGvisor.String() {
		attempts = gvisorCheckpointAttempts
	}

	var err error
	for attempt := 1; attempt <= attempts; attempt++ {
		err = rt.Checkpoint(ctx, request.ContainerId, checkpointOpts)
		if err == nil {
			break
		}
		if attempt == attempts || !retryableGvisorCheckpointError(rt, err) {
			return "", fmt.Errorf("checkpoint failed for runtime %s: %w", rt.Name(), err)
		}

		log.Warn().
			Err(err).
			Str("runtime", rt.Name()).
			Str("checkpoint_id", checkpointId).
			Int("attempt", attempt).
			Msg("retrying checkpoint after transient runtime error")

		_ = os.RemoveAll(checkpointPath)
		_ = os.RemoveAll(workDir)
		if err := os.MkdirAll(workDir, 0755); err != nil {
			return "", fmt.Errorf("failed to recreate checkpoint work directory: %w", err)
		}
		if err := waitCheckpointRetry(ctx, attempt); err != nil {
			return "", err
		}
	}

	log.Info().
		Str("runtime", rt.Name()).
		Str("checkpoint_id", checkpointId).
		Str("checkpoint_path", checkpointPath).
		Str("work_dir", workDir).
		Msg("checkpoint created successfully")

	return checkpointPath, nil
}

func retryableGvisorCheckpointError(rt runtime.Runtime, err error) bool {
	if err == nil || rt.Name() != types.ContainerRuntimeGvisor.String() {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "tcp.Endpoint") ||
		strings.Contains(msg, "invalid memory address or nil pointer dereference")
}

func waitCheckpointRetry(ctx context.Context, attempt int) error {
	delay := time.Duration(attempt) * gvisorCheckpointRetryDelay
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *NvidiaCRIUManager) RestoreCheckpoint(ctx context.Context, rt runtime.Runtime, opts *RestoreOpts) (int, error) {
	bundlePath := filepath.Dir(opts.configPath)
	imagePath := filepath.Join(c.checkpointRoot, opts.checkpoint.CheckpointId)
	workDir := filepath.Join(types.AgentTmpPath, opts.checkpoint.CheckpointId, opts.request.ContainerId)
	preserveOpenTCP := shouldPreservePodTCPOnRestore(opts)

	// Setup work directory for restore files
	err := c.setupRestoreWorkDir(workDir)
	if err != nil {
		return -1, fmt.Errorf("failed to setup restore work directory: %w", err)
	}
	if preserveOpenTCP {
		if err := writeCheckpointNetworkMap(workDir, opts.checkpoint.ContainerIp, opts.containerIP); err != nil {
			return -1, fmt.Errorf("failed to configure checkpoint network restore: %w", err)
		}
	}

	attempts := 1
	if rt.Name() == types.ContainerRuntimeRunc.String() {
		attempts = nvidiaRestoreAttempts
	}

	var exitCode int
	for attempt := 1; attempt <= attempts; attempt++ {
		outputCapture := newRestoreOutputCapture(opts.outputWriter)

		exitCode, err = rt.Restore(ctx, opts.request.ContainerId, &runtime.RestoreOpts{
			ImagePath:    imagePath,
			WorkDir:      workDir,
			BundlePath:   bundlePath,
			OutputWriter: outputCapture,
			Started:      opts.started,
			AllowOpenTCP: preserveOpenTCP,
			TCPClose:     !preserveOpenTCP,
		})
		stderr := outputCapture.stop()
		if err == nil {
			break
		}

		restoreErr := classifyRestoreError(rt.Name(), err, stderr)
		if attempt == attempts || !IsCRIURestoreError(restoreErr) {
			return exitCode, restoreErr
		}
		if cleanupErr := deleteFailedRestoreRuntimeContainer(ctx, rt, opts.request.ContainerId); cleanupErr != nil {
			return exitCode, fmt.Errorf("%w; failed to clean up before retry: %v", restoreErr, cleanupErr)
		}

		log.Warn().
			Err(restoreErr).
			Str("runtime", rt.Name()).
			Str("checkpoint_id", opts.checkpoint.CheckpointId).
			Int("attempt", attempt).
			Msg("retrying checkpoint restore after CRIU failure")
		if err := waitNvidiaRestoreRetry(ctx); err != nil {
			return exitCode, err
		}
	}

	log.Info().
		Str("runtime", rt.Name()).
		Str("checkpoint_id", opts.checkpoint.CheckpointId).
		Str("image_path", imagePath).
		Str("work_dir", workDir).
		Msg("checkpoint restored successfully")

	return exitCode, nil
}

func classifyRestoreError(runtimeName string, err error, stderr string) error {
	if strings.Contains(stderr, "criu failed") && strings.Contains(stderr, "type RESTORE") {
		return &ErrCRIURestoreFailed{Stderr: stderr}
	}
	return restoreFailureError(runtimeName, err, stderr)
}

func waitNvidiaRestoreRetry(ctx context.Context) error {
	timer := time.NewTimer(nvidiaRestoreRetryDelay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func shouldPreservePodTCPOnRestore(opts *RestoreOpts) bool {
	if opts == nil {
		return false
	}

	return isPodRequest(opts.request)
}

func writeCheckpointNetworkMap(workDir, oldContainerIP, newContainerIP string) error {
	oldIPv4 := net.ParseIP(oldContainerIP).To4()
	newIPv4 := net.ParseIP(newContainerIP).To4()
	oldIPv6 := checkpointContainerIPv6(oldContainerIP)
	newIPv6 := checkpointContainerIPv6(newContainerIP)
	if oldIPv4 == nil || newIPv4 == nil || oldIPv6 == "" || newIPv6 == "" {
		return fmt.Errorf("invalid container IP mapping %q -> %q", oldContainerIP, newContainerIP)
	}

	contents := fmt.Sprintf("%s %s %s %s %s\n", checkpointNetworkMapVersion, oldIPv4.String(), newIPv4.String(), oldIPv6, newIPv6)
	return os.WriteFile(filepath.Join(workDir, checkpointNetworkMapFile), []byte(contents), 0600)
}

func restoreFailureError(runtimeName string, err error, stderr string) error {
	stderr = strings.TrimSpace(stderr)
	if stderr == "" || strings.Contains(err.Error(), "stderr:") {
		return fmt.Errorf("restore failed for runtime %s: %w", runtimeName, err)
	}
	if len(stderr) > maxRestoreStderrBytes {
		stderr = stderr[:maxRestoreStderrBytes] + "...[truncated]"
	}
	return fmt.Errorf("restore failed for runtime %s: %w (stderr: %s)", runtimeName, err, stderr)
}

func (c *NvidiaCRIUManager) Available() bool {
	return c.available
}

// getNvidiaDriverVersion returns the NVIDIA driver version as an integer
func getNvidiaDriverVersion() (int, error) {
	cmd := exec.Command("nvidia-smi", "--query-gpu=driver_version", "--format=csv,noheader")
	output, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("failed to execute nvidia-smi: %v", err)
	}

	versionStr := strings.TrimSpace(string(output))
	parts := strings.Split(versionStr, ".")
	if len(parts) < 1 {
		return 0, fmt.Errorf("unexpected driver version format: %s", versionStr)
	}

	majorVersion, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, fmt.Errorf("failed to parse driver version: %v", err)
	}

	return majorVersion, nil
}

func crCompatible(gpuCnt int) bool {
	if gpuCnt == 0 {
		return true
	}

	// Check NVIDIA driver version is >= 570
	driverVersion, err := getNvidiaDriverVersion()
	if err != nil {
		log.Error().Msgf("Failed to get NVIDIA driver version: %v", err)
		return false
	}

	if driverVersion < minNvidiaDriverVersion {
		log.Error().Msgf("NVIDIA driver version %d is less than required version 570", driverVersion)
		return false
	}

	return true
}

func (c *NvidiaCRIUManager) setupRestoreWorkDir(workDir string) error {
	if err := os.RemoveAll(workDir); err != nil {
		return err
	}
	return os.MkdirAll(workDir, 0755)
}
