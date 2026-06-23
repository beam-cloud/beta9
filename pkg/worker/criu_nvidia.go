package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/runtime"
	types "github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	minNvidiaDriverVersion     = 570
	gvisorCheckpointAttempts   = 4
	gvisorCheckpointRetryDelay = 250 * time.Millisecond
	maxRestoreStderrBytes      = 4096
)

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

func (c *NvidiaCRIUManager) CreateCheckpoint(ctx context.Context, rt runtime.Runtime, checkpointId string, request *types.ContainerRequest) (string, error) {
	checkpointPath := filepath.Join(c.checkpointRoot, checkpointId)
	workDir := filepath.Join(types.AgentTmpPath, checkpointId)

	// Setup work directory for checkpoint files
	if err := os.MkdirAll(workDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create checkpoint work directory: %w", err)
	}

	checkpointOpts := &runtime.CheckpointOpts{
		ImagePath:    checkpointPath,
		WorkDir:      workDir, // Required for checkpoint files (logs, cache, sockets)
		LeaveRunning: true,    // Keep container running (hot checkpoint)
		AllowOpenTCP: true,    // Allow open TCP connections
		SkipInFlight: true,    // Skip in-flight TCP packets
		LinkRemap:    true,    // Enable link remapping for file descriptors
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
	workDir := filepath.Join(types.AgentTmpPath, opts.checkpoint.CheckpointId)

	// Setup work directory for restore files
	err := c.setupRestoreWorkDir(workDir)
	if err != nil {
		return -1, fmt.Errorf("failed to setup restore work directory: %w", err)
	}

	// Create a buffer to capture stderr while still forwarding to the original writer
	var stderrBuf strings.Builder
	var outputWriter io.Writer = &stderrBuf

	if opts.outputWriter != nil {
		outputWriter = io.MultiWriter(opts.outputWriter, &stderrBuf)
	}

	// Restore with all required options for proper CUDA restore
	exitCode, err := rt.Restore(ctx, opts.request.ContainerId, &runtime.RestoreOpts{
		ImagePath:    imagePath,    // Path to checkpoint image
		WorkDir:      workDir,      // Working directory for restore files
		BundlePath:   bundlePath,   // Container bundle path
		OutputWriter: outputWriter, // Output writer for logs
		Started:      opts.started, // Channel to signal process start
		TCPClose:     true,         // Close TCP connections on restore
	})

	if err != nil {
		stderr := stderrBuf.String()

		// Check if this is a CRIU restore failure by looking for specific error patterns in stderr
		if strings.Contains(stderr, "criu failed") && strings.Contains(stderr, "type RESTORE") {
			return -1, &ErrCRIURestoreFailed{Stderr: stderr}
		}

		return exitCode, restoreFailureError(rt.Name(), err, stderr)
	}

	log.Info().
		Str("runtime", rt.Name()).
		Str("checkpoint_id", opts.checkpoint.CheckpointId).
		Str("image_path", imagePath).
		Str("work_dir", workDir).
		Msg("checkpoint restored successfully")

	return exitCode, nil
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
	if _, err := os.Stat(workDir); os.IsNotExist(err) {
		err := os.MkdirAll(workDir, 0755)
		if err != nil {
			return err
		}
	}

	return nil
}
