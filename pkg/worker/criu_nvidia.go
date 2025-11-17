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

	"github.com/beam-cloud/beta9/pkg/runtime"
	types "github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	minNvidiaDriverVersion = 570
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
	cpStorageConfig types.CheckpointStorageConfig
	gpuCnt          int
	available       bool
}

func InitializeNvidiaCRIU(ctx context.Context, config types.CRIUConfig) (CRIUManager, error) {
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

	return &NvidiaCRIUManager{cpStorageConfig: config.Storage, gpuCnt: gpuCnt, available: available}, nil
}

func (c *NvidiaCRIUManager) CreateCheckpoint(ctx context.Context, rt runtime.Runtime, checkpointId string, request *types.ContainerRequest) (string, error) {
	checkpointPath := fmt.Sprintf("%s/%s", c.cpStorageConfig.MountPath, checkpointId)
	workDir := filepath.Join("/tmp", checkpointId)
	
	// Setup work directory for checkpoint files
	if err := os.MkdirAll(workDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create checkpoint work directory: %w", err)
	}
	
	// Create checkpoint with all required options for proper CUDA checkpoint
	err := rt.Checkpoint(ctx, request.ContainerId, &runtime.CheckpointOpts{
		ImagePath:    checkpointPath,
		WorkDir:      workDir,        // Required for checkpoint files (logs, cache, sockets)
		LeaveRunning: true,            // Keep container running (hot checkpoint)
		AllowOpenTCP: true,            // Allow open TCP connections
		SkipInFlight: true,            // Skip in-flight TCP packets
		LinkRemap:    true,            // Enable link remapping for file descriptors
	})
	if err != nil {
		return "", fmt.Errorf("checkpoint failed for runtime %s: %w", rt.Name(), err)
	}

	log.Info().
		Str("runtime", rt.Name()).
		Str("checkpoint_id", checkpointId).
		Str("checkpoint_path", checkpointPath).
		Str("work_dir", workDir).
		Msg("checkpoint created successfully")
	
	return checkpointPath, nil
}

func (c *NvidiaCRIUManager) RestoreCheckpoint(ctx context.Context, rt runtime.Runtime, opts *RestoreOpts) (int, error) {
	bundlePath := filepath.Dir(opts.configPath)
	imagePath := filepath.Join(c.cpStorageConfig.MountPath, opts.checkpoint.CheckpointId)
	workDir := filepath.Join("/tmp", opts.checkpoint.CheckpointId)
	
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
		ImagePath:    imagePath,                // Path to checkpoint image
		WorkDir:      workDir,                  // Working directory for restore files
		BundlePath:   bundlePath,               // Container bundle path
		OutputWriter: outputWriter,             // Output writer for logs
		Started:      opts.started,             // Channel to signal process start
		TCPClose:     true,                     // Close TCP connections on restore
	})

	if err != nil {
		stderr := stderrBuf.String()

		// Check if this is a CRIU restore failure by looking for specific error patterns in stderr
		if strings.Contains(stderr, "criu failed") && strings.Contains(stderr, "type RESTORE") {
			return -1, &ErrCRIURestoreFailed{Stderr: stderr}
		}

		return exitCode, fmt.Errorf("restore failed for runtime %s: %w", rt.Name(), err)
	}

	log.Info().
		Str("runtime", rt.Name()).
		Str("checkpoint_id", opts.checkpoint.CheckpointId).
		Str("image_path", imagePath).
		Str("work_dir", workDir).
		Msg("checkpoint restored successfully")
	
	return exitCode, nil
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
