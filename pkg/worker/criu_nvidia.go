package worker

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	types "github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/go-runc"
	"github.com/rs/zerolog/log"
)

const (
	CacheWorkerPoolSize    = 20
	minNvidiaDriverVersion = 570
)

type NvidiaCRIUManager struct {
	runcHandle      runc.Runc
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

	runcHandle := runc.Runc{}
	return &NvidiaCRIUManager{runcHandle: runcHandle, cpStorageConfig: config.Storage, gpuCnt: gpuCnt, available: available}, nil
}

func (c *NvidiaCRIUManager) CreateCheckpoint(ctx context.Context, request *types.ContainerRequest) (string, error) {
	// Create a temporary directory for the checkpoint
	tempDir, err := os.MkdirTemp("", fmt.Sprintf("checkpoint-%s-", request.StubId))
	if err != nil {
		return "", fmt.Errorf("failed to create temp directory: %v", err)
	}

	// Create checkpoint in temp directory
	err = c.runcHandle.Checkpoint(ctx, request.ContainerId, &runc.CheckpointOpts{
		LeaveRunning: true,
		SkipInFlight: true,
		AllowOpenTCP: true,
		LinkRemap:    true,
		ImagePath:    tempDir,
	})
	if err != nil {
		// Clean up temp directory on error
		os.RemoveAll(tempDir)
		return "", err
	}

	// Final destination path
	finalPath := fmt.Sprintf("%s/%s", c.cpStorageConfig.MountPath, request.StubId)

	// Copy checkpoint to final location asynchronously
	go func() {
		if err := copyDir(tempDir, finalPath); err != nil {
			log.Error().Err(err).
				Str("temp_dir", tempDir).
				Str("final_path", finalPath).
				Str("stub_id", request.StubId).
				Msg("failed to copy checkpoint from temp directory to final location")
		} else {
			log.Info().
				Str("temp_dir", tempDir).
				Str("final_path", finalPath).
				Str("stub_id", request.StubId).
				Msg("checkpoint copied from temp directory to final location")
		}
		// Clean up temp directory after copy
		os.RemoveAll(tempDir)
	}()

	// Return the final path immediately (the copy is happening asynchronously)
	return finalPath, nil
}

func (c *NvidiaCRIUManager) RestoreCheckpoint(ctx context.Context, opts *RestoreOpts) (int, error) {
	bundlePath := filepath.Dir(opts.configPath)
	imagePath := filepath.Join(c.cpStorageConfig.MountPath, opts.request.StubId)

	// Create a temporary directory for the restore operation
	tempDir, err := os.MkdirTemp("", fmt.Sprintf("restore-%s-", opts.request.StubId))
	if err != nil {
		return -1, fmt.Errorf("failed to create temp directory for restore: %v", err)
	}
	defer os.RemoveAll(tempDir) // Clean up temp directory when done

	// Copy checkpoint from final location to temp directory
	err = copyDir(imagePath, tempDir)
	if err != nil {
		return -1, fmt.Errorf("failed to copy checkpoint to temp directory: %v", err)
	}

	exitCode, err := c.runcHandle.Restore(ctx, opts.request.ContainerId, bundlePath, &runc.RestoreOpts{
		CheckpointOpts: runc.CheckpointOpts{
			AllowOpenTCP: true,
			ImagePath:    tempDir,
			OutputWriter: opts.runcOpts.OutputWriter,
		},
		Started: opts.runcOpts.Started,
	})
	if err != nil {
		return -1, err
	}

	return exitCode, nil
}

// copyDir recursively copies a directory from src to dst
func copyDir(src, dst string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())

		if entry.IsDir() {
			// Create destination directory
			if err := os.MkdirAll(dstPath, 0755); err != nil {
				return err
			}
			// Recursively copy subdirectory
			if err := copyDir(srcPath, dstPath); err != nil {
				return err
			}
		} else {
			// Copy file
			if err := copyFile(srcPath, dstPath); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *NvidiaCRIUManager) Available() bool {
	return c.available
}

func (c *NvidiaCRIUManager) Run(ctx context.Context, request *types.ContainerRequest, bundlePath string, runcOpts *runc.CreateOpts) (int, error) {
	exitCode, err := c.runcHandle.Run(ctx, request.ContainerId, bundlePath, runcOpts)
	if err != nil {
		return -1, err
	}

	return exitCode, nil
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
