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

func (c *NvidiaCRIUManager) CreateCheckpoint(ctx context.Context, request *types.ContainerRequest, onComplete func(request *types.ContainerRequest, err error)) (string, error) {
	checkpointDir := fmt.Sprintf("/tmp/checkpoint-%s", request.StubId) // Create a temporary directory for the checkpoint
	os.RemoveAll(checkpointDir)
	os.MkdirAll(checkpointDir, 0700)

	// Create checkpoint in temp directory
	err := c.runcHandle.Checkpoint(ctx, request.ContainerId, &runc.CheckpointOpts{
		AllowOpenTCP: true,
		LeaveRunning: true,
		SkipInFlight: true,
		LinkRemap:    true,
		ImagePath:    checkpointPath,
		Cgroups:      runc.Soft,
	})
	if err != nil {
		os.RemoveAll(checkpointDir)
		return "", err
	}

	// Create archive and store it asynchronously
	archivePath := fmt.Sprintf("%s/%s.tar", c.cpStorageConfig.MountPath, request.StubId)
	go func() {
		localArchive := fmt.Sprintf("/tmp/%s.tar", request.StubId)
		err := createTar(checkpointDir, localArchive)
		if err != nil {
			log.Error().Err(err).
				Str("temp_dir", checkpointDir).
				Str("local_archive", localArchive).
				Str("stub_id", request.StubId).
				Msg("failed to create checkpoint tarball")
			os.RemoveAll(checkpointDir)
			onComplete(request, err)
			return
		}

		// Copy to distributed filesystem
		err = copyFile(localArchive, archivePath)
		if err != nil {
			log.Error().Err(err).
				Str("local_archive", localArchive).
				Str("archive_path", archivePath).
				Str("stub_id", request.StubId).
				Msg("failed to copy checkpoint tarball to storage")
		} else {
			log.Info().
				Str("temp_dir", checkpointDir).
				Str("archive_path", archivePath).
				Str("stub_id", request.StubId).
				Msg("checkpoint tarballed and stored")
		}

		// Clean up
		os.Remove(localArchive)
		os.RemoveAll(checkpointDir)
		onComplete(request, err)
	}()

	return archivePath, nil
}

func (c *NvidiaCRIUManager) RestoreCheckpoint(ctx context.Context, opts *RestoreOpts) (int, error) {
	bundlePath := filepath.Dir(opts.configPath)
	tarballPath := fmt.Sprintf("%s/%s.tar", c.cpStorageConfig.MountPath, opts.request.StubId)

	exitCode, err := c.runcHandle.Restore(ctx, opts.request.ContainerId, bundlePath, &runc.RestoreOpts{
		CheckpointOpts: runc.CheckpointOpts{
			AllowOpenTCP: true,
			LinkRemap:    true,
			// Logs, irmap cache, sockets for lazy server and other go to working dir
			WorkDir:      workDir,
			ImagePath:    imagePath,
			OutputWriter: opts.runcOpts.OutputWriter,
			Cgroups:      runc.Soft,
		},
		Started: opts.runcOpts.Started,
	})
	if err != nil {
		return -1, err
	}

	return exitCode, nil
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
