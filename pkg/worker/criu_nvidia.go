package worker

import (
	"context"
	"fmt"
	"path/filepath"

	types "github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/go-runc"
)

type NvidiaCRIUManager struct {
	cpStorageConfig types.CheckpointStorageConfig
	runcHandle      runc.Runc
}

func InitializeNvidiaCRIU(ctx context.Context, config types.CRIUConfig) (CRIUManager, error) {
	runcHandle := runc.Runc{}
	return &NvidiaCRIUManager{runcHandle: runcHandle, cpStorageConfig: config.Storage}, nil
}

func (c *NvidiaCRIUManager) CreateCheckpoint(ctx context.Context, request *types.ContainerRequest) (string, error) {
	checkpointPath := fmt.Sprintf("%s/%s", c.cpStorageConfig.MountPath, request.StubId)
	err := c.runcHandle.Checkpoint(ctx, request.ContainerId, &runc.CheckpointOpts{
		LeaveRunning: true,
		SkipInFlight: true,
		AllowOpenTCP: true,
		LinkRemap:    true,
		ImagePath:    checkpointPath,
	})
	if err != nil {
		return "", err
	}

	return checkpointPath, nil
}

func (c *NvidiaCRIUManager) RestoreCheckpoint(ctx context.Context, opts *RestoreOpts) (int, error) {
	bundlePath := filepath.Dir(opts.configPath)
	imagePath := fmt.Sprintf("%s/%s", c.cpStorageConfig.MountPath, opts.request.StubId)

	exitCode, err := c.runcHandle.Restore(ctx, opts.request.ContainerId, bundlePath, &runc.RestoreOpts{
		CheckpointOpts: runc.CheckpointOpts{
			ImagePath:    imagePath,
			OutputWriter: opts.runcOpts.OutputWriter,
		},
		Started: opts.runcOpts.Started,
	})
	if err != nil {
		return -1, err
	}

	return exitCode, nil
}

func (c *NvidiaCRIUManager) Available() bool {
	// TODO: check for correct version of criu, correct driver version, etc.
	return true
}

func (c *NvidiaCRIUManager) Run(ctx context.Context, request *types.ContainerRequest, bundlePath string, runcOpts *runc.CreateOpts) (int, error) {
	exitCode, err := c.runcHandle.Run(ctx, request.ContainerId, bundlePath, runcOpts)
	if err != nil {
		return -1, err
	}

	return exitCode, nil
}
