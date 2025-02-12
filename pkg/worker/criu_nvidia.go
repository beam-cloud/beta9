package worker

import (
	"context"
	"fmt"
	"path/filepath"

	types "github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/go-runc"
)

type NvidiaCRIUManager struct {
	runcHandle runc.Runc
}

func InitializeNvidiaCRIU(ctx context.Context, config types.NvidiaCRIUConfig) (CRIUManager, error) {
	runcHandle := runc.Runc{}
	return &NvidiaCRIUManager{runcHandle: runcHandle}, nil
}

func (c *NvidiaCRIUManager) CreateCheckpoint(ctx context.Context, request *types.ContainerRequest) (string, error) {
	// TODO: actually implement this with sensible args
	checkpointPath := fmt.Sprintf("/checkpoints/%s", request.StubId)
	// FIXME: wtf is wrong with this?
	// cmd below does not work correctly either?
	err := c.runcHandle.Checkpoint(ctx, request.ContainerId, &runc.CheckpointOpts{
		LeaveRunning: true,
		SkipInFlight: true,
		AllowOpenTCP: true,
		LinkRemap:    true,
		ImagePath:    checkpointPath,
	})
	// err := exec.Command("runc", "checkpoint", "--leave-running", "--tcp-skip-in-flight", "--link-remap", "--tcp-established", "--image-path", checkpointPath, request.ContainerId).Run()
	if err != nil {
		return "", err
	}

	return checkpointPath, nil
}

func (c *NvidiaCRIUManager) RestoreCheckpoint(ctx context.Context, opts *RestoreOpts) (int, error) {
	bundlePath := filepath.Dir(opts.configPath)
	exitCode, err := c.runcHandle.Restore(ctx, opts.request.ContainerId, bundlePath, &runc.RestoreOpts{
		CheckpointOpts: runc.CheckpointOpts{
			ImagePath: fmt.Sprintf("/checkpoints/%s", opts.request.StubId),
		},
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
