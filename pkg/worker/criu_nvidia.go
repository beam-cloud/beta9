package worker

import (
	"context"

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
	err := c.runcHandle.Checkpoint(ctx, request.ContainerId, &runc.CheckpointOpts{})
	if err != nil {
		return "", err
	}

	return "", nil
}

func (c *NvidiaCRIUManager) RestoreCheckpoint(ctx context.Context, request *types.ContainerRequest, state types.CheckpointState, runcOpts *runc.CreateOpts) (int, error) {
	exitCode, err := c.runcHandle.Restore(ctx, request.ContainerId, "my-bundle", &runc.RestoreOpts{})
	if err != nil {
		return -1, err
	}

	return exitCode, nil
}

func (c *NvidiaCRIUManager) Available() bool {
	// TODO: check for correct version of criu, correct driver version, etc.
	return false
}

func (c *NvidiaCRIUManager) Run(ctx context.Context, request *types.ContainerRequest, bundlePath string, runcOpts *runc.CreateOpts) (chan int, error) {
	_, err := c.runcHandle.Run(ctx, request.ContainerId, bundlePath, runcOpts)
	if err != nil {
		return nil, err
	}

	return nil, nil
}
