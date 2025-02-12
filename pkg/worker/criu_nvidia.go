package worker

import (
	"context"

	types "github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/go-runc"
)

type NvidiaCRIUManager struct {
}

func InitializeNvidiaCRIU(ctx context.Context, config types.NvidiaCRIUConfig) (CRIUManager, error) {
	return nil, nil
}

func (c *NvidiaCRIUManager) CreateCheckpoint(ctx context.Context, request *types.ContainerRequest) (string, error) {
	return "", nil
}

func (c *NvidiaCRIUManager) RestoreCheckpoint(ctx context.Context, request *types.ContainerRequest, state types.CheckpointState, runcOpts *runc.CreateOpts) (chan int, string, error) {
	return nil, "", nil
}

func (c *NvidiaCRIUManager) Run(ctx context.Context, containerId string, bundle string, gpuEnabled bool, runcOpts *runc.CreateOpts) (chan int, error) {
	return nil, nil
}

func (c *NvidiaCRIUManager) Available() bool {
	return false
}
