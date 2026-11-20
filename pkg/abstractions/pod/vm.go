package pod

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

// vmStub reads the stub of a persistent VM and its configuration.
func (s *GenericPodService) vmStub(ctx context.Context, stubID string) (*types.StubWithRelated, types.StubConfigV1, error) {
	var spec types.StubConfigV1
	stub, err := s.backendRepo.GetStubByExternalId(ctx, stubID)
	if err != nil {
		return nil, spec, err
	}

	if stub == nil {
		return nil, spec, fmt.Errorf("VM stub not found")
	}

	if err := json.Unmarshal([]byte(stub.Config), &spec); err != nil {
		return nil, spec, err
	}

	if !spec.IsPersistentVM() || !stub.Type.IsSandbox() {
		return nil, spec, fmt.Errorf("persistent VM requires a microvm stub")
	}
	if spec.Runtime.GpuCount > 1 || spec.RequiresGPU() && spec.Runtime.GpuCount != 1 {
		return nil, spec, fmt.Errorf("microvm GPU requests must specify exactly one GPU")
	}

	return stub, spec, nil
}

// CheckVM asks the scheduler whether it would let a VM start now, and makes
// no request of it: no container is named, nothing is reserved and nothing
// is written. It answers as RunVM would for a workspace that is out of
// credit or that runs as much as its concurrency limit allows. What it
// allows can be gone by the time RunVM asks for it.
func (s *GenericPodService) CheckVM(ctx context.Context, info *auth.AuthInfo, stubID string) error {
	stub, spec, err := s.vmStub(ctx, stubID)
	if err != nil {
		return err
	}

	workspace, err := podRunWorkspace(info, stub)
	if err != nil {
		return err
	}

	return s.scheduler.CheckConcurrencyLimit(&types.ContainerRequest{
		StubId:       stub.ExternalId,
		Stub:         *stub,
		WorkspaceId:  workspace.ExternalId,
		Workspace:    *workspace,
		Cpu:          spec.Runtime.Cpu,
		GpuCount:     spec.Runtime.GpuCount,
		PoolSelector: spec.PoolSelector(),
	})
}

// RunVM reuses sandbox scheduling while preserving the VM service's reserved
// runtime identity. Never allow a VM to fall back to a container runtime.
func (s *GenericPodService) RunVM(ctx context.Context, info *auth.AuthInfo, stubID, containerID string, vmSpec types.VMSpec, checkpointID string, checkpointDisks map[string]string) error {
	stub, spec, err := s.vmStub(ctx, stubID)
	if err != nil {
		return err
	}

	spec.Ports = vmSpec.RuntimePorts()
	spec.BlockNetwork = vmSpec.BlockNetwork
	spec.AllowList = vmSpec.AllowList
	var checkpoint *types.Checkpoint
	if checkpointID != "" {
		checkpoint, err = s.backendRepo.GetCheckpointById(ctx, checkpointID)
		if err != nil {
			return err
		}

		if checkpoint.WorkspaceId != info.Workspace.Id || checkpoint.StubId != stub.Id || checkpoint.Runtime != types.ContainerRuntimeMicroVM.String() || checkpoint.Status != string(types.CheckpointStatusAvailable) {
			return fmt.Errorf("VM memory checkpoint is unavailable or incompatible")
		}

		// These launch-only snapshot IDs already travel in durable-disk mounts
		// through worker RPCs. Warm VM mounts interpret them as exact heads.
		for _, disk := range spec.Disks {
			paired := checkpointDisks[disk.Name]
			if paired == "" {
				return fmt.Errorf("VM memory checkpoint has no paired disk %s", disk.Name)
			}

			disk.SourceSnapshotId = paired
		}
	}

	data, err := json.Marshal(spec)
	if err != nil {
		return err
	}

	stub.Config = string(data)
	_, err = s.run(ctx, info, stub, runOptions{containerId: containerID, checkpoint: checkpoint})
	return err
}

// Reuse the proxy's actual backend route, including private worker transport.
func (s *GenericPodService) VMPortReady(ctx context.Context, stubID, containerID string, port uint32) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	instance, err := s.getOrCreatePodInstance(stubID)
	if err != nil {
		return false, err
	}

	return instance.buffer.primeContainerPort(containerID, int32(port), time.Second), nil
}

func (s *GenericPodService) ForwardVM(ctx echo.Context, stubID, containerID string) error {
	return s.forwardContainerRequest(ctx, stubID, containerID)
}
