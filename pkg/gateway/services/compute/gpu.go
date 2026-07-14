package compute

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

func poolGPUConfig(gpus []string) ([]string, error) {
	gpu, err := singleGPUType(types.NormalizeGPUStrings(gpus), 0)
	if err != nil {
		return nil, fmt.Errorf("private pools require one GPU type: %w", err)
	}
	if gpu == "" {
		return nil, nil
	}
	return []string{gpu}, nil
}

func (s *Service) enforcePoolGPUType(ctx context.Context, pool *model.PoolState, agent *model.AgentTokenState) error {
	if pool == nil || agent == nil {
		return nil
	}

	machineGPU, err := singleGPUType(agent.GPUs, agent.GPUCount)
	if err != nil {
		return err
	}

	poolGPU := configuredPoolGPU(pool)
	if poolGPU == "" {
		if pool.ManagementSource != "" {
			return nil
		}
		return s.lockUnsetPoolGPU(ctx, pool, agent.WorkspaceID, machineGPU)
	}

	if machineGPU == "" {
		return fmt.Errorf("pool %q requires GPU type %q, but machine %q has no GPUs", pool.Name, poolGPU, agent.MachineID)
	}
	if machineGPU != poolGPU {
		return fmt.Errorf("pool %q requires GPU type %q, but machine %q reported %q", pool.Name, poolGPU, agent.MachineID, machineGPU)
	}
	return nil
}

func (s *Service) lockUnsetPoolGPU(ctx context.Context, pool *model.PoolState, workspaceID, machineGPU string) error {
	existingGPU, hasMachines, err := s.existingPoolGPUType(ctx, workspaceID, pool.Name)
	if err != nil {
		return err
	}
	if hasMachines {
		if existingGPU == "" {
			if machineGPU != "" {
				return fmt.Errorf("pool %q was initialized without GPUs; create a separate pool for GPU type %q", pool.Name, machineGPU)
			}
			return nil
		}
		if machineGPU == "" {
			return fmt.Errorf("pool %q requires GPU type %q, but this machine has no GPUs", pool.Name, existingGPU)
		}
		if machineGPU != existingGPU {
			return fmt.Errorf("pool %q requires GPU type %q, but this machine reported %q", pool.Name, existingGPU, machineGPU)
		}
		return s.setPoolGPU(ctx, pool, workspaceID, existingGPU)
	}
	if machineGPU == "" {
		return nil
	}
	return s.setPoolGPU(ctx, pool, workspaceID, machineGPU)
}

func (s *Service) existingPoolGPUType(ctx context.Context, workspaceID, poolName string) (string, bool, error) {
	if s == nil || s.computeRepo == nil {
		return "", false, nil
	}
	machines, err := s.computeRepo.ListAgentTokenStates(ctx, workspaceID, poolName)
	if err != nil {
		return "", false, err
	}

	var gpu string
	hasMachines := false
	for _, machine := range machines {
		if machine == nil {
			continue
		}
		hasMachines = true
		machineGPU, err := singleGPUType(machine.GPUs, machine.GPUCount)
		if err != nil {
			return "", true, err
		}
		if machineGPU == "" {
			continue
		}
		if gpu == "" {
			gpu = machineGPU
			continue
		}
		if gpu != machineGPU {
			return "", true, fmt.Errorf("pool %q already has mixed GPU types %s and %s", poolName, gpu, machineGPU)
		}
	}
	return gpu, hasMachines, nil
}

func (s *Service) setPoolGPU(ctx context.Context, pool *model.PoolState, workspaceID, gpu string) error {
	config := normalizePoolConfig(pool.Config)
	if config == nil {
		config = &pb.PoolConfig{Name: pool.Name, Selector: pool.Selector}
	}
	config.Gpu = []string{gpu}
	pool.Config = config
	pool.UpdatedAt = time.Now()
	return s.savePrivatePoolState(ctx, workspaceID, pool)
}

func (s *Service) ValidatePrivatePoolGPURequest(ctx context.Context, workspaceID, poolName string, gpus []types.GpuType) error {
	requested := concreteGPUTypes(gpus)
	if s == nil || s.computeRepo == nil || workspaceID == "" || poolName == "" || len(requested) == 0 {
		return nil
	}

	pool, err := s.getPrivatePoolState(ctx, workspaceID, poolName)
	if err != nil || pool == nil {
		return err
	}

	poolGPU := configuredPoolGPU(pool)
	if poolGPU == "" || slices.Contains(requested, poolGPU) {
		return nil
	}
	return fmt.Errorf("pool %q is configured for GPU type %q, but this workload requests %s", poolName, poolGPU, strings.Join(requested, ", "))
}

func (s *Service) MissingPrivatePoolGPUWarning(ctx context.Context, workspaceID string, gpus []types.GpuType) (string, error) {
	requested := concreteGPUTypes(gpus)
	if s == nil || s.computeRepo == nil || workspaceID == "" || len(requested) == 0 {
		return "", nil
	}

	pools, err := s.listPrivatePoolStates(ctx, workspaceID, 0)
	if err != nil {
		return "", err
	}

	configured := make([]string, 0, len(pools))
	for _, pool := range pools {
		gpu := configuredPoolGPU(pool)
		if gpu == "" {
			continue
		}
		if slices.Contains(requested, gpu) {
			return "", nil
		}
		if !slices.Contains(configured, gpu) {
			configured = append(configured, gpu)
		}
	}

	requestedLabel := strings.Join(requested, ", ")
	if len(configured) == 0 {
		return fmt.Sprintf("GPU type %s is not available, and no private pool in this workspace is configured for it.", requestedLabel), nil
	}
	return fmt.Sprintf("GPU type %s is not available. Private pools in this workspace are configured for %s.", requestedLabel, strings.Join(configured, ", ")), nil
}

func configuredPoolGPU(pool *model.PoolState) string {
	if pool == nil || pool.Config == nil {
		return ""
	}
	for _, value := range pool.Config.Gpu {
		normalized := types.NormalizeGPUType(value)
		if normalized != "" && normalized != types.NO_GPU {
			return normalized.String()
		}
	}
	return ""
}

func concreteGPUTypes(gpus []types.GpuType) []string {
	out := make([]string, 0, len(gpus))
	for _, gpu := range gpus {
		normalized := types.NormalizeGPUType(string(gpu))
		if normalized == "" || normalized == types.NO_GPU {
			continue
		}
		if normalized == types.GPU_ANY {
			return nil
		}
		value := normalized.String()
		if !slices.Contains(out, value) {
			out = append(out, value)
		}
	}
	return out
}

func singleGPUType(gpus []string, gpuCount uint32) (string, error) {
	var gpu string
	seen := []string{}
	for _, value := range gpus {
		normalized := types.NormalizeGPUType(value)
		if normalized == "" || normalized == types.NO_GPU {
			continue
		}
		current := normalized.String()
		if !slices.Contains(seen, current) {
			seen = append(seen, current)
		}
		if gpu == "" {
			gpu = current
			continue
		}
		if current != gpu {
			return "", fmt.Errorf("machine has mixed GPU types %s", strings.Join(seen, ", "))
		}
	}
	if gpuCount > 0 && gpu == "" {
		return "", fmt.Errorf("machine reported GPU capacity without a GPU type")
	}
	return gpu, nil
}
