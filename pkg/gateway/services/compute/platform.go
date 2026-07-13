package compute

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
)

type PlatformMachineBootstrap struct {
	MachineID      string
	PoolName       string
	Token          string
	InstallCommand string
	ExpiresAt      time.Time
}

func requirePlatformOperator(authInfo *auth.AuthInfo) error {
	if !auth.IsPlatformOperator(authInfo) {
		return model.ErrPlatformPermissionDenied
	}
	return nil
}

func (s *Service) platformWorkspace(ctx context.Context) (*types.Workspace, error) {
	if s == nil || s.backendRepo == nil {
		return nil, fmt.Errorf("platform workspace repository is unavailable")
	}
	workspace, err := s.backendRepo.GetAdminWorkspace(ctx)
	if err != nil {
		return nil, err
	}
	if workspace == nil || strings.TrimSpace(workspace.ExternalId) == "" {
		return nil, fmt.Errorf("platform workspace is unavailable")
	}
	return workspace, nil
}

func normalizePlatformWorkerPoolConfig(config types.WorkerPoolConfig) (types.WorkerPoolConfig, error) {
	if config.Mode == "" {
		config.Mode = types.PoolModeExternal
	}
	if config.Mode != types.PoolModeExternal {
		return types.WorkerPoolConfig{}, fmt.Errorf("dashboard-managed pools must use mode %q", types.PoolModeExternal)
	}
	if config.Provider != nil {
		return types.WorkerPoolConfig{}, fmt.Errorf("dashboard-managed pools are agent-backed and cannot set provider")
	}
	if config.ContainerRuntime == "" {
		config.ContainerRuntime = types.ContainerRuntimeRunc.String()
	}
	switch config.ContainerRuntime {
	case types.ContainerRuntimeRunc.String(), types.ContainerRuntimeGvisor.String():
	default:
		return types.WorkerPoolConfig{}, fmt.Errorf("unsupported container runtime %q", config.ContainerRuntime)
	}
	if len(config.UserData) > 256*1024 {
		return types.WorkerPoolConfig{}, fmt.Errorf("user data exceeds 256 KiB")
	}
	return config, nil
}

func platformPoolProtoConfig(name string, config types.WorkerPoolConfig) *pb.PoolConfig {
	pool := &pb.PoolConfig{
		Name:      name,
		Selector:  name,
		Mode:      string(types.PoolModeExternal),
		Transport: defaultPrivateTransport,
		Fallback:  defaultPrivateFallback,
		Priority:  config.Priority,
	}
	if config.GPUType != "" {
		pool.Gpu = []string{config.GPUType}
	}
	return pool
}

func newPlatformPoolState(workspaceID, name, source, createdBy string, config types.WorkerPoolConfig, now time.Time) *model.PoolState {
	configCopy := config
	return &model.PoolState{
		WorkspaceID:        workspaceID,
		Name:               name,
		Selector:           name,
		Config:             platformPoolProtoConfig(name, config),
		Status:             "active",
		Source:             model.SourceAttached,
		Mode:               string(types.PoolModeExternal),
		Transport:          defaultPrivateTransport,
		Fallback:           defaultPrivateFallback,
		Priority:           config.Priority,
		Preemptible:        config.Preemptable,
		CreatedByTokenID:   createdBy,
		CreatedAt:          now,
		UpdatedAt:          now,
		PlatformManaged:    true,
		PlatformSource:     source,
		PlatformInstanceID: uuid.NewString(),
		WorkerConfig:       &configCopy,
	}
}

// ReconcilePlatformPools materializes config-defined agent pools in the same
// state store as dashboard-created pools and restores dynamic controllers
// after a gateway restart. It never converts local or provider-backed pools.
func (s *Service) ReconcilePlatformPools(ctx context.Context) error {
	if s == nil || s.computeRepo == nil {
		return nil
	}
	workspace, err := s.platformWorkspace(ctx)
	if err != nil {
		return err
	}
	workspaceID := workspace.ExternalId
	now := time.Now().UTC()

	for name, config := range s.appConfig.Worker.Pools {
		if config.Mode != types.PoolModeExternal || config.Provider != nil {
			continue
		}
		config, err = normalizePlatformWorkerPoolConfig(config)
		if err != nil {
			return fmt.Errorf("platform pool %q: %w", name, err)
		}
		err = s.withPoolStateLock(ctx, workspaceID, name, func() error {
			existing, err := s.computeRepo.GetPoolState(ctx, workspaceID, name)
			if err != nil {
				return err
			}
			if existing != nil && (!existing.PlatformManaged || existing.PlatformSource == model.PlatformPoolSourceDashboard) {
				return fmt.Errorf("config-defined platform pool %q conflicts with persisted pool state", name)
			}
			state := newPlatformPoolState(workspaceID, name, model.PlatformPoolSourceConfig, "platform-config", config, now)
			if existing != nil {
				state.CreatedAt = existing.CreatedAt
				state.CreatedByTokenID = existing.CreatedByTokenID
				if existing.PlatformInstanceID != "" {
					state.PlatformInstanceID = existing.PlatformInstanceID
				}
			}
			return s.computeRepo.SavePoolState(ctx, workspaceID, state)
		})
		if err != nil {
			return err
		}
	}

	states, err := s.computeRepo.ListPoolStates(ctx, workspaceID, 0)
	if err != nil {
		return err
	}
	var reconcileErrors []error
	for _, state := range states {
		if state == nil || !state.PlatformManaged || state.WorkerConfig == nil {
			continue
		}
		if state.PlatformInstanceID == "" {
			if err := s.withPoolStateLock(ctx, workspaceID, state.Name, func() error {
				current, err := s.computeRepo.GetPoolState(ctx, workspaceID, state.Name)
				if err != nil || current == nil {
					return err
				}
				if current.PlatformInstanceID == "" {
					current.PlatformInstanceID = uuid.NewString()
					current.UpdatedAt = now
					if err := s.computeRepo.SavePoolState(ctx, workspaceID, current); err != nil {
						return err
					}
				}
				state = current
				return nil
			}); err != nil {
				reconcileErrors = append(reconcileErrors, fmt.Errorf("assign platform pool instance %q: %w", state.Name, err))
				continue
			}
		}
		configured, static := s.appConfig.Worker.Pools[state.Name]
		switch state.PlatformSource {
		case model.PlatformPoolSourceConfig:
			active := static && configured.Mode == types.PoolModeExternal && configured.Provider == nil
			if !active {
				inUse, inventoryErr := s.platformPoolHasInventory(ctx, state)
				if inventoryErr != nil {
					reconcileErrors = append(reconcileErrors, fmt.Errorf("inspect stale config platform pool %q: %w", state.Name, inventoryErr))
					continue
				}
				if inUse {
					reconcileErrors = append(reconcileErrors, fmt.Errorf("config platform pool %q was removed or changed while it still has inventory", state.Name))
					continue
				}
				if s.workerPoolRepo != nil {
					if deleteErr := s.workerPoolRepo.DeleteWorkerPoolState(ctx, state.Name); deleteErr != nil {
						reconcileErrors = append(reconcileErrors, fmt.Errorf("delete stale worker pool state %q: %w", state.Name, deleteErr))
						continue
					}
				}
				if deleteErr := s.computeRepo.DeletePoolState(ctx, workspaceID, state.Name); deleteErr != nil {
					reconcileErrors = append(reconcileErrors, fmt.Errorf("delete stale config platform pool %q: %w", state.Name, deleteErr))
				}
				continue
			}
		case model.PlatformPoolSourceDashboard:
			if static {
				reconcileErrors = append(reconcileErrors, fmt.Errorf("dashboard platform pool %q conflicts with config", state.Name))
				continue
			}
		default:
			reconcileErrors = append(reconcileErrors, fmt.Errorf("platform pool %q has invalid source %q", state.Name, state.PlatformSource))
			continue
		}
		if s.scheduler != nil {
			if err := s.scheduler.RegisterAgentPool(workspaceID, state); err != nil {
				reconcileErrors = append(reconcileErrors, fmt.Errorf("register platform pool %q: %w", state.Name, err))
			}
		}
	}
	return errors.Join(reconcileErrors...)
}

func (s *Service) platformPoolState(ctx context.Context, name string) (*model.PoolState, error) {
	workspace, err := s.platformWorkspace(ctx)
	if err != nil {
		return nil, err
	}
	state, err := s.computeRepo.GetPoolState(ctx, workspace.ExternalId, name)
	if err != nil {
		return nil, err
	}
	if state == nil || !state.PlatformManaged || state.Mode != string(types.PoolModeExternal) || state.WorkerConfig == nil {
		return nil, nil
	}
	return state, nil
}

func (s *Service) platformPoolView(ctx context.Context, state *model.PoolState) (*model.PlatformPool, error) {
	if state == nil || state.WorkerConfig == nil {
		return nil, model.ErrPlatformPoolNotFound
	}
	view := &model.PlatformPool{
		Name:       state.Name,
		Config:     *state.WorkerConfig,
		Source:     state.PlatformSource,
		Controller: model.PlatformPoolControllerAgent,
		Editable:   state.PlatformSource == model.PlatformPoolSourceDashboard,
		CreatedAt:  state.CreatedAt,
		UpdatedAt:  state.UpdatedAt,
	}
	if s.workerPoolRepo != nil {
		poolState, err := s.workerPoolRepo.GetWorkerPoolState(ctx, state.Name)
		if err == nil {
			view.State = poolState
		}
	}
	machines, err := s.computeRepo.ListAgentTokenStates(ctx, state.WorkspaceID, state.Name)
	if err != nil {
		return nil, err
	}
	view.MachineCount = len(machines)
	for _, machine := range machines {
		if machine != nil && machine.Schedulable && model.AgentMachineConnected(machine, time.Now().UTC()) {
			view.ReadyMachineCount++
		}
	}
	return view, nil
}

func configuredPlatformPoolController(config types.WorkerPoolConfig) string {
	switch {
	case config.Mode == types.PoolModeLocal:
		return model.PlatformPoolControllerLocal
	case config.Mode == types.PoolModeExternal && config.Provider == nil:
		return model.PlatformPoolControllerAgent
	case config.Mode == types.PoolModeExternal:
		return model.PlatformPoolControllerExternalLegacy
	default:
		return model.PlatformPoolControllerAgent
	}
}

func (s *Service) ListPlatformPools(ctx context.Context, authInfo *auth.AuthInfo) ([]*model.PlatformPool, error) {
	if err := requirePlatformOperator(authInfo); err != nil {
		return nil, err
	}
	workspace, err := s.platformWorkspace(ctx)
	if err != nil {
		return nil, err
	}

	views := make(map[string]*model.PlatformPool, len(s.appConfig.Worker.Pools))
	for name, config := range s.appConfig.Worker.Pools {
		view := &model.PlatformPool{
			Name:       name,
			Config:     config,
			Source:     model.PlatformPoolSourceConfig,
			Controller: configuredPlatformPoolController(config),
			Editable:   false,
		}
		if s.workerPoolRepo != nil {
			if poolState, stateErr := s.workerPoolRepo.GetWorkerPoolState(ctx, name); stateErr == nil {
				view.State = poolState
				view.MachineCount = int(poolState.RegisteredMachines)
				view.ReadyMachineCount = int(poolState.ReadyMachines)
			}
		}
		if view.Controller == model.PlatformPoolControllerAgent {
			if state, stateErr := s.computeRepo.GetPoolState(ctx, workspace.ExternalId, name); stateErr != nil {
				return nil, stateErr
			} else if state != nil && state.PlatformManaged && state.WorkerConfig != nil {
				agentView, viewErr := s.platformPoolView(ctx, state)
				if viewErr != nil {
					return nil, viewErr
				}
				view.State = agentView.State
				view.MachineCount = agentView.MachineCount
				view.ReadyMachineCount = agentView.ReadyMachineCount
			}
		}
		views[name] = view
	}

	states, err := s.computeRepo.ListPoolStates(ctx, workspace.ExternalId, 0)
	if err != nil {
		return nil, err
	}
	for _, state := range states {
		if state == nil || !state.PlatformManaged || state.PlatformSource != model.PlatformPoolSourceDashboard {
			continue
		}
		if _, collision := views[state.Name]; collision {
			return nil, fmt.Errorf("platform pool %q exists in config and persisted state", state.Name)
		}
		view, err := s.platformPoolView(ctx, state)
		if err != nil {
			return nil, err
		}
		views[state.Name] = view
	}

	names := make([]string, 0, len(views))
	for name := range views {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]*model.PlatformPool, 0, len(names))
	for _, name := range names {
		out = append(out, views[name])
	}
	return out, nil
}

func (s *Service) CreatePlatformPool(ctx context.Context, authInfo *auth.AuthInfo, name string, config types.WorkerPoolConfig) (*model.PlatformPool, error) {
	if err := requirePlatformOperator(authInfo); err != nil {
		return nil, err
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, fmt.Errorf("%w: pool name is required", model.ErrPlatformInvalidConfig)
	}
	if err := model.ValidatePoolName(name); err != nil {
		return nil, fmt.Errorf("%w: %v", model.ErrPlatformInvalidConfig, err)
	}
	if _, exists := s.appConfig.Worker.Pools[name]; exists {
		return nil, model.ErrPlatformPoolConflict
	}
	config, err := normalizePlatformWorkerPoolConfig(config)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", model.ErrPlatformInvalidConfig, err)
	}
	workspace, err := s.platformWorkspace(ctx)
	if err != nil {
		return nil, err
	}
	workspaceID := workspace.ExternalId
	state := newPlatformPoolState(workspaceID, name, model.PlatformPoolSourceDashboard, computeOwnerTokenID(authInfo), config, time.Now().UTC())

	err = s.withPoolStateLock(ctx, workspaceID, name, func() error {
		existing, err := s.computeRepo.GetPoolState(ctx, workspaceID, name)
		if err != nil {
			return err
		}
		if existing != nil {
			return model.ErrPlatformPoolConflict
		}
		if err := s.computeRepo.SavePoolState(ctx, workspaceID, state); err != nil {
			return err
		}
		if s.scheduler != nil {
			if err := s.scheduler.RegisterAgentPool(workspaceID, state); err != nil {
				_ = s.computeRepo.DeletePoolState(ctx, workspaceID, name)
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s.platformPoolView(ctx, state)
}

func (s *Service) platformPoolHasInventory(ctx context.Context, state *model.PoolState) (bool, error) {
	machines, err := s.computeRepo.ListAgentTokenStates(ctx, state.WorkspaceID, state.Name)
	if err != nil {
		return false, err
	}
	if len(machines) > 0 {
		return true, nil
	}
	if s.workerRepo == nil {
		return false, nil
	}
	workers, err := s.workerRepo.GetAllWorkersInPool(state.Name)
	if err != nil {
		return false, err
	}
	return len(workers) > 0, nil
}

func (s *Service) UpdatePlatformPool(ctx context.Context, authInfo *auth.AuthInfo, name string, config types.WorkerPoolConfig) (*model.PlatformPool, error) {
	if err := requirePlatformOperator(authInfo); err != nil {
		return nil, err
	}
	config, err := normalizePlatformWorkerPoolConfig(config)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", model.ErrPlatformInvalidConfig, err)
	}
	workspace, err := s.platformWorkspace(ctx)
	if err != nil {
		return nil, err
	}
	workspaceID := workspace.ExternalId
	var updated *model.PoolState
	err = s.withPoolStateLock(ctx, workspaceID, name, func() error {
		existing, err := s.computeRepo.GetPoolState(ctx, workspaceID, name)
		if err != nil {
			return err
		}
		if existing == nil || !existing.PlatformManaged {
			return model.ErrPlatformPoolNotFound
		}
		if existing.PlatformSource != model.PlatformPoolSourceDashboard {
			return model.ErrPlatformPoolImmutable
		}
		if existing.WorkerConfig == nil {
			return model.ErrPlatformPoolNotFound
		}
		if !reflect.DeepEqual(*existing.WorkerConfig, config) {
			inUse, err := s.platformPoolHasInventory(ctx, existing)
			if err != nil {
				return err
			}
			if inUse {
				return model.ErrPlatformPoolInUse
			}
		}
		updated = newPlatformPoolState(workspaceID, name, model.PlatformPoolSourceDashboard, existing.CreatedByTokenID, config, time.Now().UTC())
		updated.CreatedAt = existing.CreatedAt
		updated.PlatformInstanceID = existing.PlatformInstanceID
		if updated.PlatformInstanceID == "" {
			updated.PlatformInstanceID = uuid.NewString()
		}
		if err := s.computeRepo.SavePoolState(ctx, workspaceID, updated); err != nil {
			return err
		}
		if s.scheduler != nil {
			if err := s.scheduler.RegisterAgentPool(workspaceID, updated); err != nil {
				_ = s.computeRepo.SavePoolState(ctx, workspaceID, existing)
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s.platformPoolView(ctx, updated)
}

func (s *Service) DeletePlatformPool(ctx context.Context, authInfo *auth.AuthInfo, name string) error {
	if err := requirePlatformOperator(authInfo); err != nil {
		return err
	}
	workspace, err := s.platformWorkspace(ctx)
	if err != nil {
		return err
	}
	workspaceID := workspace.ExternalId
	return s.withPoolStateLock(ctx, workspaceID, name, func() error {
		state, err := s.computeRepo.GetPoolState(ctx, workspaceID, name)
		if err != nil {
			return err
		}
		if state == nil || !state.PlatformManaged {
			return model.ErrPlatformPoolNotFound
		}
		if state.PlatformSource != model.PlatformPoolSourceDashboard {
			return model.ErrPlatformPoolImmutable
		}
		inUse, err := s.platformPoolHasInventory(ctx, state)
		if err != nil {
			return err
		}
		if inUse {
			return model.ErrPlatformPoolInUse
		}
		if s.workerPoolRepo != nil {
			if err := s.workerPoolRepo.DeleteWorkerPoolState(ctx, name); err != nil {
				return err
			}
		}
		if err := s.computeRepo.DeletePoolState(ctx, workspaceID, name); err != nil {
			return err
		}
		if s.scheduler != nil {
			s.scheduler.DeleteAgentPool(name)
		}
		return nil
	})
}

func (s *Service) CreatePlatformMachine(ctx context.Context, authInfo *auth.AuthInfo, poolName string) (*PlatformMachineBootstrap, bool, error) {
	if err := requirePlatformOperator(authInfo); err != nil {
		return nil, false, err
	}
	state, err := s.platformPoolState(ctx, poolName)
	if err != nil {
		return nil, false, err
	}
	if state == nil {
		return nil, false, nil
	}
	if state.PlatformInstanceID == "" {
		return nil, true, fmt.Errorf("platform pool %q has not completed secure reconciliation", state.Name)
	}
	if state.PlatformSource == model.PlatformPoolSourceConfig {
		configured, ok := s.appConfig.Worker.Pools[state.Name]
		if !ok || configured.Mode != types.PoolModeExternal || configured.Provider != nil {
			return nil, false, nil
		}
	}
	machineID := "machine-" + uuid.NewString()
	token, tokenState, err := s.createPrivatePoolJoinTokenStateForOwner(ctx, state.WorkspaceID, computeOwnerTokenID(authInfo), state.Name, defaultPrivateJoinTTL, machineID)
	if err != nil {
		return nil, true, err
	}
	tokenState.Mode = string(types.PoolModeExternal)
	tokenState.PlatformManaged = true
	tokenState.PlatformPoolInstanceID = state.PlatformInstanceID
	if err := s.saveComputeJoinTokenState(ctx, tokenState, defaultPrivateJoinTTL); err != nil {
		// The shared token helper persists the initial state before platform
		// metadata is attached. Never leave that partial token usable when the
		// authoritative rewrite fails.
		_ = s.revokeComputeJoinTokenHash(ctx, tokenState.TokenHash)
		return nil, true, err
	}
	gatewayURL := strings.TrimRight(s.appConfig.GatewayService.HTTP.GetExternalURL(), "/")
	command := agentInstallCommand(gatewayURL, token, isLocalGatewayURL(gatewayURL), agentWorkerImage(s.appConfig))
	return &PlatformMachineBootstrap{
		MachineID:      machineID,
		PoolName:       state.Name,
		Token:          token,
		InstallCommand: command,
		ExpiresAt:      tokenState.ExpiresAt,
	}, true, nil
}

func (s *Service) ListPlatformMachines(ctx context.Context, authInfo *auth.AuthInfo, poolName string) ([]*pb.Machine, bool, error) {
	if err := requirePlatformOperator(authInfo); err != nil {
		return nil, false, err
	}
	workspace, err := s.platformWorkspace(ctx)
	if err != nil {
		return nil, false, err
	}
	states, err := s.computeRepo.ListPoolStates(ctx, workspace.ExternalId, 0)
	if err != nil {
		return nil, false, err
	}
	found := poolName == ""
	out := []*pb.Machine{}
	for _, state := range states {
		if state == nil || !state.PlatformManaged || state.WorkerConfig == nil {
			continue
		}
		if poolName != "" && state.Name != poolName {
			continue
		}
		found = true
		machines, err := s.computeRepo.ListAgentTokenStates(ctx, workspace.ExternalId, state.Name)
		if err != nil {
			return nil, true, err
		}
		for _, machine := range machines {
			out = append(out, s.agentMachineToProto(machine))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Id < out[j].Id })
	return out, found, nil
}

func (s *Service) DeletePlatformMachine(ctx context.Context, authInfo *auth.AuthInfo, poolName, machineID string) (bool, error) {
	if err := requirePlatformOperator(authInfo); err != nil {
		return false, err
	}
	state, err := s.platformPoolState(ctx, poolName)
	if err != nil || state == nil {
		return state != nil, err
	}
	machine, err := s.computeRepo.GetAgentMachineState(ctx, state.WorkspaceID, state.Name, machineID)
	if err != nil {
		return true, err
	}
	if machine == nil {
		return true, model.ErrPlatformPoolNotFound
	}
	if s.containerRepo != nil {
		containers, err := s.containerRepo.GetActiveContainersByWorkerId(model.AgentMachineWorkerID(machine.MachineID))
		if err != nil {
			return true, err
		}
		if len(containers) > 0 {
			return true, fmt.Errorf("machine must be drained before deletion: %w", model.ErrPlatformPoolInUse)
		}
	}
	return true, s.removePrivateMachine(ctx, machine)
}
