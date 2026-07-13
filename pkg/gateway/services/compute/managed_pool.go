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

type ManagedMachineBootstrap struct {
	MachineID      string
	PoolName       string
	Token          string
	InstallCommand string
}

func requireClusterAdmin(authInfo *auth.AuthInfo) error {
	if authInfo == nil || authInfo.Token == nil ||
		!authInfo.Token.Active || authInfo.Token.DisabledByClusterAdmin ||
		authInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return model.ErrManagedPermissionDenied
	}
	return nil
}

func (s *Service) adminWorkspaceID(ctx context.Context) (string, error) {
	if s == nil || s.backendRepo == nil {
		return "", fmt.Errorf("admin workspace repository is unavailable")
	}
	workspace, err := s.backendRepo.GetAdminWorkspace(ctx)
	if err != nil {
		return "", err
	}
	if workspace == nil || strings.TrimSpace(workspace.ExternalId) == "" {
		return "", fmt.Errorf("admin workspace is unavailable")
	}
	return workspace.ExternalId, nil
}

func (s *Service) managedPoolWorkspace(ctx context.Context, authInfo *auth.AuthInfo) (string, error) {
	if err := requireClusterAdmin(authInfo); err != nil {
		return "", err
	}
	return s.adminWorkspaceID(ctx)
}

func normalizeManagedPoolConfig(config types.WorkerPoolConfig) (types.WorkerPoolConfig, error) {
	if config.Mode == "" {
		config.Mode = types.PoolModeExternal
	}
	if config.Mode != types.PoolModeExternal {
		return types.WorkerPoolConfig{}, fmt.Errorf("API-managed pools must use mode %q", types.PoolModeExternal)
	}
	if config.Provider != nil {
		return types.WorkerPoolConfig{}, fmt.Errorf("API-managed pools are agent-backed and cannot set provider")
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

func managedPoolConfig(name string, config types.WorkerPoolConfig) *pb.PoolConfig {
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

func newManagedPoolState(workspaceID, name, source, createdBy string, config types.WorkerPoolConfig, now time.Time) *model.PoolState {
	configCopy := config
	return &model.PoolState{
		WorkspaceID:       workspaceID,
		Name:              name,
		Selector:          name,
		Config:            managedPoolConfig(name, config),
		Status:            "active",
		Source:            model.SourceAttached,
		Mode:              string(types.PoolModeExternal),
		Transport:         defaultPrivateTransport,
		Fallback:          defaultPrivateFallback,
		Priority:          config.Priority,
		Preemptible:       config.Preemptable,
		CreatedByTokenID:  createdBy,
		CreatedAt:         now,
		UpdatedAt:         now,
		ManagementSource:  source,
		ManagedInstanceID: uuid.NewString(),
		WorkerConfig:      &configCopy,
	}
}

// ReconcileManagedPools owns the persisted runtime state for config- and
// API-managed agent pools. It never converts local or provider-backed pools.
func (s *Service) ReconcileManagedPools(ctx context.Context) error {
	if s == nil || s.computeRepo == nil {
		return nil
	}
	workspaceID, err := s.adminWorkspaceID(ctx)
	if err != nil {
		return err
	}
	now := time.Now().UTC()

	for name, config := range s.appConfig.Worker.Pools {
		if config.Mode != types.PoolModeExternal || config.Provider != nil {
			continue
		}
		config, err = normalizeManagedPoolConfig(config)
		if err != nil {
			return fmt.Errorf("pool %q: %w", name, err)
		}
		err = s.withPoolStateLock(ctx, workspaceID, name, func() error {
			existing, err := s.computeRepo.GetPoolState(ctx, workspaceID, name)
			if err != nil {
				return err
			}
			if existing != nil && (existing.ManagementSource == "" || existing.ManagementSource == model.ManagedPoolSourceAPI) {
				return fmt.Errorf("config-managed pool %q conflicts with persisted pool state", name)
			}
			state := newManagedPoolState(workspaceID, name, model.ManagedPoolSourceConfig, "config", config, now)
			if existing != nil {
				state.CreatedAt = existing.CreatedAt
				state.CreatedByTokenID = existing.CreatedByTokenID
				if existing.ManagedInstanceID != "" {
					state.ManagedInstanceID = existing.ManagedInstanceID
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
		if state == nil || state.ManagementSource == "" || state.WorkerConfig == nil {
			continue
		}
		if state.ManagedInstanceID == "" {
			if err := s.withPoolStateLock(ctx, workspaceID, state.Name, func() error {
				current, err := s.computeRepo.GetPoolState(ctx, workspaceID, state.Name)
				if err != nil || current == nil {
					return err
				}
				if current.ManagedInstanceID == "" {
					current.ManagedInstanceID = uuid.NewString()
					current.UpdatedAt = now
					if err := s.computeRepo.SavePoolState(ctx, workspaceID, current); err != nil {
						return err
					}
				}
				state = current
				return nil
			}); err != nil {
				reconcileErrors = append(reconcileErrors, fmt.Errorf("assign pool instance %q: %w", state.Name, err))
				continue
			}
		}
		configured, static := s.appConfig.Worker.Pools[state.Name]
		switch state.ManagementSource {
		case model.ManagedPoolSourceConfig:
			active := static && configured.Mode == types.PoolModeExternal && configured.Provider == nil
			if !active {
				inUse, inventoryErr := s.managedPoolHasInventory(ctx, state)
				if inventoryErr != nil {
					reconcileErrors = append(reconcileErrors, fmt.Errorf("inspect stale config pool %q: %w", state.Name, inventoryErr))
					continue
				}
				if inUse {
					reconcileErrors = append(reconcileErrors, fmt.Errorf("config pool %q was removed or changed while it still has inventory", state.Name))
					continue
				}
				if s.workerPoolRepo != nil {
					if deleteErr := s.workerPoolRepo.DeleteWorkerPoolState(ctx, state.Name); deleteErr != nil {
						reconcileErrors = append(reconcileErrors, fmt.Errorf("delete stale worker pool state %q: %w", state.Name, deleteErr))
						continue
					}
				}
				if deleteErr := s.computeRepo.DeletePoolState(ctx, workspaceID, state.Name); deleteErr != nil {
					reconcileErrors = append(reconcileErrors, fmt.Errorf("delete stale config pool %q: %w", state.Name, deleteErr))
				}
				continue
			}
		case model.ManagedPoolSourceAPI:
			if static {
				reconcileErrors = append(reconcileErrors, fmt.Errorf("API-managed pool %q conflicts with config", state.Name))
				continue
			}
		default:
			reconcileErrors = append(reconcileErrors, fmt.Errorf("pool %q has invalid management source %q", state.Name, state.ManagementSource))
			continue
		}
		if s.scheduler != nil {
			if err := s.scheduler.RegisterAgentPool(workspaceID, state); err != nil {
				reconcileErrors = append(reconcileErrors, fmt.Errorf("register pool %q: %w", state.Name, err))
			}
		}
	}
	return errors.Join(reconcileErrors...)
}

func (s *Service) managedPoolState(ctx context.Context, authInfo *auth.AuthInfo, name string) (*model.PoolState, error) {
	workspaceID, err := s.managedPoolWorkspace(ctx, authInfo)
	if err != nil {
		return nil, err
	}
	state, err := s.computeRepo.GetPoolState(ctx, workspaceID, name)
	if err != nil {
		return nil, err
	}
	if state == nil || state.ManagementSource == "" || state.Mode != string(types.PoolModeExternal) || state.WorkerConfig == nil {
		return nil, nil
	}
	return state, nil
}

func (s *Service) managedPoolView(ctx context.Context, state *model.PoolState) (*model.ManagedPool, error) {
	if state == nil || state.WorkerConfig == nil {
		return nil, model.ErrManagedPoolNotFound
	}
	view := &model.ManagedPool{
		Name:       state.Name,
		Config:     *state.WorkerConfig,
		Source:     state.ManagementSource,
		Controller: model.ManagedPoolControllerAgent,
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
	now := time.Now().UTC()
	for _, machine := range machines {
		if machine != nil && machine.Schedulable && model.AgentMachineConnected(machine, now) {
			view.ReadyMachineCount++
		}
	}
	return view, nil
}

func configuredManagedPoolController(config types.WorkerPoolConfig) string {
	switch {
	case config.Mode == types.PoolModeLocal:
		return model.ManagedPoolControllerLocal
	case config.Mode == types.PoolModeExternal && config.Provider == nil:
		return model.ManagedPoolControllerAgent
	case config.Mode == types.PoolModeExternal:
		return model.ManagedPoolControllerExternalLegacy
	default:
		return model.ManagedPoolControllerAgent
	}
}

func (s *Service) ListManagedPools(ctx context.Context, authInfo *auth.AuthInfo) ([]*model.ManagedPool, error) {
	workspaceID, err := s.managedPoolWorkspace(ctx, authInfo)
	if err != nil {
		return nil, err
	}

	views := make(map[string]*model.ManagedPool, len(s.appConfig.Worker.Pools))
	for name, config := range s.appConfig.Worker.Pools {
		view := &model.ManagedPool{
			Name:       name,
			Config:     config,
			Source:     model.ManagedPoolSourceConfig,
			Controller: configuredManagedPoolController(config),
		}
		if s.workerPoolRepo != nil {
			if poolState, stateErr := s.workerPoolRepo.GetWorkerPoolState(ctx, name); stateErr == nil {
				view.State = poolState
				view.MachineCount = int(poolState.RegisteredMachines)
				view.ReadyMachineCount = int(poolState.ReadyMachines)
			}
		}
		if view.Controller == model.ManagedPoolControllerAgent {
			if state, stateErr := s.computeRepo.GetPoolState(ctx, workspaceID, name); stateErr != nil {
				return nil, stateErr
			} else if state != nil && state.ManagementSource != "" && state.WorkerConfig != nil {
				view, err = s.managedPoolView(ctx, state)
				if err != nil {
					return nil, err
				}
			}
		}
		views[name] = view
	}

	states, err := s.computeRepo.ListPoolStates(ctx, workspaceID, 0)
	if err != nil {
		return nil, err
	}
	for _, state := range states {
		if state == nil || state.ManagementSource != model.ManagedPoolSourceAPI {
			continue
		}
		if _, collision := views[state.Name]; collision {
			return nil, fmt.Errorf("pool %q exists in config and persisted state", state.Name)
		}
		view, err := s.managedPoolView(ctx, state)
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
	out := make([]*model.ManagedPool, 0, len(names))
	for _, name := range names {
		out = append(out, views[name])
	}
	return out, nil
}

func (s *Service) CreateManagedPool(ctx context.Context, authInfo *auth.AuthInfo, name string, config types.WorkerPoolConfig) (*model.ManagedPool, error) {
	workspaceID, err := s.managedPoolWorkspace(ctx, authInfo)
	if err != nil {
		return nil, err
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, fmt.Errorf("%w: pool name is required", model.ErrInvalidManagedPoolConfig)
	}
	if err := model.ValidatePoolName(name); err != nil {
		return nil, fmt.Errorf("%w: %v", model.ErrInvalidManagedPoolConfig, err)
	}
	if _, exists := s.appConfig.Worker.Pools[name]; exists {
		return nil, model.ErrManagedPoolConflict
	}
	config, err = normalizeManagedPoolConfig(config)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", model.ErrInvalidManagedPoolConfig, err)
	}
	state := newManagedPoolState(workspaceID, name, model.ManagedPoolSourceAPI, computeOwnerTokenID(authInfo), config, time.Now().UTC())

	err = s.withPoolStateLock(ctx, workspaceID, name, func() error {
		existing, err := s.computeRepo.GetPoolState(ctx, workspaceID, name)
		if err != nil {
			return err
		}
		if existing != nil {
			return model.ErrManagedPoolConflict
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
	return s.managedPoolView(ctx, state)
}

func (s *Service) managedPoolHasInventory(ctx context.Context, state *model.PoolState) (bool, error) {
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

func (s *Service) UpdateManagedPool(ctx context.Context, authInfo *auth.AuthInfo, name string, config types.WorkerPoolConfig) (*model.ManagedPool, error) {
	workspaceID, err := s.managedPoolWorkspace(ctx, authInfo)
	if err != nil {
		return nil, err
	}
	config, err = normalizeManagedPoolConfig(config)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", model.ErrInvalidManagedPoolConfig, err)
	}
	var updated *model.PoolState
	err = s.withPoolStateLock(ctx, workspaceID, name, func() error {
		existing, err := s.computeRepo.GetPoolState(ctx, workspaceID, name)
		if err != nil {
			return err
		}
		if existing == nil || existing.ManagementSource == "" {
			return model.ErrManagedPoolNotFound
		}
		if existing.ManagementSource != model.ManagedPoolSourceAPI {
			return model.ErrManagedPoolImmutable
		}
		if existing.WorkerConfig == nil {
			return model.ErrManagedPoolNotFound
		}
		if !reflect.DeepEqual(*existing.WorkerConfig, config) {
			inUse, err := s.managedPoolHasInventory(ctx, existing)
			if err != nil {
				return err
			}
			if inUse {
				return model.ErrManagedPoolInUse
			}
		}
		now := time.Now().UTC()
		next := *existing
		configCopy := config
		next.Config = managedPoolConfig(name, config)
		next.Priority = config.Priority
		next.Preemptible = config.Preemptable
		next.WorkerConfig = &configCopy
		next.UpdatedAt = now
		updated = &next
		if updated.ManagedInstanceID == "" {
			updated.ManagedInstanceID = uuid.NewString()
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
	return s.managedPoolView(ctx, updated)
}

func (s *Service) DeleteManagedPool(ctx context.Context, authInfo *auth.AuthInfo, name string) error {
	workspaceID, err := s.managedPoolWorkspace(ctx, authInfo)
	if err != nil {
		return err
	}
	return s.withPoolStateLock(ctx, workspaceID, name, func() error {
		state, err := s.computeRepo.GetPoolState(ctx, workspaceID, name)
		if err != nil {
			return err
		}
		if state == nil || state.ManagementSource == "" {
			return model.ErrManagedPoolNotFound
		}
		if state.ManagementSource != model.ManagedPoolSourceAPI {
			return model.ErrManagedPoolImmutable
		}
		inUse, err := s.managedPoolHasInventory(ctx, state)
		if err != nil {
			return err
		}
		if inUse {
			return model.ErrManagedPoolInUse
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

func (s *Service) CreateManagedMachine(ctx context.Context, authInfo *auth.AuthInfo, poolName string) (*ManagedMachineBootstrap, bool, error) {
	state, err := s.managedPoolState(ctx, authInfo, poolName)
	if err != nil {
		return nil, false, err
	}
	if state == nil {
		return nil, false, nil
	}
	if state.ManagedInstanceID == "" {
		return nil, true, fmt.Errorf("pool %q has not completed secure reconciliation", state.Name)
	}
	if state.ManagementSource == model.ManagedPoolSourceConfig {
		configured, ok := s.appConfig.Worker.Pools[state.Name]
		if !ok || configured.Mode != types.PoolModeExternal || configured.Provider != nil {
			return nil, false, nil
		}
	}
	machineID := "machine-" + uuid.NewString()
	token, tokenState, err := newPoolJoinToken(state.WorkspaceID, computeOwnerTokenID(authInfo), state.Name, defaultPrivateJoinTTL, machineID)
	if err != nil {
		return nil, true, err
	}
	tokenState.Mode = string(types.PoolModeExternal)
	tokenState.ManagedPoolInstanceID = state.ManagedInstanceID
	if err := s.savePoolJoinToken(ctx, tokenState, defaultPrivateJoinTTL); err != nil {
		return nil, true, err
	}
	gatewayURL := strings.TrimRight(s.appConfig.GatewayService.HTTP.GetExternalURL(), "/")
	command := agentInstallCommand(gatewayURL, token, isLocalGatewayURL(gatewayURL), agentWorkerImage(s.appConfig))
	return &ManagedMachineBootstrap{
		MachineID:      machineID,
		PoolName:       state.Name,
		Token:          token,
		InstallCommand: command,
	}, true, nil
}

func (s *Service) ListManagedMachines(ctx context.Context, authInfo *auth.AuthInfo, poolName string) ([]*pb.Machine, bool, error) {
	workspaceID, err := s.managedPoolWorkspace(ctx, authInfo)
	if err != nil {
		return nil, false, err
	}
	states, err := s.computeRepo.ListPoolStates(ctx, workspaceID, 0)
	if err != nil {
		return nil, false, err
	}
	found := poolName == ""
	out := []*pb.Machine{}
	for _, state := range states {
		if state == nil || state.ManagementSource == "" || state.WorkerConfig == nil {
			continue
		}
		if poolName != "" && state.Name != poolName {
			continue
		}
		found = true
		machines, err := s.computeRepo.ListAgentTokenStates(ctx, workspaceID, state.Name)
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

func (s *Service) DeleteManagedMachine(ctx context.Context, authInfo *auth.AuthInfo, poolName, machineID string) (bool, error) {
	state, err := s.managedPoolState(ctx, authInfo, poolName)
	if err != nil || state == nil {
		return state != nil, err
	}
	machine, err := s.computeRepo.GetAgentMachineState(ctx, state.WorkspaceID, state.Name, machineID)
	if err != nil {
		return true, err
	}
	if machine == nil {
		return true, model.ErrManagedPoolNotFound
	}
	if s.containerRepo != nil {
		containers, err := s.containerRepo.GetActiveContainersByWorkerId(model.AgentMachineWorkerID(machine.MachineID))
		if err != nil {
			return true, err
		}
		if len(containers) > 0 {
			return true, fmt.Errorf("machine must be drained before deletion: %w", model.ErrManagedPoolInUse)
		}
	}
	return true, s.removePrivateMachine(ctx, machine)
}
